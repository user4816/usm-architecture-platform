"""
Ingestion Service - Incremental document indexing for RAG Chat.

Responsibilities:
- Walk docs_path for .md and .yaml files
- Track file changes via mtime+hash in indexing_state.json
- Chunk text using HuggingFace AutoTokenizer (bge-m3)
- Extract metadata from folder structure and file content
- Call remote embedding API and upsert to local ChromaDB
"""

import json
import hashlib
import shutil
import logging
import re
from pathlib import Path

import httpx
import yaml
import chromadb
from chromadb.config import Settings as ChromaSettings

logger = logging.getLogger(__name__)


# ─── Tokenizer (lazy-loaded singleton) ────────────────────────────
_tokenizer = None


def _get_tokenizer(model_name: str = "BAAI/bge-m3"):
    """Lazy-load HuggingFace AutoTokenizer for accurate token counting."""
    global _tokenizer
    if _tokenizer is None:
        try:
            from transformers import AutoTokenizer
            _tokenizer = AutoTokenizer.from_pretrained(model_name)
            logger.info(f"Loaded tokenizer: {model_name}")
        except Exception as e:
            logger.warning(f"Failed to load AutoTokenizer ({e}); falling back to whitespace split.")
            _tokenizer = "fallback"
    return _tokenizer


def _tokenize(text: str, model_name: str = "BAAI/bge-m3") -> list[str]:
    """Tokenize text; falls back to whitespace if AutoTokenizer unavailable."""
    tok = _get_tokenizer(model_name)
    if tok == "fallback":
        return text.split()
    return tok.tokenize(text)


def _detokenize(tokens: list[str], model_name: str = "BAAI/bge-m3") -> str:
    """Convert tokens back to text."""
    tok = _get_tokenizer(model_name)
    if tok == "fallback":
        return " ".join(tokens)
    return tok.convert_tokens_to_string(tokens)


# ─── File discovery ──────────────────────────────────────────────
def _discover_files(docs_path: Path) -> list[Path]:
    """Recursively find all .md and .yaml files under docs_path."""
    found = []
    for ext in ("*.md", "*.yaml", "*.yml"):
        found.extend(docs_path.rglob(ext))
    return sorted(set(found))


# ─── Incremental state ──────────────────────────────────────────
def _load_indexing_state(state_path: Path) -> dict:
    """Load the indexing state JSON (maps relative_path -> {mtime, hash})."""
    if state_path.exists():
        try:
            return json.loads(state_path.read_text(encoding="utf-8"))
        except Exception:
            logger.warning("Corrupted indexing_state.json; starting fresh.")
    return {}


def _save_indexing_state(state_path: Path, state: dict):
    """Persist the indexing state to disk."""
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def _file_hash(file_path: Path) -> str:
    """Compute SHA-256 hash of a file's content."""
    return hashlib.sha256(file_path.read_bytes()).hexdigest()


def _needs_update(file_path: Path, rel_key: str, state: dict) -> bool:
    """Check if a file has changed since last indexing."""
    if rel_key not in state:
        return True
    prev = state[rel_key]
    current_mtime = file_path.stat().st_mtime
    if current_mtime != prev.get("mtime"):
        current_hash = _file_hash(file_path)
        return current_hash != prev.get("hash")
    return False


# ─── Chunking ────────────────────────────────────────────────────

_FCAPS_KEYS = {"CM", "FM", "PM", "SM"}


def _chunk_text(text: str, chunk_size_tokens: int, chunk_overlap_tokens: int,
                model_name: str = "BAAI/bge-m3") -> list[str]:
    """Split text into chunks of approximately chunk_size_tokens with overlap."""
    tokens = _tokenize(text, model_name)
    if not tokens:
        return []

    chunks = []
    start = 0
    while start < len(tokens):
        end = start + chunk_size_tokens
        chunk_tokens = tokens[start:end]
        chunk_text = _detokenize(chunk_tokens, model_name)
        if chunk_text.strip():
            chunks.append(chunk_text.strip())
        if end >= len(tokens):
            break
        start = end - chunk_overlap_tokens

    return chunks


def _chunk_yaml_structured(file_path: Path, content: str, docs_path: Path,
                           chunk_size_tokens: int, chunk_overlap_tokens: int,
                           metadata_fields: list[str]) -> list[dict]:
    """
    Structural YAML chunking — split by FCAPS (CM/FM/PM/SM) × section (rest/sftp).

    Returns a list of dicts: {"text": str, "metadata": dict}
    Each chunk gets full metadata: usm_version, package, operator, fcaps, section.
    If a section exceeds chunk_size_tokens, it is secondary-split with overlap.
    Non-FCAPS keys (e.g. main_sequence) get fcaps="other" + description metadata.
    """
    # ── Path-based metadata ──
    base_meta = {}
    try:
        rel = file_path.relative_to(docs_path)
        parts = rel.parts  # e.g., ("USMv1", "26B", "Verizon", "doc.yaml")
        if len(parts) >= 3:
            base_meta["usm_version"] = parts[0]
            if "package" in metadata_fields or "version" in metadata_fields:
                base_meta["package"] = parts[1]   # e.g., "26B"
            if "operator" in metadata_fields:
                base_meta["operator"] = parts[2]  # e.g., "Verizon"
    except ValueError:
        pass
    base_meta["source_file"] = str(file_path.relative_to(docs_path))

    # ── Parse YAML ──
    try:
        data = yaml.safe_load(content)
    except Exception:
        # Fallback to generic text chunking if YAML parse fails
        text_chunks = _chunk_text(content, chunk_size_tokens, chunk_overlap_tokens)
        return [{"text": t, "metadata": {**base_meta, "fcaps": "unknown", "section": "unknown"}} for t in text_chunks]

    if not isinstance(data, dict):
        text_chunks = _chunk_text(content, chunk_size_tokens, chunk_overlap_tokens)
        return [{"text": t, "metadata": {**base_meta, "fcaps": "unknown", "section": "unknown"}} for t in text_chunks]

    result = []

    for top_key, top_val in data.items():
        top_upper = top_key.upper()

        if top_upper in _FCAPS_KEYS and isinstance(top_val, dict):
            # ── FCAPS key: iterate sections (rest, sftp, ...) ──
            for section_key, section_val in top_val.items():
                chunk_meta = {
                    **base_meta,
                    "fcaps": top_upper,
                    "section": section_key,  # "rest" or "sftp"
                }
                # Serialize section sub-tree to YAML text
                section_text = f"{top_key}.{section_key}:\n"
                section_text += yaml.dump(
                    section_val, default_flow_style=False,
                    allow_unicode=True, sort_keys=False
                )

                # Secondary split if the section exceeds chunk_size_tokens
                section_tokens = _tokenize(section_text)
                if len(section_tokens) <= chunk_size_tokens:
                    result.append({"text": section_text.strip(), "metadata": chunk_meta})
                else:
                    sub_chunks = _chunk_text(
                        section_text, chunk_size_tokens, chunk_overlap_tokens
                    )
                    for sub in sub_chunks:
                        result.append({"text": sub, "metadata": {**chunk_meta}})
        else:
            # ── Non-FCAPS key (e.g. main_sequence) ──
            chunk_meta = {
                **base_meta,
                "fcaps": "other",
                "section": top_key,
                "description": f"Non-FCAPS data: {top_key}",
            }
            if isinstance(top_val, str):
                text_val = f"{top_key}: {top_val}"
            else:
                text_val = f"{top_key}:\n" + yaml.dump(
                    top_val, default_flow_style=False,
                    allow_unicode=True, sort_keys=False
                )
            sub_chunks = _chunk_text(
                text_val, chunk_size_tokens, chunk_overlap_tokens
            )
            for sub in sub_chunks:
                result.append({"text": sub, "metadata": {**chunk_meta}})

    if not result:
        # Empty YAML — fallback
        text_chunks = _chunk_text(content, chunk_size_tokens, chunk_overlap_tokens)
        return [{"text": t, "metadata": {**base_meta, "fcaps": "unknown", "section": "unknown"}} for t in text_chunks]

    return result


# ─── Metadata extraction ────────────────────────────────────────
def _extract_metadata(file_path: Path, content: str, docs_path: Path,
                      metadata_fields: list[str]) -> dict:
    """
    Extract metadata from folder structure and file content.
    Used for .md files (YAML files use _chunk_yaml_structured instead).

    Folder structure: data/{USMversion}/{package}/{operator}/filename
    - package = release package (e.g., "26B") — NOT the YAML info.version
    - operator = operator folder name (e.g., "Verizon")
    """
    meta = {}

    # Parse folder structure
    try:
        rel = file_path.relative_to(docs_path)
        parts = rel.parts  # e.g., ("USMv1", "26B", "Verizon", "doc.yaml")
        if len(parts) >= 3:
            if "package" in metadata_fields or "version" in metadata_fields:
                meta["package"] = parts[1]  # release package: "26B"
            if "operator" in metadata_fields:
                meta["operator"] = parts[2]  # operator: "Verizon"
            meta["usm_version"] = parts[0]  # USMv1 / USMv2
    except ValueError:
        pass

    # Parse section from markdown headings
    if "section" in metadata_fields and file_path.suffix == ".md":
        heading_match = re.search(r'^#\s+(.+)', content, re.MULTILINE)
        if heading_match:
            meta["section"] = heading_match.group(1).strip()

    # Add source file reference
    meta["source_file"] = str(file_path.relative_to(docs_path))

    return meta


# ─── Embedding via remote API ───────────────────────────────────
async def _get_embeddings(texts: list[str], api_url: str, model: str,
                          timeout: int = 120) -> list[list[float]]:
    """Call remote embedding API (Ollama /api/embed format) for a batch of texts."""
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(api_url, json={
            "model": model,
            "input": texts
        })
        response.raise_for_status()
        data = response.json()

        # Ollama returns {"embeddings": [[...], [...]]}
        embeddings = data.get("embeddings", [])
        if not embeddings:
            raise ValueError(f"No embeddings returned from {api_url}")

        return embeddings


# ─── ChromaDB client (lazy singleton) ───────────────────────────
_chroma_client = None
_chroma_collection = None


def _get_chroma_collection(chromadb_path: str, collection_name: str = "nbi_docs"):
    """Get or create the ChromaDB collection."""
    global _chroma_client, _chroma_collection
    if _chroma_collection is None:
        Path(chromadb_path).mkdir(parents=True, exist_ok=True)
        _chroma_client = chromadb.PersistentClient(
            path=chromadb_path,
            settings=ChromaSettings(anonymized_telemetry=False)
        )
        _chroma_collection = _chroma_client.get_or_create_collection(
            name=collection_name,
            metadata={"hnsw:space": "cosine"}
        )
        logger.info(f"ChromaDB collection '{collection_name}' ready at {chromadb_path}")
    return _chroma_collection


def get_chroma_collection_for_query(config: dict):
    """
    Get the ChromaDB collection for queries (reuses singleton).
    MUST share the same PersistentClient as indexing — two separate
    PersistentClient instances cause Rust FFI access violation on Windows.
    """
    storage = config["local_storage"]
    return _get_chroma_collection(str(Path(storage["chromadb_path"]).resolve()))


# ─── Main Ingestion Entry Point ─────────────────────────────────
async def run_incremental_index(config: dict, force_reindex: bool = False) -> dict:
    """
    Run incremental indexing: discover files, detect changes, chunk, embed, upsert.

    For .yaml files: uses structural chunking (FCAPS × section).
    For .md files: uses generic text chunking.

    Args:
        config: Full config dict.
        force_reindex: If True, clear collection + state and re-index everything.

    Returns: {"processed": int, "skipped": int, "total": int, "errors": list}
    """
    storage = config["local_storage"]
    rag_params = config["rag_parameters"]
    timeout = config.get("request_timeout", 120)

    docs_path = Path(storage["docs_path"]).resolve()
    state_path = Path(storage["indexing_state_path"]).resolve()
    chromadb_path = str(Path(storage["chromadb_path"]).resolve())

    chunk_size = rag_params["chunk_size_tokens"]
    chunk_overlap = rag_params["chunk_overlap_tokens"]
    embedding_model = rag_params["embedding_model"]
    embedding_api = config["embedding_api_url"]
    metadata_fields = rag_params["metadata_fields"]

    # Load state and discover files
    state = _load_indexing_state(state_path)
    all_files = _discover_files(docs_path)

    # Do not open the collection BEFORE force_reindex deletion or Windows will lock the file.
    collection = None

    # Force reindex: delete chroma_data/ directory entirely and reset singleton
    if force_reindex:
        global _chroma_client, _chroma_collection
        logger.info("Force reindex: deleting chroma_data/ directory for clean re-embedding")
        # Reset ChromaDB singleton so it re-creates fresh
        _chroma_client = None
        _chroma_collection = None
        # Physically delete chroma_data/ directory
        chroma_dir = Path(chromadb_path)
        if chroma_dir.exists():
            try:
                shutil.rmtree(chroma_dir)
                logger.info(f"Deleted chroma_data directory: {chroma_dir}")
            except Exception as e:
                logger.warning(f"Failed to delete chroma_data/: {e}")
        # Delete indexing state file
        if state_path.exists():
            try:
                state_path.unlink()
                logger.info(f"Deleted indexing state: {state_path}")
            except Exception as e:
                logger.warning(f"Failed to delete indexing state: {e}")
        state = {}
        # Re-create fresh collection
        collection = _get_chroma_collection(chromadb_path)
    else:
        # If not forcing reindex, open the existing collection
        collection = _get_chroma_collection(chromadb_path)

    processed = 0
    skipped = 0
    errors = []

    for file_path in all_files:
        rel_key = str(file_path.relative_to(docs_path))

        if not force_reindex and not _needs_update(file_path, rel_key, state):
            skipped += 1
            continue

        try:
            content = file_path.read_text(encoding="utf-8")
            if not content.strip():
                skipped += 1
                continue

            # ── Dispatch: structured YAML vs generic text chunking ──
            is_yaml = file_path.suffix in (".yaml", ".yml")

            if is_yaml:
                # Structural chunking: FCAPS × section
                structured_chunks = _chunk_yaml_structured(
                    file_path, content, docs_path,
                    chunk_size, chunk_overlap, metadata_fields
                )
                if not structured_chunks:
                    skipped += 1
                    continue

                chunk_texts = [c["text"] for c in structured_chunks]
                chunk_metas = [c["metadata"] for c in structured_chunks]
            else:
                # Generic text chunking for .md files
                chunk_texts = _chunk_text(content, chunk_size, chunk_overlap)
                if not chunk_texts:
                    skipped += 1
                    continue
                base_meta = _extract_metadata(file_path, content, docs_path, metadata_fields)
                chunk_metas = [{**base_meta, "chunk_index": i} for i in range(len(chunk_texts))]

            # Get embeddings from remote API
            embeddings = await _get_embeddings(chunk_texts, embedding_api, embedding_model, timeout)

            # Prepare IDs (deterministic: file path + chunk index)
            ids = [f"{rel_key}::chunk_{i}" for i in range(len(chunk_texts))]

            # Delete old chunks for this file before upserting
            try:
                existing = collection.get(where={"source_file": rel_key})
                if existing and existing["ids"]:
                    collection.delete(ids=existing["ids"])
            except Exception:
                pass  # Collection might be empty

            # Add chunk_index to YAML structured chunks
            for i, meta in enumerate(chunk_metas):
                meta["chunk_index"] = i

            # Upsert to ChromaDB
            collection.upsert(
                ids=ids,
                documents=chunk_texts,
                embeddings=embeddings,
                metadatas=chunk_metas
            )

            # Update state
            state[rel_key] = {
                "mtime": file_path.stat().st_mtime,
                "hash": _file_hash(file_path),
                "chunks": len(chunk_texts)
            }
            processed += 1
            logger.info(f"Indexed: {rel_key} ({len(chunk_texts)} chunks)")

        except Exception as e:
            error_msg = f"{rel_key}: {str(e)}"
            errors.append(error_msg)
            logger.error(f"Failed to index {error_msg}")

    # Persist updated state
    _save_indexing_state(state_path, state)

    result = {
        "processed": processed,
        "skipped": skipped,
        "total": len(all_files),
        "errors": errors
    }
    logger.info(f"Indexing complete: {result}")
    return result
