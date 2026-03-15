"""
Chat Service - Hybrid RAG-based chat with dual-route query handling.

Route 1 (Spec Query): Direct YAML lookup for specific config values.
Route 2 (Semantic RAG): Full pipeline — embed → ChromaDB → rerank → LLM.

Features:
- Automatic Route 1 → Route 2 fallback when YAML lookup finds nothing
- Context-aware metadata filtering from UI (version/operator)
- All URLs, models, prompts driven by config.yaml
"""

import re
import os
import sys
import json
import asyncio
import logging
import subprocess
from pathlib import Path

import httpx
import yaml


logger = logging.getLogger(__name__)


# ─── Tokenizer for Prompt Budget (lazy-loaded singleton) ────────
_chat_tokenizer = None


def _get_chat_tokenizer(model_name: str = "BAAI/bge-m3"):
    """Lazy-load HuggingFace AutoTokenizer for accurate prompt token counting."""
    global _chat_tokenizer
    if _chat_tokenizer is None:
        try:
            from transformers import AutoTokenizer
            _chat_tokenizer = AutoTokenizer.from_pretrained(model_name)
            logger.info(f"[Tokenizer] Loaded chat tokenizer: {model_name}")
        except Exception as e:
            logger.warning(f"[Tokenizer] Failed to load AutoTokenizer ({e}); "
                           "falling back to len/4 estimate.")
            _chat_tokenizer = "fallback"
    return _chat_tokenizer


def _count_tokens(text: str) -> int:
    """Count the number of tokens in text using AutoTokenizer.
    Falls back to len(text)//4 if tokenizer is unavailable."""
    tok = _get_chat_tokenizer()
    if tok == "fallback":
        return len(text) // 4
    return len(tok.encode(text, add_special_tokens=False))


def _build_grouped_context(chunks: list[dict]) -> tuple[str, list[str]]:
    """Group retrieved chunks by source_file and sort by chunk_index within
    each group.  Returns (context_text, source_list).

    Output format:
        [Document: Filename_A.pdf]
        - chunk text 1 ...
        - chunk text 2 ...
        [Document: Filename_B.docx]
        - chunk text 3 ...
    """
    from collections import OrderedDict

    grouped: dict[str, list[dict]] = OrderedDict()
    for chunk in chunks:
        src = chunk.get("metadata", {}).get("source_file", "unknown")
        grouped.setdefault(src, []).append(chunk)

    # Sort each group by chunk_index to preserve document order
    for src in grouped:
        grouped[src].sort(
            key=lambda c: c.get("metadata", {}).get("chunk_index", 0)
        )

    parts: list[str] = []
    sources: list[str] = []
    for src, src_chunks in grouped.items():
        parts.append(f"\n[Document: {src}]")
        for chunk in src_chunks:
            parts.append(f"- {chunk['document']}")
        if src not in sources:
            sources.append(src)

    return "\n".join(parts), sources


def _truncate_context_by_token_budget(
    context_text: str,
    system_prompt: str,
    query: str,
    context_window: int,
    generation_buffer: int = 512,
) -> str:
    """Truncate context_text so the full prompt fits within context_window.

    Uses top-down budget allocation:
      budget = context_window - system_tokens - query_tokens - generation_buffer
    If context exceeds budget, truncate from the end.
    """
    system_tokens = _count_tokens(system_prompt)
    query_tokens = _count_tokens(query)
    overhead = system_tokens + query_tokens + generation_buffer
    budget = context_window - overhead

    if budget <= 0:
        logger.warning(f"[DIAG] Token budget exhausted: system={system_tokens}, "
                       f"query={query_tokens}, buffer={generation_buffer}, "
                       f"window={context_window} → budget={budget}")
        return ""

    context_tokens = _count_tokens(context_text)
    total_tokens = overhead + context_tokens
    print(f"[DIAG] Token usage: system={system_tokens} + query={query_tokens} + "
          f"context={context_tokens} + buffer={generation_buffer} = "
          f"{total_tokens} / {context_window}")

    if context_tokens <= budget:
        return context_text

    # Truncate: encode → slice → decode
    tok = _get_chat_tokenizer()
    if tok == "fallback":
        # Rough char-based truncation
        ratio = budget / max(context_tokens, 1)
        cut = int(len(context_text) * ratio)
        logger.warning(f"[DIAG] Context TRUNCATED (fallback): {context_tokens} → ~{budget} tokens")
        return context_text[:cut]

    token_ids = tok.encode(context_text, add_special_tokens=False)
    truncated_ids = token_ids[:budget]
    truncated = tok.decode(truncated_ids)
    logger.warning(f"[DIAG] Context TRUNCATED: {context_tokens} → {budget} tokens")
    print(f"[DIAG] Context TRUNCATED: {context_tokens} → {budget} tokens")
    return truncated

# Path to the subprocess query helper
_HELPER_SCRIPT = str(Path(__file__).parent / "chroma_query_helper.py")


# ─── Route Selection ─────────────────────────────────────────────

# Patterns that suggest a direct spec/config value lookup
_SPEC_PATTERNS = [
    r'\b(?:what|which)\s+(?:port|endpoint|path|url|address|protocol|timeout|version|schedule)',
    r'\b(?:show|get|find|tell)\s+(?:me\s+)?(?:the\s+)?(?:port|endpoint|path|url|address|protocol)',
    r'\bport\s+(?:number|value|for)\b',
    r'\b(?:sftp|rest|api)\s+port\b',
]

_SPEC_RE = re.compile('|'.join(_SPEC_PATTERNS), re.IGNORECASE)


def _is_spec_query(query: str) -> bool:
    """Check if the query is asking for a specific config/spec value."""
    return bool(_SPEC_RE.search(query))




# ─── Direct YAML Lookup (Route 1) ────────────────────────────────

def _flatten_yaml(data, prefix: str = "") -> dict:
    """Flatten a nested dict/list into dot-notated key-value pairs."""
    items = {}
    if isinstance(data, dict):
        for k, v in data.items():
            new_key = f"{prefix}.{k}" if prefix else str(k)
            items.update(_flatten_yaml(v, new_key))
    elif isinstance(data, list):
        for i, v in enumerate(data):
            new_key = f"{prefix}[{i}]"
            if isinstance(v, dict):
                # Also try to use 'name' field for more readable keys
                name = v.get("name", v.get("path", str(i)))
                readable_key = f"{prefix}.{name}" if name != str(i) else new_key
                items.update(_flatten_yaml(v, readable_key))
            else:
                items[new_key] = v
    else:
        items[prefix] = data
    return items


def _search_yaml_value(query: str, docs_path: Path,
                       context_version: str = None,
                       context_operator: str = None) -> dict | None:
    """
    Search YAML files for values matching the query keywords.
    Prioritizes files matching the UI context (version/operator).
    """
    # Extract search keywords from query
    keywords = _extract_search_keywords(query)
    if not keywords:
        return None

    # Discover YAML files, prioritize context-matching files
    yaml_files = sorted(docs_path.rglob("*.yaml"))
    if context_version and context_operator:
        # Move context-matching files to front
        context_files = []
        other_files = []
        for f in yaml_files:
            rel = str(f.relative_to(docs_path)).lower()
            if context_version.lower() in rel and context_operator.lower() in rel:
                context_files.append(f)
            else:
                other_files.append(f)
        yaml_files = context_files + other_files

    for yaml_path in yaml_files:
        try:
            content = yaml_path.read_text(encoding="utf-8")
            data = yaml.safe_load(content)
            if not isinstance(data, dict):
                continue

            flat = _flatten_yaml(data)
            # Search for keys matching the query keywords
            matches = []
            for key, value in flat.items():
                key_lower = key.lower()
                if all(kw in key_lower for kw in keywords):
                    matches.append({
                        "key": key,
                        "value": value,
                        "source": str(yaml_path.relative_to(docs_path))
                    })

            if matches:
                return {
                    "found": True,
                    "matches": matches[:5],  # Limit to top 5
                    "source_file": str(yaml_path.relative_to(docs_path))
                }
        except Exception as e:
            logger.debug(f"YAML parse error in {yaml_path}: {e}")
            continue

    return None


def _extract_search_keywords(query: str) -> list[str]:
    """Extract meaningful keywords from a spec query for YAML key matching."""
    # Remove common question words
    cleaned = re.sub(
        r'\b(what|which|show|get|find|tell|me|the|is|are|does|do|for|of|a|an|in)\b',
        '', query, flags=re.IGNORECASE
    )
    # Extract remaining meaningful words (min 2 chars)
    words = [w.lower().strip() for w in cleaned.split() if len(w.strip()) >= 2]
    # Broaden search: also match partial key names
    return words if words else []


# ─── Query Metadata Extraction ──────────────────────────────────

# Known values for metadata extraction from queries
_KNOWN_PACKAGES = {"24a", "25a", "25b", "26a", "26b"}
_KNOWN_OPERATORS = {"jio", "telus", "verizon"}

# Alias map for USM versions
_USM_ALIASES = {
    "v1": "USMv1", "v2": "USMv2",
    "usm1": "USMv1", "usm2": "USMv2",
    "usmv1": "USMv1", "usmv2": "USMv2",
    "usm v1": "USMv1", "usm v2": "USMv2",
}

# Operator display-name mapping (all lowercase keys)
_OPERATOR_DISPLAY = {"jio": "Jio", "telus": "TELUS", "verizon": "Verizon"}

# Regex to detect file path patterns like USMv2\26A\Jio\doc.yaml or USMv1/25B/Verizon
_PATH_RE = re.compile(
    r'(USMv[12])[\\/]([\dA-Za-z]{2,3})[\\/]([A-Za-z]+)',
    re.IGNORECASE
)


def _extract_query_metadata(query: str, chat_history: list = None) -> dict:
    """
    Parse the query text to detect which metadata fields are mentioned.
    Uses a global scanning approach — fields can appear in any order and
    at any position in the text. All matching is case-insensitive.

    Also scans the last few user messages from chat_history to carry over
    previously mentioned metadata (stateful context retention).
    New explicit values from the current query OVERRIDE historical ones,
    but historical values are preserved for fields NOT mentioned in the
    current query.

    Returns a dict with keys: usm_version, package, operator (values or None).
    """
    def _parse_single(text: str) -> dict:
        """Parse a single text string for metadata fields using global scan."""
        result = {"usm_version": None, "package": None, "operator": None}

        # ── 1) File path pattern (captures all 3 at once) ──
        path_match = _PATH_RE.search(text)
        if path_match:
            result["usm_version"] = "USMv" + path_match.group(1)[-1]
            result["package"] = path_match.group(2).upper()
            raw_op = path_match.group(3)
            result["operator"] = _OPERATOR_DISPLAY.get(raw_op.lower(), raw_op)
            # Don't return early — continue scanning for any extra metadata
            # that might not be in the file path

        # ── 2) USM Version (global scan, case-insensitive) ──
        if result["usm_version"] is None:
            # Explicit: USMv1, USMv2, USM v1, USM v2
            usm_match = re.search(r'\b(USM\s*v\s*[12])\b', text, re.IGNORECASE)
            if usm_match:
                raw = usm_match.group(1).replace(" ", "").lower()
                result["usm_version"] = _USM_ALIASES.get(raw, raw)
            else:
                # Short aliases: v1, v2, usm1, usm2
                alias_match = re.search(r'\b(v[12]|usm[12])\b', text, re.IGNORECASE)
                if alias_match:
                    result["usm_version"] = _USM_ALIASES.get(alias_match.group(1).lower())

        # ── 3) Package (global scan, case-insensitive) ──
        if result["package"] is None:
            pkg_match = re.search(r'\b(\d{2}[A-Ba-b])\b', text, re.IGNORECASE)
            if pkg_match:
                result["package"] = pkg_match.group(1).upper()

        # ── 4) Operator (global scan, case-insensitive word boundary) ──
        if result["operator"] is None:
            for op_key, op_display in _OPERATOR_DISPLAY.items():
                if re.search(r'\b' + re.escape(op_key) + r'\b', text, re.IGNORECASE):
                    result["operator"] = op_display
                    break

        return result

    # Parse the current query
    current = _parse_single(query)

    # Scan chat history: merge missing fields from recent user messages.
    # Current query's explicit values always take priority (override).
    if chat_history:
        user_msgs = [m["content"] for m in reversed(chat_history)
                     if m.get("role") == "user"][:4]
        for msg in user_msgs:
            historical = _parse_single(msg)
            for key in ("usm_version", "package", "operator"):
                if current[key] is None and historical[key] is not None:
                    current[key] = historical[key]
            if all(current[k] for k in ("usm_version", "package", "operator")):
                break

    return current


def _detect_path_query(query: str) -> dict | None:
    """
    If the query contains a file path (USMv2\\26A\\Jio\\doc.yaml),
    return a ChromaDB metadata filter that matches source_file prefix.
    Properly handles backslashes and enforces correct operator/package matching.
    Returns None if no path pattern detected.
    """
    match = _PATH_RE.search(query)
    if not match:
        return None

    usm = "USMv" + match.group(1)[-1]   # Normalize to USMv1 or USMv2
    pkg = match.group(2).upper()
    raw_op = match.group(3)
    op = _OPERATOR_DISPLAY.get(raw_op.lower(), raw_op)

    # Build the expected source_file prefix with backslashes
    # ChromaDB stores: "USMv2\26A\Jio\doc.yaml"
    source_prefix = f"{usm}\\{pkg}\\{op}\\"

    logger.info(f"[Path Route] Detected path -> usm={usm}, pkg={pkg}, op={op}, "
                f"source_prefix='{source_prefix}'")

    # Filter by package + operator metadata fields
    # Also add source_file prefix filter to ensure correct file
    return {
        "$and": [
            {"package": pkg},
            {"operator": op}
        ]
    }


def _build_smart_clarification(query: str, meta: dict) -> str:
    """
    Build a specific clarification message identifying which fields
    are present and which are missing.
    """
    present = []
    missing = []

    if meta["usm_version"]:
        present.append(f"USM Version = **{meta['usm_version']}**")
    else:
        missing.append("**USM Version** (e.g., USMv1 or USMv2)")

    if meta["package"]:
        present.append(f"Package = **{meta['package']}**")
    else:
        missing.append("**Package** (e.g., 24A, 25B, 26A)")

    if meta["operator"]:
        present.append(f"Operator = **{meta['operator']}**")
    else:
        missing.append("**Operator** (e.g., Jio, TELUS, Verizon)")

    if present:
        present_str = f"I've identified {', '.join(present)} from your query."
    else:
        present_str = "I couldn't identify any specific metadata from your query."

    if missing:
        missing_str = (f"I still need {', '.join(missing)} to provide "
                       "the exact specification from the documentation.")
    else:
        missing_str = ""

    parts = [present_str]
    if missing_str:
        parts.append(missing_str)
    parts.append("\nPlease try again with a more specific question, for example:")
    parts.append('- *"What is the SFTP port for USMv1 25B Jio?"*')
    parts.append('- *"Show USMv2 26A Verizon CM REST endpoints"*')

    return "\n".join(parts)



# ─── Context-Aware Metadata Filter ──────────────────────────────

def _extract_metadata_filter(query: str, context: dict = None) -> list[dict | None]:
    """
    Build a priority-ordered list of ChromaDB where-clauses.
    Returns [strict_filter, relaxed_filter_1, ..., None].
    The caller tries each in order until results are found.

    Filter priority (progressive relaxation):
    1. package + operator + fcaps + section  (most specific)
    2. package + operator + fcaps            (drop section)
    3. package + operator                    (drop fcaps)
    4. None                                  (no filter)
    """
    core = []    # critical: package + operator
    extra = []   # optional: fcaps, section

    # UI context filters (required when present)
    if context:
        ctx_version = context.get("context_version")
        ctx_operator = context.get("context_operator")
        if ctx_version:
            core.append({"package": ctx_version})
        if ctx_operator:
            core.append({"operator": ctx_operator})

    # Parse query for FCAPS hints (CM/FM/PM/SM)
    fcaps_match = re.search(r'\b(CM|FM|PM|SM)\b', query, re.IGNORECASE)
    if fcaps_match:
        extra.append({"fcaps": fcaps_match.group(1).upper()})

    # Parse query for section hints (rest/sftp)
    section_match = re.search(r'\b(rest|sftp)\b', query, re.IGNORECASE)
    if section_match:
        extra.append({"section": section_match.group(1).lower()})

    def _build(conditions: list[dict]) -> dict | None:
        if not conditions:
            return None
        if len(conditions) == 1:
            return conditions[0]
        return {"$and": conditions}

    # Build progressive filter list
    filters = []

    # Level 1: all conditions (most strict)
    all_conds = core + extra
    if all_conds:
        filters.append(_build(all_conds))

    # Level 2: core + fcaps only (drop section)
    if extra and any("section" in c for c in extra):
        no_section = core + [c for c in extra if "section" not in c]
        if no_section != all_conds:
            filters.append(_build(no_section) if no_section else None)

    # Level 3: core only (drop fcaps + section)
    if core and core != all_conds:
        filters.append(_build(core))

    # Level 4: no filter (fallback)
    filters.append(None)

    return filters


# ─── ChromaDB Query with Fallback ───────────────────────────────

async def _query_chromadb_subprocess(chromadb_path: str, query_embedding: list,
                                     top_k: int, where_filter: dict | None,
                                     collection_name: str = "nbi_docs") -> dict:
    """
    Run ChromaDB query in subprocess. Returns raw results dict.
    """
    cmd = [
        sys.executable, _HELPER_SCRIPT,
        chromadb_path,
        json.dumps(query_embedding),
        str(top_k)
    ]
    if where_filter:
        cmd.append(json.dumps(where_filter))
    else:
        cmd.append("__none__")
    # Always pass collection_name as 6th arg
    cmd.append(collection_name)

    empty = {"documents": [[]], "metadatas": [[]], "distances": [[]]}
    try:
        proc_env = {**os.environ, "PYTHONUTF8": "1", "ANONYMIZED_TELEMETRY": "false"}
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=proc_env
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
        if proc.returncode != 0:
            logger.warning(f"ChromaDB subprocess error: {stderr.decode('utf-8', errors='replace')}")
            return empty
        results = json.loads(stdout.decode("utf-8"))
        if "error" in results:
            logger.warning(f"ChromaDB query error: {results['error']}")
            return empty
        return results
    except asyncio.TimeoutError:
        logger.warning("ChromaDB subprocess timed out")
        return empty
    except Exception as e:
        logger.warning(f"ChromaDB subprocess failed: {e}")
        return empty


async def _query_chromadb_with_fallback(
    chromadb_path: str, query_embedding: list, top_k: int,
    filter_chain: list[dict | None]
) -> tuple[dict, dict | None]:
    """
    Try each filter in filter_chain; return results from the first that yields > 0 docs.
    Returns: (results_dict, filter_used)
    """
    for filt in filter_chain:
        results = await _query_chromadb_subprocess(
            chromadb_path, query_embedding, top_k, filt
        )
        if results["documents"] and results["documents"][0]:
            print(f"[DIAG] ChromaDB hit with filter={filt}, docs={len(results['documents'][0])}")
            return results, filt
        else:
            print(f"[DIAG] ChromaDB miss with filter={filt}, relaxing...")

    # All filters exhausted
    empty = {"documents": [[]], "metadatas": [[]], "distances": [[]]}
    return empty, None


# ─── Embedding via Remote API ───────────────────────────────────

async def _embed_query(query: str, api_url: str, model: str,
                       timeout: int = 120) -> list[float]:
    """Get embedding vector for a single query string."""
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(api_url, json={
            "model": model,
            "input": [query]
        })
        response.raise_for_status()
        data = response.json()
        embeddings = data.get("embeddings", [])
        if not embeddings:
            raise ValueError("No embedding returned")
        return embeddings[0]


# ─── Reranking via Remote API ───────────────────────────────────

async def _rerank_chunks(query: str, chunks: list[dict], api_url: str,
                         timeout: int = 120) -> list[dict]:
    """
    Call remote reranker to re-score retrieved chunks.
    Expected API: POST {query, passages} -> [{index, score}]
    """
    if not chunks:
        return chunks

    passages = [c["document"] for c in chunks]

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(api_url, json={
                "query": query,
                "documents": passages
            })
            response.raise_for_status()
            scores = response.json()

            # Sort by score descending
            if isinstance(scores, list):
                scored = []
                for item in scores:
                    idx = item.get("index", 0)
                    score = item.get("score", 0)
                    if idx < len(chunks):
                        chunks[idx]["rerank_score"] = score
                        scored.append(chunks[idx])
                scored.sort(key=lambda x: x.get("rerank_score", 0), reverse=True)
                return scored

    except Exception as e:
        logger.warning(f"Reranker call failed ({e}); using original order.")

    return chunks


# ─── LLM API Resolution ────────────────────────────────────────

def _resolve_llm_api(model: str, config: dict) -> dict:
    """
    Resolve LLM API URL and key from the llm_apis config based on model name.
    Returns {"api_url": str, "api_key": str} or raises ValueError.
    """
    llm_apis = config.get("llm_apis", {})
    for provider, info in llm_apis.items():
        if model in info.get("models", []):
            url = info.get("api_url", "")
            if not url:
                raise ValueError(
                    f"API URL not configured for model '{model}' (provider: {provider}). "
                    f"Please configure the API URL in Settings."
                )
            return {"api_url": url, "api_key": info.get("api_key", "")}
    # Fallback: no matching provider found
    # (removed legacy rag_environments fallback)
    raise ValueError(f"No API configuration found for model '{model}'.")


# ─── LLM Generation via Remote API ─────────────────────────────

async def _call_llm(system_prompt: str, user_prompt: str, model: str,
                    api_url: str, timeout: int = 120,
                    temperature: float = 0.3, context_window: int = 8192) -> str:
    """Call remote LLM API (Ollama /api/chat format with messages)."""
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(api_url, json={
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_ctx": context_window
            }
        })
        response.raise_for_status()
        data = response.json()
        msg = data.get("message", {})
        return msg.get("content", "") if isinstance(msg, dict) else str(msg)


async def _call_llm_stream(system_prompt: str, user_prompt: str, model: str,
                           api_url: str, timeout: int = 120,
                           temperature: float = 0.3, context_window: int = 8192):
    """
    Streaming LLM call — async generator yielding content chunks.
    Aborts the HTTP request on asyncio.CancelledError to free GPU resources.
    """
    client = httpx.AsyncClient(timeout=timeout)
    try:
        async with client.stream("POST", api_url, json={
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "stream": True,
            "options": {
                "temperature": temperature,
                "num_ctx": context_window
            }
        }) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if not line.strip():
                    continue
                try:
                    chunk = json.loads(line)
                except json.JSONDecodeError:
                    continue
                # Ollama streaming: {"message":{"content":"..."}, "done":false}
                msg = chunk.get("message", {})
                content = msg.get("content", "") if isinstance(msg, dict) else ""
                if content:
                    yield content
                if chunk.get("done", False):
                    break
    except asyncio.CancelledError:
        logger.info("Client disconnected — aborting LLM stream to free GPU")
        raise
    finally:
        await client.aclose()


# ─── SSE Helpers ────────────────────────────────────────────────

def _sse_event(event: str, data: str) -> str:
    """Format a single SSE frame. Multi-line data is split per SSE spec."""
    lines = data.split("\n")
    data_part = "\n".join(f"data: {line}" for line in lines)
    return f"event: {event}\n{data_part}\n\n"


# ─── RAG Pipeline (Route 2) ─────────────────────────────────────

async def _rag_pipeline(query: str, model: str, config: dict,
                        context: dict = None) -> dict:
    """
    Full RAG pipeline:
    1. Embed query
    2. Search ChromaDB (with fallback filter relaxation)
    3. Rerank results
    4. Construct prompt with system_prompt + context
    5. Call LLM
    """
    rag_params = config["rag_parameters"]
    embedding_api_url = config["embedding_api_url"]
    reranker_api_url = config["reranker_api_url"]
    timeout = config.get("request_timeout", 120)

    embedding_model = rag_params["embedding_model"]
    llm_model = model or rag_params["llm_model"]
    system_prompt = rag_params["system_prompt"]
    top_k = rag_params["top_k_retrieval"]
    temperature = rag_params.get("temperature", 0.3)
    context_window = rag_params.get("context_window", 8192)

    # Resolve LLM API URL from llm_apis config
    llm_api = _resolve_llm_api(llm_model, config)
    llm_api_url = llm_api["api_url"]

    # Step 1: Embed query
    query_embedding = await _embed_query(
        query, embedding_api_url, embedding_model, timeout
    )

    # Step 2: Search ChromaDB (full-database, no metadata filter)
    storage = config["local_storage"]
    chromadb_path = str(Path(storage["chromadb_path"]).resolve())

    results = await _query_chromadb_subprocess(
        chromadb_path, query_embedding, top_k, None
    )

    # Format results
    chunks = []
    if results and results["documents"] and results["documents"][0]:
        for i, doc in enumerate(results["documents"][0]):
            chunk = {
                "document": doc,
                "metadata": results["metadatas"][0][i] if results["metadatas"] else {},
                "distance": results["distances"][0][i] if results["distances"] else 0
            }
            chunks.append(chunk)

    # Step 3: Rerank and keep top_k_rerank
    top_k_rerank = rag_params.get("top_k_rerank", 7)
    if chunks:
        chunks = await _rerank_chunks(query, chunks, reranker_api_url, timeout)
        chunks = chunks[:top_k_rerank]

    # Step 4: Build prompt
    context_text, sources = _build_grouped_context(chunks)

    # ── Top-down token budget: truncate context to fit within window ──
    context_text = _truncate_context_by_token_budget(
        context_text, system_prompt, query, context_window
    )

    full_prompt = f"""Context from documentation:
{context_text if context_text else "(No relevant documents found)"}

User question: {query}"""

    # Step 5: Call LLM
    reply = await _call_llm(system_prompt, full_prompt, llm_model, llm_api_url,
                            timeout, temperature=temperature,
                            context_window=context_window)

    return {
        "reply": reply,
        "sources": sources,
        "route_used": "semantic_rag",
        "chunks_retrieved": len(chunks)
    }


async def _rag_pipeline_stream(query: str, model: str, config: dict,
                               context: dict = None, meta: dict = None):
    """
    Streaming RAG pipeline — async generator yielding SSE events.
    Sends status updates during retrieval so the user sees progress.
    """
    rag_params = config["rag_parameters"]
    embedding_api_url = config["embedding_api_url"]
    reranker_api_url = config["reranker_api_url"]
    timeout = config.get("request_timeout", 120)

    embedding_model = rag_params["embedding_model"]
    llm_model = model or rag_params["llm_model"]
    system_prompt = rag_params["system_prompt"]
    top_k = rag_params["top_k_retrieval"]
    temperature = rag_params.get("temperature", 0.3)
    context_window = rag_params.get("context_window", 8192)

    # Resolve LLM API URL from llm_apis config
    llm_api = _resolve_llm_api(llm_model, config)
    llm_api_url = llm_api["api_url"]

    # ── Status: searching ──
    yield _sse_event("status", "Searching documents...")

    # Step 1: Embed query
    query_embedding = await _embed_query(
        query, embedding_api_url, embedding_model, timeout
    )

    # Step 2: Search ChromaDB (full-database, no metadata filter)
    storage = config["local_storage"]
    chromadb_path = str(Path(storage["chromadb_path"]).resolve())

    print(f"[DIAG] Searching full collection (no filter), top_k={top_k}")

    results = await _query_chromadb_subprocess(
        chromadb_path, query_embedding, top_k, None
    )

    # Format results
    chunks = []
    if results and results["documents"] and results["documents"][0]:
        for i, doc in enumerate(results["documents"][0]):
            chunk = {
                "document": doc,
                "metadata": results["metadatas"][0][i] if results["metadatas"] else {},
                "distance": results["distances"][0][i] if results["distances"] else 0
            }
            chunks.append(chunk)

    # ── Diagnostic: log retrieved chunks ──
    print(f"[DIAG] Retrieved {len(chunks)} chunks:")
    for i, c in enumerate(chunks):
        src = c.get('metadata', {}).get('source_file', '?')
        fcaps = c.get('metadata', {}).get('fcaps', '?')
        section = c.get('metadata', {}).get('section', '?')
        dist = c.get('distance', 0)
        preview = c['document'][:120].replace('\n', ' ')
        print(f"  [{i}] dist={dist:.4f} src={src} fcaps={fcaps} section={section}")
        print(f"       preview: {preview}")

    # Step 3: Rerank and keep top_k_rerank
    top_k_rerank = rag_params.get("top_k_rerank", 7)
    yield _sse_event("status", "Analyzing relevance...")
    if chunks:
        chunks = await _rerank_chunks(query, chunks, reranker_api_url, timeout)
        chunks = chunks[:top_k_rerank]

    # Step 4: Build prompt
    context_text, sources = _build_grouped_context(chunks)

    # Inject metadata context — FORCEFULLY tell LLM which fields are identified
    meta_context = ""
    if meta:
        identified = []
        missing = []
        field_labels = {
            "usm_version": "USM Version",
            "package": "Package",
            "operator": "Operator"
        }
        for key, label in field_labels.items():
            if meta.get(key):
                identified.append(f"{label}: {meta[key]}")
            else:
                missing.append(label)

        if identified:
            meta_context = (
                "\n[IDENTIFIED METADATA — DO NOT ask for these fields again]: "
                + ", ".join(identified) + "\n"
            )
            if missing:
                meta_context += (
                    "If necessary to answer the question accurately, "
                    "please naturally ask the user for: "
                    + ", ".join(missing) + ".\n"
                )
            else:
                meta_context += (
                    "[ALL fields identified — answer directly using the retrieved context.]\n"
                )

    # ── Top-down token budget: truncate context to fit within window ──
    context_text = _truncate_context_by_token_budget(
        context_text, system_prompt, query + meta_context, context_window
    )

    full_prompt = f"""Context from documentation:
{context_text if context_text else "(No relevant documents found)"}
{meta_context}
User question: {query}"""

    # ── Diagnostic: verify context is not empty ──
    ctx_preview = context_text[:200].replace('\n', ' ') if context_text else '(EMPTY)'
    print(f"[DIAG] context_text length={len(context_text)}, preview: {ctx_preview}")

    # ── Meta event: sources & route ──
    yield _sse_event("meta", json.dumps({
        "sources": sources,
        "route_used": "semantic_rag",
        "model_used": llm_model
    }))

    # ── Status: generating ──
    yield _sse_event("status", "Generating answer...")

    # Step 5: Stream LLM tokens
    full_reply = []
    try:
        async for token in _call_llm_stream(
            system_prompt, full_prompt, llm_model, llm_api_url, timeout,
            temperature=temperature, context_window=context_window
        ):
            full_reply.append(token)
            yield _sse_event("token", token)
    except asyncio.CancelledError:
        logger.info("RAG stream cancelled by client disconnect")
        raise

    # ── Done event: include context for conversation history ──
    yield _sse_event("done", json.dumps({
        "context": [
            {"role": "user", "content": query},
            {"role": "assistant", "content": "".join(full_reply)}
        ]
    }))


# ─── Main Entry Point ───────────────────────────────────────────

async def handle_chat(query: str, model: str, config: dict,
                      context: dict = None) -> dict:
    """
    Handle a chat query with dual-route logic + automatic fallback.

    Args:
        query: User's question
        model: Selected LLM model name
        config: Full config dict from config.yaml
        context: Optional UI context {context_version, context_operator}

    Returns: {reply, sources, route_used, model_used}
    """
    if not query or not query.strip():
        return {"reply": "Please enter a question.", "sources": [], "route_used": "none", "model_used": model}

    docs_path = Path(config["local_storage"]["docs_path"]).resolve()

    # Route 1: Try spec query (direct YAML lookup)
    if _is_spec_query(query):
        logger.info(f"Route 1 (Spec Query) triggered for: {query}")
        result = _search_yaml_value(
            query, docs_path,
            context_version=context.get("context_version") if context else None,
            context_operator=context.get("context_operator") if context else None
        )
        if result and result.get("found"):
            # Format the direct answer
            matches = result["matches"]
            answer_parts = []
            sources = []
            for m in matches:
                answer_parts.append(f"**{m['key']}**: `{m['value']}`")
                if m["source"] not in sources:
                    sources.append(m["source"])

            return {
                "reply": "\n".join(answer_parts),
                "sources": sources,
                "route_used": "spec_lookup",
                "model_used": "direct_lookup"
            }
        else:
            # Fallback: Route 1 matched pattern but found nothing → Route 2
            logger.info("Route 1 found no results; falling back to Route 2 (RAG)")

    # Route 2: Semantic RAG pipeline
    try:
        logger.info(f"Route 2 (Semantic RAG) for: {query}")
        result = await _rag_pipeline(query, model, config, context)
        result["model_used"] = model
        return result
    except Exception as e:
        logger.error(f"RAG pipeline error: {e}")
        return {
            "reply": f"An error occurred while processing your query: {str(e)}",
            "sources": [],
            "route_used": "error",
            "model_used": model
        }



async def handle_chat_stream(query: str, model: str, config: dict,
                             context: dict = None,
                             chat_history: list = None):
    """
    Streaming entry point — async generator yielding SSE events.

    Route 0 (path_filter): direct metadata filter for file path queries.
    Route 1 (spec_lookup): yields result instantly (no LLM needed).
    Route 2 (semantic_rag): delegates to _rag_pipeline_stream.
    Errors: yields event: error.
    """
    if not query or not query.strip():
        yield _sse_event("token", "Please enter a question.")
        yield _sse_event("done", json.dumps({"context": []}))
        return

    # ── Ignore UI sidebar selections — rely purely on chat input ──
    context = None

    # ── Greeting bypass — skip RAG for simple greetings ──
    _GREETINGS = {"hi", "hello", "hey", "good morning", "good afternoon",
                  "good evening", "what's your name", "who are you",
                  "안녕", "안녕하세요"}
    if query.strip().lower().rstrip("?!.") in _GREETINGS:
        greeting_reply = (
            "Hello! I'm the USM NBI Assistant. "
            "Ask me anything about NBI specifications — for example:\n\n"
            "- *\"Show 26B Verizon CM REST endpoints\"*\n"
            "- *\"What is the SFTP port for 24A Jio?\"*\n\n"
            "Please include the **USM Version**, **Package**, and **Operator** in your question "
            "so I can give you an accurate answer."
        )
        yield _sse_event("token", greeting_reply)
        yield _sse_event("done", json.dumps({
            "context": [
                {"role": "user", "content": query},
                {"role": "assistant", "content": greeting_reply}
            ]
        }))
        return

    # ── Extract metadata from current query + chat history ──
    meta = _extract_query_metadata(query, chat_history=chat_history)
    print(f"[DEBUG handle_chat_stream] query='{query}', model='{model}', "
          f"meta={meta}, is_spec={_is_spec_query(query)}")

    docs_path = Path(config["local_storage"]["docs_path"]).resolve()


    # ── Route 0: Path-to-filter — direct metadata lookup for file path queries ──
    path_filter = _detect_path_query(query)
    if path_filter:
        logger.info(f"Route 0 (Path Filter) triggered for: {query}, filter={path_filter}")
        yield _sse_event("status", "Searching by document path...")

        # Pass the path filter to _rag_pipeline_stream to restrict search scope
        # Wrap query to instruct LLM to summarize the file content
        path_meta = _extract_query_metadata(query)
        path_context_str = (
            f"{path_meta.get('usm_version', '')} "
            f"{path_meta.get('package', '')} "
            f"{path_meta.get('operator', '')}"
        ).strip()
        augmented_query = (
            f"Summarize the content of the document from {path_context_str}. "
            f"List the available sections (e.g., CM, FM, PM) and briefly describe "
            f"what each section covers. Original user query: {query}"
        )

        try:
            async for event in _rag_pipeline_stream(
                    augmented_query, model, config, context, meta=meta):
                yield event
            return
        except Exception as e:
            logger.error(f"Route 0 pipeline error: {e}")
            # Fall through to Route 2


    # Route 1: Try spec query (direct YAML lookup)
    if _is_spec_query(query):
        logger.info(f"Route 1 (Spec Query) triggered for: {query}")
        result = _search_yaml_value(
            query, docs_path,
            context_version=None,
            context_operator=None
        )
        if result and result.get("found"):
            matches = result["matches"]
            answer_parts = []
            sources = []
            for m in matches:
                source_path = m["source"]
                # Wrap each match in a descriptive sentence
                answer_parts.append(
                    f"The **{m['key']}** from `{source_path}` is `{m['value']}`."
                )
                if source_path not in sources:
                    sources.append(source_path)

            reply_text = "\n\n".join(answer_parts)
            yield _sse_event("meta", json.dumps({
                "sources": sources,
                "route_used": "spec_lookup",
                "model_used": "direct_lookup"
            }))
            yield _sse_event("token", reply_text)
            yield _sse_event("done", json.dumps({
                "context": [
                    {"role": "user", "content": query},
                    {"role": "assistant", "content": reply_text}
                ]
            }))
            return
        else:
            logger.info("Route 1 found no results; falling back to Route 2 (RAG)")

    # Route 2: Semantic RAG pipeline (streaming)
    try:
        logger.info(f"Route 2 (Semantic RAG, streaming) for: {query}")
        async for event in _rag_pipeline_stream(query, model, config, context,
                                                     meta=meta):
            yield event
    except asyncio.CancelledError:
        logger.info("Stream cancelled by client")
        raise
    except Exception as e:
        logger.error(f"RAG pipeline stream error: {e}")
        yield _sse_event("error", str(e))
        yield _sse_event("done", json.dumps({"context": []}))


# ─── Upload Chat Pipeline (Completely Isolated) ────────────────

async def handle_upload_chat_stream(query: str, model: str, config: dict,
                                     chat_history: list = None):
    """
    Streaming chat for uploaded documents — COMPLETELY ISOLATED from NBI specs.

    - NO Route 0 (path filter)
    - NO Route 1 (spec lookup)
    - Queries ONLY 'temp_uploaded_docs' collection
    - Uses upload_rag_settings config (system_prompt, temperature, top_k)
    """
    if not query or not query.strip():
        yield _sse_event("token", "Please enter a question.")
        yield _sse_event("done", json.dumps({"context": []}))
        return

    # ── Greeting bypass ──
    _GREETINGS = {"hi", "hello", "hey", "good morning", "good afternoon",
                  "good evening", "안녕", "안녕하세요"}
    if query.strip().lower().rstrip("?!.") in _GREETINGS:
        greeting_reply = (
            "Hello! I'm the Document Assistant. "
            "Ask me anything about the documents you've uploaded.\n\n"
            "For example:\n"
            "- *\"Summarize the main topics\"*\n"
            "- *\"What does section 3 discuss?\"*"
        )
        yield _sse_event("token", greeting_reply)
        yield _sse_event("done", json.dumps({
            "context": [
                {"role": "user", "content": query},
                {"role": "assistant", "content": greeting_reply}
            ]
        }))
        return

    # ── Load upload-specific configuration ──
    upload_cfg = config["upload_rag_settings"]
    embedding_api_url = config["embedding_api_url"]
    reranker_api_url = config["reranker_api_url"]
    timeout = config.get("request_timeout", 120)

    embedding_model = config["rag_parameters"]["embedding_model"]
    llm_model = model or config["rag_parameters"]["llm_model"]
    system_prompt = upload_cfg["system_prompt"]
    top_k = upload_cfg.get("top_k_retrieval", 30)
    top_k_rerank = upload_cfg.get("top_k_rerank", 8)
    temperature = upload_cfg.get("temperature", 0.1)
    context_window = upload_cfg.get("context_window", 8000)
    collection_name = upload_cfg.get("collection_name", "temp_uploaded_docs")

    # Resolve LLM API URL
    llm_api = _resolve_llm_api(llm_model, config)
    llm_api_url = llm_api["api_url"]

    try:
        # ── Status: searching uploaded docs ──
        yield _sse_event("status", "Searching uploaded documents...")

        # Step 1: Embed query
        query_embedding = await _embed_query(
            query, embedding_api_url, embedding_model, timeout
        )

        # Step 2: Search ChromaDB — ONLY temp_uploaded_docs collection
        storage = config["local_storage"]
        chromadb_path = str(Path(storage["chromadb_path"]).resolve())

        logger.info(f"[Upload Chat] Querying collection='{collection_name}', top_k={top_k}")
        results = await _query_chromadb_subprocess(
            chromadb_path, query_embedding, top_k, None,
            collection_name=collection_name
        )

        # Format results
        chunks = []
        if results and results["documents"] and results["documents"][0]:
            for i, doc in enumerate(results["documents"][0]):
                chunk = {
                    "document": doc,
                    "metadata": results["metadatas"][0][i] if results["metadatas"] else {},
                    "distance": results["distances"][0][i] if results["distances"] else 0
                }
                chunks.append(chunk)

        if not chunks:
            no_docs_msg = (
                "No relevant content was found in the uploaded documents. "
                "Please make sure you have uploaded documents and try rephrasing your question."
            )
            yield _sse_event("token", no_docs_msg)
            yield _sse_event("done", json.dumps({
                "context": [
                    {"role": "user", "content": query},
                    {"role": "assistant", "content": no_docs_msg}
                ]
            }))
            return

        # Step 3: Rerank (30 candidates → top 8)
        yield _sse_event("status", "Analyzing relevance...")
        pre_rerank_chunks = list(chunks)  # keep copy for fallback
        try:
            chunks = await _rerank_chunks(query, chunks, reranker_api_url, timeout)
            chunks = chunks[:top_k_rerank]
        except Exception as e:
            import traceback
            logger.error(f"[Upload Chat] Reranker FAILED: {e}\n{traceback.format_exc()}")
            chunks = pre_rerank_chunks[:top_k_rerank]
            yield _sse_event("status", "Reranker unavailable, using retrieval order...")

        # Safety: if reranker dropped everything, fallback to top 3 from retrieval
        if not chunks and pre_rerank_chunks:
            logger.warning("[Upload Chat] Reranker returned 0 docs — fallback to top 3 retrieval")
            chunks = pre_rerank_chunks[:3]

        # Step 4: Build prompt (NO metadata injection — purely document-based)
        # NOTE: No main-tab metadata variables (operator, version, package,
        # where_filter) are used here.  Upload docs lack this metadata so
        # all filters are explicitly None to avoid accidental blocking.
        context_text, sources = _build_grouped_context(chunks)

        # ── Top-down token budget: truncate context to fit within window ──
        context_text = _truncate_context_by_token_budget(
            context_text, system_prompt, query, context_window
        )

        full_prompt = f"""Context from uploaded documents:
{context_text}

User question: {query}"""

        # ── Meta event ──
        yield _sse_event("meta", json.dumps({
            "sources": sources,
            "route_used": "upload_rag",
            "model_used": llm_model
        }))

        # ── Status: generating ──
        yield _sse_event("status", "Generating answer...")

        # Step 5: Stream LLM tokens
        full_reply = []
        async for token in _call_llm_stream(
            system_prompt, full_prompt, llm_model, llm_api_url, timeout,
            temperature=temperature, context_window=context_window
        ):
            full_reply.append(token)
            yield _sse_event("token", token)

        # ── Done event ──
        yield _sse_event("done", json.dumps({
            "context": [
                {"role": "user", "content": query},
                {"role": "assistant", "content": "".join(full_reply)}
            ]
        }))

    except asyncio.CancelledError:
        logger.info("Upload chat stream cancelled by client")
        raise
    except Exception as e:
        logger.error(f"Upload chat error: {e}")
        yield _sse_event("error", str(e))
        yield _sse_event("done", json.dumps({"context": []}))
