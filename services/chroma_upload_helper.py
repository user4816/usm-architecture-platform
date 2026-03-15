"""
chroma_upload_helper.py — Subprocess wrapper for ChromaDB upload operations.

Runs in a SEPARATE PROCESS to avoid FFI/Rust segfaults on Windows.
The main process communicates via stdin (JSON) → stdout (JSON).

Supported actions:
  - count_files:  Count unique source files in collection
  - list_files:   List unique source filenames
  - upsert:       Upsert chunks with embeddings
  - reset:        Delete and recreate collection
"""

import sys
import json
import traceback

import chromadb
from chromadb.config import Settings as ChromaSettings


def get_client(chromadb_path: str):
    return chromadb.PersistentClient(
        path=chromadb_path,
        settings=ChromaSettings(anonymized_telemetry=False)
    )


def get_collection(chromadb_path: str, collection_name: str):
    client = get_client(chromadb_path)
    return client.get_or_create_collection(
        name=collection_name,
        metadata={"hnsw:space": "cosine"}
    )


def action_count_files(chromadb_path: str, collection_name: str, **_) -> dict:
    try:
        col = get_collection(chromadb_path, collection_name)
        result = col.get(include=["metadatas"])
        if result and result["metadatas"]:
            sources = set(m.get("source_file", "") for m in result["metadatas"])
            return {"count": len(sources)}
        return {"count": 0}
    except Exception:
        return {"count": 0, "error": traceback.format_exc()}


def action_list_files(chromadb_path: str, collection_name: str, **_) -> dict:
    try:
        col = get_collection(chromadb_path, collection_name)
        result = col.get(include=["metadatas"])
        if result and result["metadatas"]:
            sources = sorted(set(
                m.get("source_file", "") for m in result["metadatas"]
                if m.get("source_file")
            ))
            return {"files": sources}
        return {"files": []}
    except Exception:
        return {"files": [], "error": traceback.format_exc()}


def action_upsert(chromadb_path: str, collection_name: str,
                  ids: list, documents: list,
                  embeddings: list, metadatas: list, **_) -> dict:
    try:
        col = get_collection(chromadb_path, collection_name)
        batch_sz = 100
        for i in range(0, len(ids), batch_sz):
            col.upsert(
                ids=ids[i:i + batch_sz],
                documents=documents[i:i + batch_sz],
                embeddings=embeddings[i:i + batch_sz],
                metadatas=metadatas[i:i + batch_sz]
            )
        return {"ok": True, "count": len(ids)}
    except Exception:
        return {"ok": False, "error": traceback.format_exc()}


def action_delete_doc(chromadb_path: str, collection_name: str,
                      filename: str, **_) -> dict:
    """Delete all chunks for a specific source_file."""
    try:
        col = get_collection(chromadb_path, collection_name)
        col.delete(where={"source_file": filename})
        return {"ok": True, "deleted_file": filename}
    except Exception:
        return {"ok": False, "error": traceback.format_exc()}


def action_reset(chromadb_path: str, collection_name: str, **_) -> dict:
    try:
        client = get_client(chromadb_path)
        try:
            client.delete_collection(collection_name)
        except Exception:
            pass
        client.get_or_create_collection(
            name=collection_name,
            metadata={"hnsw:space": "cosine"}
        )
        return {"ok": True}
    except Exception:
        return {"ok": False, "error": traceback.format_exc()}


def main():
    raw = sys.stdin.read()
    request = json.loads(raw)
    action = request.get("action")

    handlers = {
        "count_files": action_count_files,
        "list_files": action_list_files,
        "upsert": action_upsert,
        "delete_doc": action_delete_doc,
        "reset": action_reset,
    }

    handler = handlers.get(action)
    if not handler:
        result = {"error": f"Unknown action: {action}"}
    else:
        result = handler(**request)

    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
