"""
ChromaDB Query Helper — runs in a subprocess to isolate ChromaDB's Rust FFI
from the uvicorn process, which crashes on Windows with access violation.

Usage:
    python services/chroma_query_helper.py <chromadb_path> <embedding_json> <n_results> [filter_json]

Output: JSON to stdout with query results
"""
import sys
import json
import chromadb
from chromadb.config import Settings
from pathlib import Path


def main():
    if len(sys.argv) < 4:
        print(json.dumps({"error": "Usage: chroma_query_helper.py <path> <embedding_json> <n_results> [filter_json]"}))
        sys.exit(1)

    chromadb_path = sys.argv[1]
    embedding = json.loads(sys.argv[2])
    n_results = int(sys.argv[3])
    where_filter = json.loads(sys.argv[4]) if len(sys.argv) > 4 and sys.argv[4] != "__none__" else None
    collection_name = sys.argv[5] if len(sys.argv) > 5 else "nbi_docs"

    try:
        client = chromadb.PersistentClient(
            path=chromadb_path,
            settings=Settings(anonymized_telemetry=False)
        )
        collection = client.get_or_create_collection(
            name=collection_name,
            metadata={"hnsw:space": "cosine"}
        )

        kwargs = {
            "query_embeddings": [embedding],
            "n_results": n_results
        }
        if where_filter:
            kwargs["where"] = where_filter

        try:
            results = collection.query(**kwargs)
        except Exception as e:
            # Retry without filter if filter fails
            results = collection.query(
                query_embeddings=[embedding],
                n_results=n_results
            )

        # Convert to JSON-safe output
        output = {
            "documents": results.get("documents", [[]]),
            "metadatas": results.get("metadatas", [[]]),
            "distances": results.get("distances", [[]])
        }
        print(json.dumps(output, ensure_ascii=False))

    except Exception as e:
        print(json.dumps({"error": str(e)}))
        sys.exit(1)


if __name__ == "__main__":
    main()
