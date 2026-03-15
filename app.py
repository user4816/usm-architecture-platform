"""
USM AI Platform - Main Application
FastAPI backend serving NBI Management, with extensible module routing.
"""

import os
import re
import sys
import yaml
import json
import logging
import asyncio
import tempfile
import faulthandler
from pathlib import Path
from contextlib import asynccontextmanager

# Enable faulthandler to capture C-level crashes (segfaults, etc.)
faulthandler.enable()

from fastapi import FastAPI, HTTPException, Request, Query, UploadFile, File, APIRouter
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from services.doc_renderer import render_document, validate_yaml_content
from services.pdf_generator import generate_pdf, shutdown_browser
from services.diff_service import (
    compute_text_diff, compute_yaml_diff,
    render_diff_html, render_semantic_diff_html
)
from services.chat_service import handle_chat, handle_chat_stream, handle_upload_chat_stream
from services.ingestion_service import run_incremental_index
from services.upload_service import (
    process_upload, get_uploaded_file_count, get_uploaded_files,
    reset_collection, delete_doc, create_task, get_progress, cleanup_task
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent
with open(BASE_DIR / "config.yaml", "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f)

DATA_ROOT = (BASE_DIR / CONFIG["data_root_path"]).resolve()
SERVER_PORT = CONFIG["server"]["port"]
SERVER_HOST = CONFIG["server"]["host"]
PDF_MARGINS = CONFIG["pdf"]["margins"]

VALID_USM_VERSIONS = ["USMv1", "USMv2"]


def validate_usm_version(usm_version: str) -> str:
    """Strict validation: only allow known USM versions."""
    if usm_version not in VALID_USM_VERSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid usm_version '{usm_version}'. Must be one of {VALID_USM_VERSIONS}."
        )
    return usm_version


async def _background_index():
    """Run incremental indexing in the background (non-blocking)."""
    try:
        logger.info("Background indexing started...")
        result = await run_incremental_index(CONFIG)
        logger.info(f"Background indexing complete: {result}")
    except Exception as e:
        logger.warning(f"Background indexing failed (non-fatal): {e}")


@asynccontextmanager
async def lifespan(application: FastAPI):
    # Clear upload collection on startup — ensures zero pre-loaded docs
    try:
        reset_collection(CONFIG)
        logger.info("Upload collection 'temp_uploaded_docs' cleared on startup.")
    except Exception as e:
        # First install: collection may not exist yet — safe to ignore
        logger.info(f"Upload collection reset skipped (first install?): {e}")

    # Launch indexing as a background task — server starts immediately
    task = asyncio.create_task(_background_index())
    yield
    task.cancel()
    await shutdown_browser()


app = FastAPI(title="USM AI Platform", version="3.0.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# ─── Module Routers (for future endpoint separation) ─────────
nbi_router = APIRouter(prefix="/api", tags=["NBI Management"])


def path_safeguard(value: str, param_name: str = "parameter") -> None:
    """Prevent path traversal attacks."""
    if not value or not value.strip():
        raise HTTPException(status_code=400, detail=f"'{param_name}' is empty.")
    if ".." in value:
        raise HTTPException(status_code=400, detail=f"'{param_name}' contains disallowed path traversal '..'.")
    if os.path.isabs(value) or re.match(r'^[a-zA-Z]:', value) or value.startswith(('/', '\\\\')):
        raise HTTPException(status_code=400, detail=f"'{param_name}' contains an absolute path.")
    if '/' in value or '\\' in value:
        raise HTTPException(status_code=400, detail=f"'{param_name}' contains disallowed path separators.")


def get_doc_dir(pkg: str, operator: str, usm_version: str = "USMv1") -> Path:
    """Return validated package/operator directory path under the given USM version."""
    validate_usm_version(usm_version)
    path_safeguard(pkg, "pkg")
    path_safeguard(operator, "operator")
    doc_dir = (DATA_ROOT / usm_version / pkg / operator).resolve()
    try:
        doc_dir.relative_to(DATA_ROOT)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Disallowed path: {usm_version}/{pkg}/{operator}")
    if not doc_dir.exists():
        raise HTTPException(status_code=404, detail=f"Path not found: {usm_version}/{pkg}/{operator}")
    return doc_dir


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/packages")
async def get_packages(usm_version: str = Query("USMv1")):
    """Return package/operator tree from data directory for the given USM version."""
    validate_usm_version(usm_version)
    tree = []
    version_root = DATA_ROOT / usm_version
    if not version_root.exists():
        return JSONResponse(content={"usm_version": usm_version, "packages": tree})
    for pkg_dir in sorted(version_root.iterdir(), reverse=True):
        if pkg_dir.is_dir():
            operators = []
            for op_dir in sorted(pkg_dir.iterdir()):
                if op_dir.is_dir():
                    files = [f.name for f in sorted(op_dir.iterdir()) if f.is_file()]
                    operators.append({"name": op_dir.name, "files": files})
            tree.append({"name": pkg_dir.name, "operators": operators})
    return JSONResponse(content={"usm_version": usm_version, "packages": tree})


@app.get("/api/doc/{pkg}/{operator}")
async def get_doc(pkg: str, operator: str, usm_version: str = Query("USMv1")):
    """Return doc.md content."""
    doc_dir = get_doc_dir(pkg, operator, usm_version)
    doc_path = doc_dir / "doc.md"
    if not doc_path.exists():
        raise HTTPException(status_code=404, detail="doc.md not found.")
    try:
        content = doc_path.read_text(encoding="utf-8")
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": "File read error", "detail": str(e)})
    return JSONResponse(content={"content": content, "filename": "doc.md"})


@app.put("/api/doc/{pkg}/{operator}")
async def save_doc(pkg: str, operator: str, request: Request, usm_version: str = Query("USMv1")):
    """Save doc.md content."""
    doc_dir = get_doc_dir(pkg, operator, usm_version)
    body = await request.json()
    content = body.get("content", "")
    try:
        (doc_dir / "doc.md").write_text(content, encoding="utf-8")
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": "File save error", "detail": str(e)})
    return JSONResponse(content={"status": "success", "message": "doc.md saved."})


@app.get("/api/files/{pkg}/{operator}/{filename}")
async def get_file(pkg: str, operator: str, filename: str, usm_version: str = Query("USMv1")):
    """Return individual file content."""
    path_safeguard(filename, "filename")
    doc_dir = get_doc_dir(pkg, operator, usm_version)
    file_path = (doc_dir / filename).resolve()
    try:
        file_path.relative_to(doc_dir)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Disallowed file path: {filename}")
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {filename}")
    try:
        content = file_path.read_text(encoding="utf-8")
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": "File read error", "detail": str(e)})
    return JSONResponse(content={"content": content, "filename": filename})


@app.put("/api/files/{pkg}/{operator}/{filename}")
async def save_file(pkg: str, operator: str, filename: str, request: Request, usm_version: str = Query("USMv1")):
    """Save individual file. Validates YAML files."""
    path_safeguard(filename, "filename")
    doc_dir = get_doc_dir(pkg, operator, usm_version)
    body = await request.json()
    content = body.get("content", "")

    if filename.endswith((".yaml", ".yml")):
        try:
            is_valid, message = validate_yaml_content(content)
            if not is_valid:
                return JSONResponse(status_code=400, content={"error": "YAML validation failed", "detail": message})
        except Exception as e:
            return JSONResponse(status_code=400, content={"error": "YAML parse error", "detail": str(e)})

    file_path = (doc_dir / filename).resolve()
    try:
        file_path.relative_to(doc_dir)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Disallowed file path: {filename}")
    try:
        file_path.write_text(content, encoding="utf-8")
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": "File save error", "detail": str(e)})
    return JSONResponse(content={"status": "success", "message": f"{filename} saved."})


@app.get("/api/preview/{pkg}/{operator}", response_class=HTMLResponse)
async def preview(pkg: str, operator: str, usm_version: str = Query("USMv1")):
    """Render doc.md with embeddings as HTML preview."""
    doc_dir = get_doc_dir(pkg, operator, usm_version)
    try:
        html_body = render_document(str(doc_dir))
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": "Render error", "detail": str(e)})

    preview_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link rel="stylesheet" href="/static/css/print.css">
    <script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
    <script>mermaid.initialize({{startOnLoad: true, theme: 'default'}});</script>
</head>
<body class="preview-body">
    <div class="document-content">{html_body}</div>
</body>
</html>"""
    return HTMLResponse(content=preview_html)


@app.get("/api/pdf/{pkg}/{operator}")
async def download_pdf(
    pkg: str, operator: str,
    usm_version: str = Query("USMv1"),
    diff_base_pkg: str = None, diff_base_op: str = None,
    diff_file: str = "doc.yaml",
    usm_a: str = None, usm_b: str = None
):
    """Generate and download PDF.
    When diff_base_pkg/diff_base_op are provided, renders the full diff-highlighted
    HTML (same as Compare tab) instead of the standard document.
    usm_a/usm_b allow cross-version comparison.
    """
    doc_dir = get_doc_dir(pkg, operator, usm_version)

    diff_css_inline = ""
    is_diff_mode = False
    html_body = ""

    if diff_base_pkg and diff_base_op:
        # --- DIFF MODE: render full diff-highlighted HTML ---
        try:
            path_safeguard(diff_file, "diff_file")
            base_usm = usm_a or usm_version
            target_usm = usm_b or usm_version
            base_dir = get_doc_dir(diff_base_pkg, diff_base_op, base_usm)
            target_dir = get_doc_dir(pkg, operator, target_usm)
            base_file_path = base_dir / diff_file
            curr_file_path = target_dir / diff_file

            if not base_file_path.exists():
                raise HTTPException(status_code=404, detail=f"{diff_base_pkg}/{diff_base_op}/{diff_file} not found.")
            if not curr_file_path.exists():
                raise HTTPException(status_code=404, detail=f"{pkg}/{operator}/{diff_file} not found.")

            base_text = base_file_path.read_text(encoding="utf-8")
            curr_text = curr_file_path.read_text(encoding="utf-8")
            label_a = f"[{base_usm}] {diff_base_pkg}/{diff_base_op}"
            label_b = f"[{target_usm}] {pkg}/{operator}"

            # Build diff header
            header_html = (
                f'<div style="margin-bottom:1.5rem;padding:0.75rem 1rem;background:#f8fafc;border:1px solid #e5e7eb;border-radius:8px;">'
                f'<h2 style="margin:0 0 0.25rem;font-size:1.1rem;color:#1e3a5f;">Version Comparison Report</h2>'
                f'<p style="margin:0;font-size:0.85rem;color:#6b7280;">{label_a} vs {label_b} &mdash; {diff_file}</p>'
                f'</div>'
            )

            # Render the same diff HTML as the Compare tab
            if diff_file.endswith((".yaml", ".yml")):
                yaml_diff = compute_yaml_diff(base_text, curr_text, label_a, label_b)
                diff_html = render_semantic_diff_html(yaml_diff)
            else:
                text_diff = compute_text_diff(base_text, curr_text, label_a, label_b)
                diff_html = render_diff_html(text_diff)

            html_body = header_html + diff_html
            is_diff_mode = True

            diff_css_path = BASE_DIR / "static" / "css" / "diff.css"
            if diff_css_path.exists():
                diff_css_inline = diff_css_path.read_text(encoding="utf-8")

        except HTTPException:
            raise
        except Exception as e:
            return JSONResponse(status_code=500, content={"error": "Diff render error", "detail": str(e)})
    else:
        # --- STANDARD MODE: render document normally ---
        try:
            html_body = render_document(str(doc_dir))
        except Exception as e:
            return JSONResponse(status_code=500, content={"error": "Render error", "detail": str(e)})

    # Assemble full HTML from PDF template
    pdf_template_path = BASE_DIR / "templates" / "pdf_template.html"
    pdf_template = pdf_template_path.read_text(encoding="utf-8")
    full_html = pdf_template.replace("<!-- __CONTENT_PLACEHOLDER__ -->", html_body)

    print_css_path = BASE_DIR / "static" / "css" / "print.css"
    if print_css_path.exists():
        css_content = print_css_path.read_text(encoding="utf-8")
        if diff_css_inline:
            css_content += "\n" + diff_css_inline
        full_html = full_html.replace("/* __PRINT_CSS_PLACEHOLDER__ */", css_content)

    try:
        pdf_bytes = await generate_pdf(full_html, PDF_MARGINS)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": "PDF generation error", "detail": str(e)})

    filename = f"diff_{diff_base_pkg}_vs_{pkg}_{diff_file.split('.')[0]}.pdf" if is_diff_mode else f"NBI_{pkg}_{operator}.pdf"
    return StreamingResponse(
        iter([pdf_bytes]),
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@app.get("/api/diff/{pkg_a}/{op_a}/{pkg_b}/{op_b}")
async def get_diff(
    pkg_a: str, op_a: str, pkg_b: str, op_b: str,
    file: str = "doc.yaml",
    usm_a: str = Query("USMv1"),
    usm_b: str = Query("USMv1")
):
    """Compare files between two packages. Supports cross-version compare via usm_a/usm_b."""
    path_safeguard(file, "file")
    dir_a = get_doc_dir(pkg_a, op_a, usm_a)
    dir_b = get_doc_dir(pkg_b, op_b, usm_b)

    file_a, file_b = dir_a / file, dir_b / file
    if not file_a.exists():
        raise HTTPException(status_code=404, detail=f"{pkg_a}/{op_a}/{file} not found.")
    if not file_b.exists():
        raise HTTPException(status_code=404, detail=f"{pkg_b}/{op_b}/{file} not found.")

    text_a = file_a.read_text(encoding="utf-8")
    text_b = file_b.read_text(encoding="utf-8")
    label_a, label_b = f"[{usm_a}] {pkg_a}/{op_a}", f"[{usm_b}] {pkg_b}/{op_b}"

    if file.endswith((".yaml", ".yml")):
        result = compute_yaml_diff(text_a, text_b, label_a, label_b)
    else:
        result = compute_text_diff(text_a, text_b, label_a, label_b)
    return JSONResponse(content=result)


@app.get("/api/diff/preview/{pkg_a}/{op_a}/{pkg_b}/{op_b}", response_class=HTMLResponse)
async def diff_preview(
    pkg_a: str, op_a: str, pkg_b: str, op_b: str,
    file: str = "doc.yaml",
    usm_a: str = Query("USMv1"),
    usm_b: str = Query("USMv1")
):
    """Render Side-by-Side diff as HTML. Supports cross-version compare via usm_a/usm_b."""
    path_safeguard(file, "file")
    dir_a = get_doc_dir(pkg_a, op_a, usm_a)
    dir_b = get_doc_dir(pkg_b, op_b, usm_b)

    file_a, file_b = dir_a / file, dir_b / file
    if not file_a.exists():
        raise HTTPException(status_code=404, detail=f"{pkg_a}/{op_a}/{file} not found.")
    if not file_b.exists():
        raise HTTPException(status_code=404, detail=f"{pkg_b}/{op_b}/{file} not found.")

    text_a = file_a.read_text(encoding="utf-8")
    text_b = file_b.read_text(encoding="utf-8")
    label_a, label_b = f"[{usm_a}] {pkg_a}/{op_a}", f"[{usm_b}] {pkg_b}/{op_b}"

    if file.endswith((".yaml", ".yml")):
        yaml_diff = compute_yaml_diff(text_a, text_b, label_a, label_b)
        diff_html = render_semantic_diff_html(yaml_diff)
    else:
        text_diff = compute_text_diff(text_a, text_b, label_a, label_b)
        diff_html = render_diff_html(text_diff)

    diff_css = ""
    diff_css_path = BASE_DIR / "static" / "css" / "diff.css"
    if diff_css_path.exists():
        diff_css = diff_css_path.read_text(encoding="utf-8")

    full_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {{ font-family: 'Segoe UI', sans-serif; margin: 0; padding: 1rem; background: white; color: #1f2937; }}
        {diff_css}
    </style>
</head>
<body>
    <h2 style="font-size:1rem;color:#1e3a5f;margin-bottom:1rem;">{label_a} vs {label_b} &mdash; {file}</h2>
    {diff_html}
</body>
</html>"""
    return HTMLResponse(content=full_html)


@app.post("/api/chat")
async def chat_endpoint(request: Request):
    """Handle chat messages with SSE streaming."""
    body = await request.json()
    model = body.get("model", CONFIG["rag_parameters"]["llm_model"])
    message = body.get("message", "")
    chat_history = body.get("chat_history", [])
    mode = body.get("mode", "default")

    # ── Upload mode: use isolated pipeline (NO Route 0/1 spec lookup) ──
    if mode == "upload":
        return StreamingResponse(
            handle_upload_chat_stream(message, model, CONFIG,
                                     chat_history=chat_history),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            }
        )

    # ── Default mode: NBI spec RAG pipeline ──
    context = {
        "context_version": body.get("context_version"),
        "context_operator": body.get("context_operator")
    }

    return StreamingResponse(
        handle_chat_stream(message, model, CONFIG, context,
                           chat_history=chat_history),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        }
    )


# ─── Upload Document Endpoints ──────────────────────────────────

@app.post("/api/upload-docs")
async def upload_docs(files: list[UploadFile] = File(...)):
    """Upload PDF/DOCX documents for isolated RAG.
    Double-checks existing + new file count against max_files limit.
    """
    import traceback as _tb

    upload_cfg = CONFIG["upload_rag_settings"]
    max_files = upload_cfg.get("max_files", 5)

    # ── Double-check: backend validation of total file count ──
    existing_count = get_uploaded_file_count(CONFIG)
    if existing_count + len(files) > max_files:
        return JSONResponse(
            status_code=400,
            content={
                "error": f"Maximum {max_files} files allowed. "
                         f"Currently {existing_count} file(s) uploaded, "
                         f"attempted to add {len(files)} more."
            }
        )

    # Validate file types
    valid_extensions = {".pdf", ".docx", ".doc"}
    filenames = []
    for f in files:
        ext = Path(f.filename).suffix.lower()
        if ext not in valid_extensions:
            return JSONResponse(
                status_code=400,
                content={"error": f"Unsupported file type: {f.filename}. Only PDF and DOCX are allowed."}
            )
        filenames.append(f.filename)

    # Save temp files and create task
    task_id = create_task(filenames)
    temp_dir = tempfile.mkdtemp(prefix="nbi_upload_")

    # ── Debug: verify temp directory write permissions ──
    dir_writable = os.access(temp_dir, os.W_OK)
    logger.info(f"[Upload API] temp_dir={temp_dir}, writable={dir_writable}, "
                f"files={len(files)}")

    for f in files:
        try:
            temp_path = os.path.join(temp_dir, f.filename)
            content = await f.read()

            # ── Debug: log file metadata ──
            logger.info(
                f"[Upload API] Received '{f.filename}' | "
                f"content_type={f.content_type} | "
                f"size={len(content)} bytes"
            )

            with open(temp_path, "wb") as fp:
                fp.write(content)

            # Spawn background processing task
            asyncio.create_task(
                process_upload(temp_path, f.filename, CONFIG, task_id)
            )
        except Exception as e:
            logger.error(
                f"[Upload API] FAILED saving '{f.filename}': {e}\n"
                f"{_tb.format_exc()}"
            )
            return JSONResponse(
                status_code=500,
                content={"error": f"Failed to save file '{f.filename}': {str(e)}"}
            )

    return JSONResponse(content={
        "task_id": task_id,
        "files": filenames,
        "message": f"Processing {len(filenames)} file(s)"
    })


@app.get("/api/upload-progress/{task_id}")
async def upload_progress(task_id: str):
    """SSE endpoint streaming per-file progress until all files are done."""
    async def progress_stream():
        while True:
            progress = get_progress(task_id)
            if progress is None:
                yield f"data: {json.dumps({'error': 'Task not found'})}\n\n"
                return

            file_data = progress["files"]
            yield f"data: {json.dumps(file_data)}\n\n"

            # Check if all files are done or errored
            all_done = all(
                f["status"] in ("done", "error")
                for f in file_data.values()
            )
            if all_done:
                return

            await asyncio.sleep(0.5)

    return StreamingResponse(
        progress_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        }
    )


@app.delete("/api/upload-docs")
async def delete_upload_docs():
    """Reset the upload collection and clear all uploaded documents."""
    try:
        reset_collection(CONFIG)
        return JSONResponse(content={"status": "ok", "message": "Upload collection reset."})
    except Exception as e:
        logger.error(f"Upload reset error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/upload-docs")
async def list_upload_docs():
    """List uploaded files in the collection."""
    files = get_uploaded_files(CONFIG)
    return JSONResponse(content={"files": files, "count": len(files)})


@app.delete("/api/delete-doc")
async def delete_single_doc(filename: str = Query(..., description="Filename to delete")):
    """Delete a single uploaded document's chunks from ChromaDB."""
    import traceback as _tb
    try:
        delete_doc(CONFIG, filename)
        return JSONResponse(content={"status": "ok", "deleted": filename})
    except Exception as e:
        logger.error(f"Delete doc error: {e}\n{_tb.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/api/reindex")
async def reindex_endpoint():
    """Trigger incremental document re-indexing."""
    try:
        result = await run_incremental_index(CONFIG, force_reindex=True)
        return JSONResponse(content={"status": "success", **result})
    except Exception as e:
        logger.error(f"Reindex error: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "detail": str(e)})


@app.get("/api/llm-status")
async def llm_status(model: str = ""):
    """Check if a model has a configured API URL."""
    llm_apis = CONFIG.get("llm_apis", {})
    api_url = ""
    for provider, cfg in llm_apis.items():
        models = cfg.get("models", [])
        if model in models:
            api_url = cfg.get("api_url", "")
            break
    return JSONResponse(content={"model": model, "api_url": api_url})


if __name__ == "__main__":
    import uvicorn
    print(f"[*] USM AI Platform starting on http://{SERVER_HOST}:{SERVER_PORT}")
    uvicorn.run(app, host=SERVER_HOST, port=SERVER_PORT)
