"""
Document rendering service.
Parses doc.md, applies auto-numbering, and processes {{TYPE:FILENAME:KEY}} tag embeddings.

Tag format (domain-centric doc.yaml):
  {{REST:doc.yaml:CM}}              -> CM.rest from doc.yaml
  {{SFTP:doc.yaml:FM}}              -> FM.sftp from doc.yaml
  {{SEQUENCE:doc.yaml:main_sequence}} -> block scalar Mermaid extraction
  {{REST}}                           -> fallback: full doc.yaml REST render
"""

import os
import re
import yaml
import markdown
from bs4 import BeautifulSoup

TAG_PATTERN = re.compile(
    r'\{\{(REST|SFTP|SEQUENCE):?([^:}]*)?:?([^}]*)?\}\}'
)

DEFAULT_FILE = "doc.yaml"

DOMAIN_KEYS = {"CM", "FM", "PM"}
EXCEPTION_KEYS = {"main_sequence"}


def load_yaml_file(filepath: str) -> dict:
    """Load and parse a YAML file."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        if data is None:
            raise ValueError(f"YAML file is empty: {os.path.basename(filepath)}")
        return data
    except yaml.YAMLError as e:
        raise ValueError(f"YAML parse error ({os.path.basename(filepath)}): {e}")
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {os.path.basename(filepath)}")


def extract_domain_interface(yaml_data: dict, domain_key: str, interface_type: str, filename: str) -> dict:
    """
    Two-level extraction: domain key (CM/FM/PM) -> interface type (rest/sftp).
    Uses .get() with defaults to prevent KeyError crashes.
    """
    domain_data = yaml_data.get(domain_key)
    if domain_data is None:
        available = ", ".join(k for k in yaml_data.keys() if k in DOMAIN_KEYS)
        raise KeyError(
            f"Domain '{domain_key}' not found in {filename}. "
            f"Available domains: [{available}]"
        )

    if not isinstance(domain_data, dict):
        raise KeyError(f"Domain '{domain_key}' in {filename} is not a valid structure.")

    iface_key = interface_type.lower()
    interface_data = domain_data.get(iface_key)
    if interface_data is None:
        available_ifaces = ", ".join(domain_data.keys())
        raise KeyError(
            f"Interface '{iface_key}' not found under '{domain_key}' in {filename}. "
            f"Available: [{available_ifaces}]"
        )

    if isinstance(interface_data, dict):
        return interface_data
    return {iface_key: interface_data}


def extract_yaml_by_key(yaml_data: dict, key: str, filename: str) -> dict:
    """Extract a specific key from YAML data. Returns the value directly if dict."""
    if key not in yaml_data:
        available_keys = ", ".join(yaml_data.keys())
        raise KeyError(
            f"Key '{key}' not found in {filename}. "
            f"Available keys: [{available_keys}]"
        )
    value = yaml_data[key]
    if isinstance(value, dict):
        return value
    return {key: value}


def validate_yaml_content(content: str) -> tuple[bool, str]:
    """Validate YAML content structure. Supports domain-centric (CM/FM/PM) and legacy formats."""
    try:
        data = yaml.safe_load(content)
        if data is None:
            return False, "YAML file is empty."
        if not isinstance(data, dict):
            return False, "Top-level YAML structure must be a dictionary."

        has_domain = any(k in data for k in DOMAIN_KEYS)
        has_exception = any(k in data for k in EXCEPTION_KEYS)

        if has_domain or has_exception:
            return True, "Valid domain-centric YAML."

        if "info" in data:
            info = data["info"]
            if not isinstance(info, dict):
                return False, "'info' must be a dictionary."
            if "title" not in info:
                return False, "'info.title' is missing."
            if "version" not in info:
                return False, "'info.version' is missing."
            return True, "Valid legacy YAML."

        return False, "YAML must contain domain keys (CM/FM/PM), 'main_sequence', or 'info'."
    except yaml.YAMLError as e:
        return False, f"YAML parse error: {e}"


def render_rest_table(yaml_data: dict) -> str:
    """Convert REST YAML data to HTML tables."""
    html = []

    info = yaml_data.get("info", {})
    if info:
        html.append(f'<div class="spec-info-box">')
        html.append(f'<h4 class="spec-info-title">{info.get("title", "REST API")}</h4>')
        html.append(f'<p class="spec-info-meta">Version: {info.get("version", "-")} | {info.get("description", "")}</p>')
        html.append(f'</div>')

    for ep in yaml_data.get("endpoints", []):
        method = ep.get("method", "GET")
        html.append(f'<div class="endpoint-card">')
        html.append(f'<div class="endpoint-header">')
        html.append(f'<span class="method-badge method-{method.lower()}">{method}</span>')
        html.append(f'<code class="endpoint-path">{ep.get("path", "")}</code>')
        html.append(f'</div>')
        html.append(f'<p class="endpoint-summary">{ep.get("summary", "")}</p>')
        html.append(f'<p class="endpoint-desc">{ep.get("description", "")}</p>')

        params = ep.get("parameters", [])
        if params:
            html.append('<h5 class="section-label">Parameters</h5>')
            html.append('<table class="spec-table"><thead><tr>')
            html.append('<th>Name</th><th>In</th><th>Type</th><th>Required</th><th>Description</th>')
            html.append('</tr></thead><tbody>')
            for p in params:
                req = "&#10004;" if p.get("required") else ""
                html.append(
                    f'<tr><td><code>{p.get("name","")}</code></td>'
                    f'<td>{p.get("in","")}</td>'
                    f'<td>{p.get("type","")}</td>'
                    f'<td class="text-center">{req}</td>'
                    f'<td>{p.get("description","")}</td></tr>'
                )
            html.append('</tbody></table>')

        req_body = ep.get("request_body")
        if req_body:
            html.append('<h5 class="section-label">Request Body</h5>')
            html.append(f'<p class="content-type">Content-Type: <code>{req_body.get("content_type","")}</code></p>')
            schema = req_body.get("schema", {})
            if schema:
                html.append('<table class="spec-table"><thead><tr><th>Field</th><th>Type</th></tr></thead><tbody>')
                for field, ftype in schema.items():
                    html.append(f'<tr><td><code>{field}</code></td><td>{ftype}</td></tr>')
                html.append('</tbody></table>')

        responses = ep.get("responses", {})
        if responses:
            html.append('<h5 class="section-label">Responses</h5>')
            html.append('<table class="spec-table"><thead><tr><th>Status</th><th>Description</th><th>Schema</th></tr></thead><tbody>')
            for code, resp in responses.items():
                schema_name = resp.get("schema", "-") if isinstance(resp, dict) else "-"
                desc = resp.get("description", "") if isinstance(resp, dict) else str(resp)
                html.append(f'<tr><td><code>{code}</code></td><td>{desc}</td><td>{schema_name}</td></tr>')
            html.append('</tbody></table>')

        html.append('</div>')

    return "\n".join(html)


def render_sftp_table(yaml_data: dict) -> str:
    """Convert SFTP YAML data to HTML tables."""
    html = []

    info = yaml_data.get("info", {})
    if info:
        html.append(f'<div class="spec-info-box">')
        html.append(f'<h4 class="spec-info-title">{info.get("title", "SFTP Interface")}</h4>')
        html.append(f'<p class="spec-info-meta">Version: {info.get("version", "-")} | {info.get("description", "")}</p>')
        html.append(f'</div>')

    conn = yaml_data.get("connection", {})
    if conn:
        html.append('<div class="connection-info">')
        html.append('<h5 class="section-label">Connection Information</h5>')
        html.append('<table class="spec-table"><thead><tr><th>Property</th><th>Value</th></tr></thead><tbody>')
        html.append(f'<tr><td>Protocol</td><td>{conn.get("protocol","")}</td></tr>')
        html.append(f'<tr><td>Port</td><td>{conn.get("port","")}</td></tr>')
        html.append(f'<tr><td>Authentication</td><td>{conn.get("authentication","")}</td></tr>')
        html.append(f'<tr><td>Timeout</td><td>{conn.get("timeout_seconds","")} sec</td></tr>')
        html.append(f'<tr><td>Max Retries</td><td>{conn.get("max_retries","")}</td></tr>')
        encryption = conn.get("encryption")
        if encryption:
            html.append(f'<tr><td>Encryption</td><td>{encryption}</td></tr>')
        html.append('</tbody></table></div>')

    transfers = yaml_data.get("transfers", [])
    if transfers:
        html.append('<h5 class="section-label">File Transfers</h5>')
        html.append('<table class="spec-table"><thead><tr>')
        html.append('<th>Name</th><th>Direction</th><th>Source</th><th>Destination</th><th>Pattern</th><th>Schedule</th><th>Format</th>')
        html.append('</tr></thead><tbody>')
        for t in transfers:
            html.append(
                f'<tr>'
                f'<td><strong>{t.get("name","")}</strong><br/><small>{t.get("description","")}</small></td>'
                f'<td>{t.get("direction","")}</td>'
                f'<td><code>{t.get("source_path","")}</code></td>'
                f'<td><code>{t.get("destination_path","")}</code></td>'
                f'<td><code>{t.get("file_pattern","")}</code></td>'
                f'<td>{t.get("schedule","")}</td>'
                f'<td>{t.get("format","")}</td>'
                f'</tr>'
            )
        html.append('</tbody></table>')

    return "\n".join(html)


def render_sequence_diagram(mmd_content: str) -> str:
    """Wrap Mermaid content in a <pre class="mermaid"> tag."""
    return f'<pre class="mermaid">\n{mmd_content.strip()}\n</pre>'


def apply_auto_numbering(html_content: str) -> str:
    """Apply hierarchical numbering to h1/h2/h3 tags."""
    soup = BeautifulSoup(html_content, "html.parser")
    counters = [0, 0, 0]

    for tag in soup.find_all(["h1", "h2", "h3"]):
        level = int(tag.name[1])
        idx = level - 1
        counters[idx] += 1
        for i in range(idx + 1, 3):
            counters[i] = 0

        number_parts = [str(counters[i]) for i in range(level)]
        number = ".".join(number_parts) + "."

        number_span = soup.new_tag("span", attrs={"class": "heading-number"})
        number_span.string = number + " "
        tag.insert(0, number_span)
        tag["id"] = f"section-{number.rstrip('.')}"

    return str(soup)


def _replace_tag(match: re.Match, doc_dir: str) -> str:
    """Process a tag match and return embedded HTML."""
    tag_type = match.group(1).upper()
    filename = match.group(2) or None
    key = match.group(3) or None

    if filename and not filename.strip():
        filename = None
    if key and not key.strip():
        key = None

    if filename is None:
        filename = DEFAULT_FILE

    filepath = os.path.join(doc_dir, filename)
    tag_label = f"{tag_type}:{filename}" + (f":{key}" if key else "")

    try:
        if not os.path.exists(filepath):
            return f'\n\n<div class="error-box">Error: File not found: {filename}</div>\n\n'

        if tag_type == "SEQUENCE":
            if filename.endswith((".yaml", ".yml")):
                yaml_data = load_yaml_file(filepath)
                if key:
                    if key not in yaml_data:
                        return f'\n\n<div class="error-box">Error: Key [{key}] not found in [{filename}]</div>\n\n'
                    mmd_content = yaml_data[key]
                    if not isinstance(mmd_content, str):
                        return f'\n\n<div class="error-box">Error: Key [{key}] in [{filename}] is not a text value.</div>\n\n'
                else:
                    mmd_content = None
                    for v in yaml_data.values():
                        if isinstance(v, str) and "sequenceDiagram" in v:
                            mmd_content = v
                            break
                    if mmd_content is None:
                        return f'\n\n<div class="error-box">Error: No sequence diagram found in [{filename}]. Please specify a KEY.</div>\n\n'
            else:
                with open(filepath, "r", encoding="utf-8") as f:
                    mmd_content = f.read()

            result_html = render_sequence_diagram(mmd_content)
            return f"\n\n<!-- SEQ_EMBED_START ({tag_label}) -->\n{result_html}\n<!-- SEQ_EMBED_END -->\n\n"

        else:
            yaml_data = load_yaml_file(filepath)

            if key and key in DOMAIN_KEYS:
                try:
                    yaml_data = extract_domain_interface(yaml_data, key, tag_type, filename)
                except KeyError as e:
                    return f'\n\n<div class="error-box">Error: {e}</div>\n\n'
            elif key:
                try:
                    yaml_data = extract_yaml_by_key(yaml_data, key, filename)
                except KeyError:
                    return f'\n\n<div class="error-box">Error: Key [{key}] not found in [{filename}]</div>\n\n'

            if tag_type == "REST":
                result_html = render_rest_table(yaml_data)
            elif tag_type == "SFTP":
                result_html = render_sftp_table(yaml_data)
            else:
                return f'\n\n<div class="error-box">Error: Unknown tag type: {tag_type}</div>\n\n'

            return f"\n\n<!-- {tag_type}_EMBED_START ({tag_label}) -->\n{result_html}\n<!-- {tag_type}_EMBED_END -->\n\n"

    except Exception as e:
        return f'\n\n<div class="error-box">{tag_type} processing error ({filename}): {e}</div>\n\n'


def render_document(doc_dir: str) -> str:
    """Read doc.md, process all embedding tags, and return complete HTML."""
    try:
        doc_path = os.path.join(doc_dir, "doc.md")

        if not os.path.exists(doc_path):
            return "<div class='error-box'>doc.md file not found.</div>"

        with open(doc_path, "r", encoding="utf-8") as f:
            doc_content = f.read()

        doc_content = TAG_PATTERN.sub(
            lambda m: _replace_tag(m, doc_dir),
            doc_content
        )

        try:
            html_body = markdown.markdown(
                doc_content,
                extensions=["tables", "fenced_code", "md_in_html"],
                output_format="html5"
            )
        except Exception as e:
            return f'<div class="error-box">Markdown conversion error: {e}</div>'

        html_body = apply_auto_numbering(html_body)
        return html_body

    except Exception as e:
        return f'<div class="error-box">Document rendering error: {e}<br/>Path: {doc_dir}</div>'
