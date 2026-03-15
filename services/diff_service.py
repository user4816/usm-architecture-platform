"""
Diff service for visual version comparison.
Uses deepdiff for semantic YAML diff, difflib for text diff, with server-side caching.
"""

import difflib
import hashlib
import yaml
import time
from deepdiff import DeepDiff

_diff_cache: dict[str, tuple[float, dict]] = {}
_CACHE_TTL = 300
_CACHE_MAX = 50


def _cache_key(text_a: str, text_b: str) -> str:
    combined = (text_a + "||" + text_b).encode("utf-8")
    return hashlib.md5(combined).hexdigest()


def _get_cached(key: str) -> dict | None:
    if key in _diff_cache:
        ts, result = _diff_cache[key]
        if time.time() - ts < _CACHE_TTL:
            return result
        del _diff_cache[key]
    return None


def _set_cache(key: str, result: dict) -> None:
    if len(_diff_cache) >= _CACHE_MAX:
        oldest = min(_diff_cache, key=lambda k: _diff_cache[k][0])
        del _diff_cache[oldest]
    _diff_cache[key] = (time.time(), result)


def compute_text_diff(text_a: str, text_b: str, label_a: str = "A", label_b: str = "B") -> dict:
    """Compute line-by-line text diff."""
    cache_key = _cache_key(text_a, text_b)
    cached = _get_cached(cache_key)
    if cached:
        return cached

    lines_a = text_a.splitlines(keepends=True)
    lines_b = text_b.splitlines(keepends=True)
    matcher = difflib.SequenceMatcher(None, lines_a, lines_b)
    diff_lines = []
    stats = {"added": 0, "removed": 0, "changed": 0, "equal": 0}

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            for k in range(i2 - i1):
                diff_lines.append({
                    "type": "equal",
                    "line_a": i1 + k + 1,
                    "line_b": j1 + k + 1,
                    "content": lines_a[i1 + k].rstrip("\n\r")
                })
                stats["equal"] += 1
        elif tag == "replace":
            max_len = max(i2 - i1, j2 - j1)
            for k in range(max_len):
                la = lines_a[i1 + k].rstrip("\n\r") if i1 + k < i2 else ""
                lb = lines_b[j1 + k].rstrip("\n\r") if j1 + k < j2 else ""
                diff_lines.append({
                    "type": "changed",
                    "line_a": i1 + k + 1 if i1 + k < i2 else None,
                    "line_b": j1 + k + 1 if j1 + k < j2 else None,
                    "content_a": la,
                    "content_b": lb
                })
                stats["changed"] += 1
        elif tag == "delete":
            for k in range(i2 - i1):
                diff_lines.append({
                    "type": "removed",
                    "line_a": i1 + k + 1,
                    "line_b": None,
                    "content": lines_a[i1 + k].rstrip("\n\r")
                })
                stats["removed"] += 1
        elif tag == "insert":
            for k in range(j2 - j1):
                diff_lines.append({
                    "type": "added",
                    "line_a": None,
                    "line_b": j1 + k + 1,
                    "content": lines_b[j1 + k].rstrip("\n\r")
                })
                stats["added"] += 1

    result = {"lines": diff_lines, "stats": stats, "label_a": label_a, "label_b": label_b}
    _set_cache(cache_key, result)
    return result


def compute_yaml_diff(yaml_text_a: str, yaml_text_b: str, label_a: str = "A", label_b: str = "B") -> dict:
    """Semantic YAML comparison using deepdiff for nested structure detection."""
    cache_key = _cache_key("yaml:" + yaml_text_a, yaml_text_b)
    cached = _get_cached(cache_key)
    if cached:
        return cached

    data_a = yaml.safe_load(yaml_text_a) or {}
    data_b = yaml.safe_load(yaml_text_b) or {}
    deep = DeepDiff(data_a, data_b, ignore_order=True, verbose_level=2)

    summary = []
    stats = {"added": 0, "removed": 0, "changed": 0}

    for path, value in deep.get("dictionary_item_added", {}).items():
        clean_path = _format_path(path)
        summary.append({
            "type": "added",
            "path": clean_path,
            "detail": f"Added: {_truncate(str(value), 100)}"
        })
        stats["added"] += 1

    for path, value in deep.get("iterable_item_added", {}).items():
        clean_path = _format_path(path)
        summary.append({
            "type": "added",
            "path": clean_path,
            "detail": f"Item added: {_truncate(str(value), 100)}"
        })
        stats["added"] += 1

    for path, value in deep.get("dictionary_item_removed", {}).items():
        clean_path = _format_path(path)
        summary.append({
            "type": "removed",
            "path": clean_path,
            "detail": f"Removed: {_truncate(str(value), 100)}"
        })
        stats["removed"] += 1

    for path, value in deep.get("iterable_item_removed", {}).items():
        clean_path = _format_path(path)
        summary.append({
            "type": "removed",
            "path": clean_path,
            "detail": f"Item removed: {_truncate(str(value), 100)}"
        })
        stats["removed"] += 1

    for path, change in deep.get("values_changed", {}).items():
        clean_path = _format_path(path)
        old_val = _truncate(str(change.get("old_value", "")), 60)
        new_val = _truncate(str(change.get("new_value", "")), 60)
        summary.append({
            "type": "changed",
            "path": clean_path,
            "detail": f'"{old_val}" -> "{new_val}"'
        })
        stats["changed"] += 1

    for path, change in deep.get("type_changes", {}).items():
        clean_path = _format_path(path)
        old_t = getattr(change.get("old_type", ""), "__name__", "?")
        new_t = getattr(change.get("new_type", ""), "__name__", "?")
        summary.append({
            "type": "changed",
            "path": clean_path,
            "detail": f"Type changed: {old_t} -> {new_t}"
        })
        stats["changed"] += 1

    text_diff = compute_text_diff(yaml_text_a, yaml_text_b, label_a, label_b)

    result = {
        "summary": summary,
        "stats": stats,
        "text_diff": text_diff,
        "label_a": label_a,
        "label_b": label_b
    }
    _set_cache(cache_key, result)
    return result


# Domain key mapping for human-readable path labels
_DOMAIN_LABELS = {
    "CM": "CM", "FM": "FM", "PM": "PM",
    "rest": "REST", "sftp": "SFTP",
    "endpoints": "Endpoints", "transfers": "Transfers",
    "connection": "Connection", "info": "Info",
    "parameters": "Parameters", "responses": "Responses",
}


def _format_path(path: str) -> str:
    """Convert DeepDiff paths like root['CM']['rest'] to readable CM > REST format."""
    path = path.replace("root", "")
    path = path.replace("['", ".").replace("']", "")
    path = path.replace("[", "[").replace("]", "]")
    if path.startswith("."):
        path = path[1:]

    parts = path.split(".")
    formatted = []
    for part in parts:
        bracket_idx = part.find("[")
        if bracket_idx > 0:
            key = part[:bracket_idx]
            idx = part[bracket_idx:]
            formatted.append(_DOMAIN_LABELS.get(key, key) + idx)
        else:
            formatted.append(_DOMAIN_LABELS.get(part, part))

    return " > ".join(formatted)


def _truncate(text: str, max_len: int) -> str:
    if len(text) > max_len:
        return text[:max_len] + "..."
    return text


def render_diff_html(diff_result: dict) -> str:
    """Render text diff as Side-by-Side HTML table."""
    lines = diff_result.get("lines", [])
    label_a = diff_result.get("label_a", "A")
    label_b = diff_result.get("label_b", "B")
    stats = diff_result.get("stats", {})

    html = []
    html.append('<div class="diff-stats">')
    html.append(f'<span class="diff-stat-added">+{stats.get("added", 0)} Added</span>')
    html.append(f'<span class="diff-stat-removed">-{stats.get("removed", 0)} Removed</span>')
    html.append(f'<span class="diff-stat-changed">~{stats.get("changed", 0)} Changed</span>')
    html.append('</div>')

    html.append('<div class="diff-container">')
    html.append('<table class="diff-table">')
    html.append(f'<thead><tr><th class="diff-line-num">#</th><th class="diff-content-header">{label_a}</th>')
    html.append(f'<th class="diff-line-num">#</th><th class="diff-content-header">{label_b}</th></tr></thead>')
    html.append('<tbody>')

    for line in lines:
        lt = line["type"]
        css = f"diff-{lt}"

        if lt == "equal":
            ln = line.get("line_a", "")
            c = _escape_html(line.get("content", ""))
            html.append(f'<tr class="{css}"><td class="diff-line-num">{ln}</td><td class="diff-content">{c}</td>'
                        f'<td class="diff-line-num">{line.get("line_b","")}</td><td class="diff-content">{c}</td></tr>')
        elif lt == "changed":
            la = line.get("line_a", "") or ""
            lb = line.get("line_b", "") or ""
            ca = _escape_html(line.get("content_a", ""))
            cb = _escape_html(line.get("content_b", ""))
            html.append(f'<tr class="{css}"><td class="diff-line-num">{la}</td><td class="diff-content diff-old">{ca}</td>'
                        f'<td class="diff-line-num">{lb}</td><td class="diff-content diff-new">{cb}</td></tr>')
        elif lt == "removed":
            ln = line.get("line_a", "")
            c = _escape_html(line.get("content", ""))
            html.append(f'<tr class="{css}"><td class="diff-line-num">{ln}</td><td class="diff-content diff-old">{c}</td>'
                        f'<td class="diff-line-num"></td><td class="diff-content diff-empty"></td></tr>')
        elif lt == "added":
            ln = line.get("line_b", "")
            c = _escape_html(line.get("content", ""))
            html.append(f'<tr class="{css}"><td class="diff-line-num"></td><td class="diff-content diff-empty"></td>'
                        f'<td class="diff-line-num">{ln}</td><td class="diff-content diff-new">{c}</td></tr>')

    html.append('</tbody></table></div>')
    return "\n".join(html)


def render_semantic_diff_html(yaml_diff: dict) -> str:
    """Render semantic YAML diff as HTML with summary table + line diff."""
    summary = yaml_diff.get("summary", [])
    stats = yaml_diff.get("stats", {})

    html = []
    html.append('<div class="semantic-diff">')
    html.append('<h3 class="semantic-diff-title">Semantic YAML Comparison</h3>')
    html.append('<div class="diff-stats">')
    html.append(f'<span class="diff-stat-added">+{stats.get("added", 0)} Added</span>')
    html.append(f'<span class="diff-stat-removed">-{stats.get("removed", 0)} Removed</span>')
    html.append(f'<span class="diff-stat-changed">~{stats.get("changed", 0)} Changed</span>')
    html.append('</div>')

    if not summary:
        html.append('<p class="diff-no-changes">No changes detected.</p>')
    else:
        html.append('<table class="semantic-table"><thead><tr>')
        html.append('<th>Type</th><th>Path</th><th>Detail</th>')
        html.append('</tr></thead><tbody>')
        for item in summary:
            icon = {"added": "&#10010;", "removed": "&#10006;", "changed": "&#8634;"}.get(item["type"], "")
            css = f'diff-{item["type"]}'
            html.append(
                f'<tr class="{css}">'
                f'<td>{icon}</td>'
                f'<td><code>{_escape_html(item["path"])}</code></td>'
                f'<td>{_escape_html(item["detail"])}</td>'
                f'</tr>'
            )
        html.append('</tbody></table>')

    html.append('</div>')

    text_diff = yaml_diff.get("text_diff", {})
    if text_diff:
        html.append('<hr class="diff-separator">')
        html.append('<h3 class="semantic-diff-title">Line-by-Line Comparison</h3>')
        html.append(render_diff_html(text_diff))

    return "\n".join(html)


def _escape_html(text: str) -> str:
    return (
        text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace(" ", "&nbsp;")
    )
