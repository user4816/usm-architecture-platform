/**
 * app.js - USM AI Platform Frontend Logic
 */

const state = {
    currentPkg: null,
    currentOp: null,
    currentFile: null,
    modified: false,
    packages: [],
    activeTab: 'chat',
    activeView: 'dashboard',   // GNB view: dashboard | nbi | rfp | dimensioning | statistics
    isComparisonActive: false,
    cmdHistory: [],
    historyIndex: -1,
    compareBase: { pkg: null, op: null },
    compareTarget: { pkg: null, op: null },
    compareFile: 'doc.yaml',
    usmVersion: 'USMv1',
    comparePackages: [],
    selectedModel: 'qwen2.5:32b-instruct-q5_K_M',
    chatHistory: [],
    currentAbortController: null,
    // Upload tab state
    uploadFiles: [],
    uploadChatHistory: [],
    uploadCmdHistory: [],
    uploadHistoryIndex: -1,
    uploadTaskId: null,
    uploadEventSource: null,
    uploadAbortController: null
};

const $ = id => document.getElementById(id);
const $$ = sel => document.querySelectorAll(sel);

document.addEventListener('DOMContentLoaded', () => {
    initTheme();
    initGlobalNav();
    loadPackages();
    loadComparePackages();
    initTabs();
    initSidebar();
    initSave();
    initPdf();
    initVersionToggles();
    initModelSelector();
    initUpload();
    initRfp();
});

// ─── Theme Toggle ───────────────────────────────────────
function initTheme() {
    const saved = localStorage.getItem('nbi-theme');
    if (saved === 'dark') {
        document.documentElement.setAttribute('data-theme', 'dark');
    }
    updateThemeIcons();
}

function toggleTheme() {
    const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
    if (isDark) {
        document.documentElement.removeAttribute('data-theme');
        localStorage.setItem('nbi-theme', 'light');
    } else {
        document.documentElement.setAttribute('data-theme', 'dark');
        localStorage.setItem('nbi-theme', 'dark');
    }
    updateThemeIcons();
}

function updateThemeIcons() {
    const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
    const sun = $('icon-sun');
    const moon = $('icon-moon');
    if (sun) sun.classList.toggle('hidden', !isDark);
    if (moon) moon.classList.toggle('hidden', isDark);
}

// ─── Model Selector (Chat) ──────────────────────────────
function initModelSelector() {
    const btn = $('btn-model-select');
    const menu = $('model-dropdown-menu');
    const textSpan = $('selected-model-text');
    const options = $$('.s-model-option');

    if (!btn || !menu) return;

    // Toggle dropdown
    btn.addEventListener('click', (e) => {
        e.stopPropagation();
        const isOpen = !menu.classList.contains('hidden');
        if (isOpen) {
            menu.classList.add('hidden');
            btn.classList.remove('open');
        } else {
            menu.classList.remove('hidden');
            btn.classList.add('open');
        }
    });

    // API URL map from config.yaml llm_apis
    const MODEL_API_MAP = {
        'qwen2.5:32b-instruct-q5_K_M': 'qwen',
        'Gauss': 'gauss'
    };

    // Handle option selection
    options.forEach(opt => {
        opt.addEventListener('click', (e) => {
            e.stopPropagation();
            state.selectedModel = opt.dataset.model;
            textSpan.textContent = state.selectedModel;

            // Update active state
            options.forEach(o => o.classList.remove('active'));
            opt.classList.add('active');

            // Close dropdown
            menu.classList.add('hidden');
            btn.classList.remove('open');

            // Validate API URL — disable send immediately until validation completes
            const sendBtn = $('btn-chat-send');
            sendBtn.disabled = true;
            const apiKey = MODEL_API_MAP[state.selectedModel];
            _validateModelApi(state.selectedModel, apiKey, sendBtn);
        });
    });

    // Initial validation for the default model
    const sendBtn = $('btn-chat-send');
    const defaultApiKey = MODEL_API_MAP[state.selectedModel] || 'qwen';
    _validateModelApi(state.selectedModel, defaultApiKey, sendBtn);

    // Close when clicking outside
    document.addEventListener('click', () => {
        if (!menu.classList.contains('hidden')) {
            menu.classList.add('hidden');
            btn.classList.remove('open');
        }
    });
}

// ─── Version Toggle ─────────────────────────────────────
function initVersionToggles() {
    // Sidebar version toggle
    const sidebarToggle = $('sidebar-version-toggle');
    if (sidebarToggle) {
        sidebarToggle.querySelectorAll('.s-version-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                const version = btn.dataset.version;
                if (version === state.usmVersion) return;

                // Update active state
                sidebarToggle.querySelectorAll('.s-version-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');

                state.usmVersion = version;

                // Reset current file state
                state.currentPkg = null;
                state.currentOp = null;
                state.currentFile = null;
                state.modified = false;

                // Reset editor
                $('editor-empty').classList.remove('hidden');
                $('editor-area').classList.add('hidden');
                $('editor-area').value = '';
                $('btn-save').disabled = true;
                $('btn-pdf').disabled = true;

                // Reset preview
                const previewFrame = $('preview-frame');
                const previewEmpty = $('preview-empty');
                if (previewFrame) { previewFrame.classList.add('hidden'); previewFrame.src = ''; }
                if (previewEmpty) previewEmpty.classList.remove('hidden');

                updateStatus('');
                loadPackages();
                loadComparePackages();
                showToast(`Switched to ${version === 'USMv1' ? 'USM v1' : 'USM v2'}`, 'info');
            });
        });
    }

}

// ─── Package tree ────────────────────────────────────────
async function loadPackages() {
    try {
        const res = await fetch(`/api/packages?usm_version=${state.usmVersion}`);
        const data = await res.json();
        state.packages = data.packages || data;
        renderTree(state.packages);
    } catch (e) {
        showToast('Failed to load packages', 'error');
    }
}

async function loadComparePackages() {
    try {
        const res = await fetch(`/api/packages?usm_version=${state.usmVersion}`);
        const data = await res.json();
        state.comparePackages = data.packages || data;
        populateCompareDropdowns();
    } catch (e) {
        showToast('Failed to load compare packages', 'error');
    }
}

function renderTree(packages) {
    const container = $('tree-container');
    if (!packages.length) {
        container.innerHTML = '<div style="text-align:center;padding:40px 0;color:var(--s-text-muted)">No packages found</div>';
        return;
    }

    let html = '';
    packages.forEach(pkg => {
        html += `<div style="margin-bottom:4px">
            <div class="tree-pkg"
                 onclick="togglePkg(this, '${pkg.name}')">
                <svg style="width:15px;height:15px;transition:transform 0.2s;flex-shrink:0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"/>
                </svg>
                <svg style="width:18px;height:18px;color:#fbbf24;flex-shrink:0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                        d="M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z"/>
                </svg>
                <span>${pkg.name}</span>
            </div>
            <div class="pkg-children hidden" style="margin-left:16px">`;

        pkg.operators.forEach(op => {
            html += `<div style="margin-bottom:2px">
                <div class="tree-op"
                     onclick="toggleOperator(this, '${pkg.name}', '${op.name}')">
                    <svg style="width:13px;height:13px;transition:transform 0.2s;flex-shrink:0;margin-right:4px" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"/>
                    </svg>
                    ${op.name}
                </div>
                <div class="op-files hidden" style="margin-left:16px">`;

            op.files.forEach(file => {
                const iconClass = file.endsWith('.md') ? 'file-icon-md' :
                    file.endsWith('.yaml') || file.endsWith('.yml') ? 'file-icon-yaml' : 'file-icon-mmd';
                html += `<div class="file-item"
                     data-pkg="${pkg.name}" data-op="${op.name}" data-file="${file}"
                     onclick="loadFile('${pkg.name}', '${op.name}', '${file}')">
                    <span class="${iconClass}" style="font-size:10px">●</span>
                    <span>${file}</span>
                </div>`;
            });

            html += `</div></div>`;
        });

        html += `</div></div>`;
    });

    container.innerHTML = html;
}

function togglePkg(el, pkgName) {
    const children = el.nextElementSibling;
    const arrow = el.querySelector('svg');
    if (children) {
        children.classList.toggle('hidden');
        arrow.classList.toggle('rotate-90');
    }
}

function toggleOperator(el, pkgName, opName) {
    const files = el.nextElementSibling;
    const arrow = el.querySelector('svg');
    if (files) {
        files.classList.toggle('hidden');
        arrow.classList.toggle('rotate-90');
    }
}

// ─── File loading ────────────────────────────────────────
async function loadFile(pkg, op, file) {
    if (state.modified && !confirm('Unsaved changes will be lost. Continue?')) return;

    try {
        let url;
        if (file === 'doc.md') {
            url = `/api/doc/${pkg}/${op}?usm_version=${state.usmVersion}`;
        } else {
            url = `/api/files/${pkg}/${op}/${file}?usm_version=${state.usmVersion}`;
        }
        const res = await fetch(url);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();

        state.currentPkg = pkg;
        state.currentOp = op;
        state.currentFile = file;
        state.modified = false;

        // Switch to editor tab
        switchTab('editor');

        // Show editor
        $('editor-empty').classList.add('hidden');
        $('editor-area').classList.remove('hidden');
        $('editor-area').value = data.content;

        // Enable buttons
        $('btn-save').disabled = false;
        $('btn-pdf').disabled = false;

        // Highlight active file in sidebar
        $$('.file-item').forEach(el => el.classList.remove('active'));
        const activeEl = document.querySelector(
            `.file-item[data-pkg="${pkg}"][data-op="${op}"][data-file="${file}"]`
        );
        if (activeEl) activeEl.classList.add('active');

        updateStatus(`[${state.usmVersion}] ${pkg}/${op}/${file}`);
        showToast(`${file} loaded`, 'success');

    } catch (e) {
        showToast(`Failed to load ${file}: ${e.message}`, 'error');
    }
}

// ─── Tab system ──────────────────────────────────────────
function initTabs() {
    $$('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const tab = btn.dataset.tab;
            switchTab(tab);
        });
    });
}

function switchTab(tab) {
    state.activeTab = tab;

    $$('.tab-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.tab === tab);
    });

    $('panel-chat').classList.toggle('hidden', tab !== 'chat');
    $('panel-editor').classList.toggle('hidden', tab !== 'editor');
    $('panel-preview').classList.toggle('hidden', tab !== 'preview');
    $('panel-compare').classList.toggle('hidden', tab !== 'compare');
    const uploadPanel = $('panel-upload');
    if (uploadPanel) uploadPanel.classList.toggle('hidden', tab !== 'upload');

    // Toggle header buttons based on active tab
    const uploadPdfBtn = $('btn-upload-pdf');
    const mainPdfBtn = $('btn-pdf');

    if (tab === 'upload') {
        // Show Upload PDF, hide main PDF
        if (uploadPdfBtn) uploadPdfBtn.classList.remove('hidden');
        if (mainPdfBtn) mainPdfBtn.classList.add('hidden');
    } else {
        // Hide Upload PDF, show main PDF
        if (uploadPdfBtn) uploadPdfBtn.classList.add('hidden');
        if (mainPdfBtn) mainPdfBtn.classList.remove('hidden');
    }

    if (tab === 'preview' && state.currentPkg) {
        loadPreview();
    }

    if (tab === 'compare') {
        $$('.file-item').forEach(el => el.classList.remove('active'));
        $('btn-pdf').disabled = !state.isComparisonActive;
    } else if (tab === 'chat') {
        const hasChat = state.chatHistory.some(m => m.role === 'assistant') || document.querySelector('#chat-messages .assistant');
        $('btn-pdf').disabled = !hasChat;
    } else if (tab === 'upload') {
        // btn-pdf already hidden — no action needed
    } else {
        $('btn-pdf').disabled = !state.currentPkg;
    }
}

// ─── Preview ─────────────────────────────────────────────
async function loadPreview() {
    const frame = $('preview-frame');
    const empty = $('preview-empty');

    if (!state.currentPkg || !state.currentOp) {
        frame.classList.add('hidden');
        empty.classList.remove('hidden');
        return;
    }

    frame.classList.remove('hidden');
    empty.classList.add('hidden');
    frame.src = `/api/preview/${state.currentPkg}/${state.currentOp}?usm_version=${state.usmVersion}`;
}

// ─── Save ────────────────────────────────────────────────
function initSave() {
    const editor = $('editor-area');
    const saveBtn = $('btn-save');

    editor.addEventListener('input', () => {
        state.modified = true;
        updateStatus('Modified');
    });

    saveBtn.addEventListener('click', async () => {
        if (!state.currentPkg || !state.currentFile) return;

        saveBtn.disabled = true;
        saveBtn.innerHTML = '<span class="spinner"></span> Saving...';

        try {
            let url;
            if (state.currentFile === 'doc.md') {
                url = `/api/doc/${state.currentPkg}/${state.currentOp}?usm_version=${state.usmVersion}`;
            } else {
                url = `/api/files/${state.currentPkg}/${state.currentOp}/${state.currentFile}?usm_version=${state.usmVersion}`;
            }

            const res = await fetch(url, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ content: editor.value })
            });

            const data = await res.json();
            if (!res.ok) {
                showToast(data.detail || data.error || 'Save failed', 'error');
                return;
            }

            state.modified = false;
            showToast('Saved successfully', 'success');
            updateStatus(`[${state.usmVersion}] ${state.currentPkg}/${state.currentOp}/${state.currentFile}`);
        } catch (e) {
            showToast(`Save error: ${e.message}`, 'error');
        } finally {
            saveBtn.disabled = false;
            saveBtn.innerHTML = `<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                    d="M8 7H5a2 2 0 00-2 2v9a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-3m-1 4l-3 3m0 0l-3-3m3 3V4"/>
            </svg> Save`;
        }
    });
}

// ─── PDF Download ────────────────────────────────────────
function initPdf() {
    $('btn-pdf').addEventListener('click', async () => {
        const btn = $('btn-pdf');
        const btnText = $('pdf-btn-text');

        if (state.activeTab === 'chat') {
            downloadChatPDF();
            return;
        }

        if (state.activeTab === 'compare') {
            if (!state.isComparisonActive || !state.compareBase.pkg || !state.compareTarget.pkg) return;

            btn.disabled = true;
            btnText.textContent = 'Generating...';

            try {
                const url = `/api/pdf/${state.compareTarget.pkg}/${state.compareTarget.op}`
                    + `?usm_version=${state.usmVersion}`
                    + `&diff_base_pkg=${state.compareBase.pkg}`
                    + `&diff_base_op=${state.compareBase.op}`
                    + `&diff_file=${state.compareFile}`
                    + `&usm_a=${state.usmVersion}`
                    + `&usm_b=${state.usmVersion}`;

                const res = await fetch(url);
                if (!res.ok) throw new Error(`HTTP ${res.status}`);

                const blob = await res.blob();
                const a = document.createElement('a');
                a.href = URL.createObjectURL(blob);
                a.download = `diff_${state.compareBase.pkg}_vs_${state.compareTarget.pkg}_${state.compareFile.split('.')[0]}.pdf`;
                a.click();
                URL.revokeObjectURL(a.href);
                showToast('Diff PDF downloaded', 'success');
            } catch (e) {
                showToast(`PDF error: ${e.message}`, 'error');
            } finally {
                btn.disabled = false;
                btnText.textContent = 'Download PDF';
            }
        } else {
            if (!state.currentPkg || !state.currentOp) return;

            btn.disabled = true;
            btnText.textContent = 'Generating...';

            try {
                const url = `/api/pdf/${state.currentPkg}/${state.currentOp}?usm_version=${state.usmVersion}`;
                const res = await fetch(url);
                if (!res.ok) throw new Error(`HTTP ${res.status}`);

                const blob = await res.blob();
                const a = document.createElement('a');
                a.href = URL.createObjectURL(blob);
                a.download = `NBI_${state.currentPkg}_${state.currentOp}.pdf`;
                a.click();
                URL.revokeObjectURL(a.href);
                showToast('PDF downloaded', 'success');
            } catch (e) {
                showToast(`PDF error: ${e.message}`, 'error');
            } finally {
                btn.disabled = false;
                btnText.textContent = 'Download PDF';
            }
        }
    });
}

// ─── Compare functionality ───────────────────────────────
function populateCompareDropdowns() {
    const pkgs = state.comparePackages;
    const vLabel = state.usmVersion === 'USMv1' ? 'v1' : 'v2';

    ['compare-pkg-a', 'compare-pkg-b'].forEach(id => {
        const sel = $(id);
        sel.innerHTML = `<option value="">Package</option>`;
        pkgs.forEach(pkg => {
            sel.innerHTML += `<option value="${pkg.name}">[${vLabel}] ${pkg.name}</option>`;
        });
    });

    $('compare-pkg-a').addEventListener('change', () => updateOperatorDropdown('compare-pkg-a', 'compare-op-a'));
    $('compare-pkg-b').addEventListener('change', () => updateOperatorDropdown('compare-pkg-b', 'compare-op-b'));
}

function updateOperatorDropdown(pkgSelectId, opSelectId) {
    const pkgName = $(pkgSelectId).value;
    const opSel = $(opSelectId);
    opSel.innerHTML = '<option value="">Operator</option>';

    if (!pkgName) return;

    const pkg = state.comparePackages.find(p => p.name === pkgName);
    if (pkg) {
        pkg.operators.forEach(op => {
            opSel.innerHTML += `<option value="${op.name}">${op.name}</option>`;
        });
    }
}

async function runCompare() {
    const pkgA = $('compare-pkg-a').value;
    const opA = $('compare-op-a').value;
    const pkgB = $('compare-pkg-b').value;
    const opB = $('compare-op-b').value;
    const file = $('compare-file').value;

    if (!pkgA || !opA || !pkgB || !opB) {
        showToast('Please select both Base and Target packages', 'warning');
        return;
    }

    const btn = $('btn-compare');
    btn.disabled = true;
    btn.textContent = 'Comparing...';

    try {
        const url = `/api/diff/preview/${pkgA}/${opA}/${pkgB}/${opB}?file=${file}&usm_a=${state.usmVersion}&usm_b=${state.usmVersion}`;
        const frame = $('compare-frame');
        const empty = $('compare-empty');

        frame.src = url;
        frame.classList.remove('hidden');
        empty.classList.add('hidden');

        // Set comparison active flag for auto PDF diff
        state.isComparisonActive = true;
        state.compareBase = { pkg: pkgA, op: opA };
        state.compareTarget = { pkg: pkgB, op: opB };
        state.compareFile = file;

        // Enable PDF download now that comparison is ready
        $('btn-pdf').disabled = false;

        showToast('Comparison loaded', 'success');
    } catch (e) {
        showToast(`Compare error: ${e.message}`, 'error');
    } finally {
        btn.disabled = false;
        btn.textContent = 'Run Compare';
    }
}

function resetCompare() {
    // Reset dropdowns
    $('compare-pkg-a').value = '';
    $('compare-op-a').innerHTML = '<option value="">Operator</option>';
    $('compare-pkg-b').value = '';
    $('compare-op-b').innerHTML = '<option value="">Operator</option>';
    $('compare-file').value = 'doc.yaml';

    // Reset comparison state
    state.isComparisonActive = false;
    state.compareBase = { pkg: null, op: null };
    state.compareTarget = { pkg: null, op: null };
    state.compareFile = 'doc.yaml';

    // Reset UI
    const frame = $('compare-frame');
    const empty = $('compare-empty');
    frame.classList.add('hidden');
    frame.src = '';
    empty.classList.remove('hidden');

    // Disable PDF download since comparison is cleared
    $('btn-pdf').disabled = true;

    showToast('Compare reset', 'info');
}

// ─── Sidebar toggle ─────────────────────────────────────
function initSidebar() {
    $('sidebar-toggle').addEventListener('click', () => {
        $('sidebar').classList.toggle('sidebar-collapsed');
    });
}

// ─── Status ──────────────────────────────────────────────
function updateStatus(text) {
    $('status-badge').textContent = text;
}

// ─── Toast ───────────────────────────────────────────────
function showToast(message, type = 'info') {
    const container = $('toast-container');
    const colors = {
        success: 'background:#059669;',
        error: 'background:#dc2626;',
        warning: 'background:#d97706;',
        info: 'background:var(--s-blue);'
    };

    const toast = document.createElement('div');
    toast.className = 'toast';
    toast.style.cssText = `${colors[type] || colors.info}color:white;`;
    toast.textContent = message;
    container.appendChild(toast);
    setTimeout(() => toast.remove(), 3000);
}

// ─── Chat Functions ─────────────────────────────────────
async function sendChat() {
    const input = $('chat-input');
    const text = input.value.trim();
    if (!text) return;

    // ── Push to command history ──
    state.cmdHistory.push(text);
    state.historyIndex = state.cmdHistory.length;

    const messages = $('chat-messages');
    const sendBtn = $('btn-chat-send');
    const modelBtn = $('btn-model-select');

    // Remove welcome screen if present
    const welcome = messages.querySelector('.s-chat-welcome');
    if (welcome) welcome.remove();

    // Add user bubble
    const userBubble = document.createElement('div');
    userBubble.className = 's-chat-bubble user';
    userBubble.textContent = text;
    messages.appendChild(userBubble);

    // Clear input
    input.value = '';
    input.style.height = 'auto';

    // Scroll to bottom
    messages.scrollTop = messages.scrollHeight;

    // Show typing indicator (replaced by status text once stream begins)
    const typing = document.createElement('div');
    typing.className = 's-chat-typing';
    typing.innerHTML = '<span></span><span></span><span></span>';
    messages.appendChild(typing);
    messages.scrollTop = messages.scrollHeight;

    // Disable inputs while loading
    input.disabled = true;
    sendBtn.disabled = true;
    if (modelBtn) modelBtn.disabled = true;

    // Prepare assistant bubble (hidden until first token)
    const assistantBubble = document.createElement('div');
    assistantBubble.className = 's-chat-bubble assistant';
    const replyDiv = document.createElement('div');
    replyDiv.className = 's-chat-reply-text';
    assistantBubble.appendChild(replyDiv);

    // SSE state
    let meta = { sources: [], route_used: '', model_used: '' };
    let firstTokenReceived = false;
    let statusEl = null;
    let rawReply = '';   // accumulate raw markdown text

    // Configure marked for GFM tables
    if (typeof marked !== 'undefined') {
        marked.setOptions({ gfm: true, breaks: true });
    }

    // AbortController for cancelling on disconnect
    const abortController = new AbortController();
    state.currentAbortController = abortController;

    // ── Switch to Stop mode ──
    const sendIcon = $('send-icon');
    const stopIcon = $('stop-icon');
    sendBtn.classList.add('stop-mode');
    if (sendIcon) sendIcon.classList.add('hidden');
    if (stopIcon) stopIcon.classList.remove('hidden');
    sendBtn.disabled = false;  // Ensure stop button is clickable
    sendBtn.onclick = () => {
        abortController.abort();
        showToast('Generation stopped', 'info');
    };

    try {
        const res = await fetch('/api/chat', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                message: text,
                model: state.selectedModel,
                context_version: state.currentPkg || null,
                context_operator: state.currentOp || null,
                chat_history: (state.chatHistory || []).slice(-6)
            }),
            signal: abortController.signal
        });

        if (!res.ok) throw new Error(`HTTP ${res.status}`);

        // ── Stream reader with line buffer ──
        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';

        while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            // ── Force stop check: kill connection immediately on abort ──
            if (abortController.signal.aborted) {
                try { await reader.cancel(); } catch (_) { }
                try { reader.releaseLock(); } catch (_) { }
                if (!firstTokenReceived) {
                    messages.appendChild(assistantBubble);
                }
                const stoppedMsg = document.createElement('div');
                stoppedMsg.className = 's-chat-stopped';
                stoppedMsg.textContent = '[System] Stopped by user request.';
                assistantBubble.appendChild(stoppedMsg);
                messages.scrollTop = messages.scrollHeight;
                break;
            }

            buffer += decoder.decode(value, { stream: true });

            // Normalize line endings (Windows compatibility)
            buffer = buffer.replace(/\r\n/g, '\n').replace(/\r/g, '\n');

            // Process complete SSE frames (delimited by double newline)
            let boundary;
            while ((boundary = buffer.indexOf('\n\n')) !== -1) {
                const frame = buffer.slice(0, boundary);
                buffer = buffer.slice(boundary + 2);

                // Debug: log raw SSE frame
                console.debug('[SSE frame]', JSON.stringify(frame));

                // Parse SSE frame
                let eventType = 'message';
                let dataLines = [];
                for (const line of frame.split('\n')) {
                    if (line.startsWith('event: ')) {
                        eventType = line.slice(7).trim();
                    } else if (line.startsWith('data: ')) {
                        dataLines.push(line.slice(6));
                    } else if (line.startsWith('data:')) {
                        dataLines.push(line.slice(5));
                    }
                }
                const dataStr = dataLines.join('\n');

                // ── Handle event types ──
                if (eventType === 'status') {
                    // Show status in a temporary element (replaces typing dots)
                    if (typing.parentNode) typing.remove();
                    if (!statusEl) {
                        statusEl = document.createElement('div');
                        statusEl.className = 's-chat-typing';
                        statusEl.style.cssText = 'font-size:0.8rem;color:var(--s-text-muted);padding:8px 14px;';
                        messages.appendChild(statusEl);
                    }
                    statusEl.textContent = dataStr;
                    messages.scrollTop = messages.scrollHeight;

                } else if (eventType === 'meta') {
                    try { meta = JSON.parse(dataStr); } catch (_) { }

                } else if (eventType === 'token') {
                    if (!firstTokenReceived) {
                        firstTokenReceived = true;
                        if (typing.parentNode) typing.remove();
                        if (statusEl && statusEl.parentNode) statusEl.remove();
                        messages.appendChild(assistantBubble);
                        // ── Immediately enable PDF button on first token ──
                        const pdfBtnEarly = document.getElementById('btn-pdf');
                        if (pdfBtnEarly) {
                            pdfBtnEarly.disabled = false;
                            pdfBtnEarly.style.opacity = '1';
                        }
                    }
                    rawReply += dataStr;
                    // Live markdown rendering (including raw mermaid during stream)
                    if (typeof marked !== 'undefined') {
                        replyDiv.innerHTML = marked.parse(rawReply);
                    } else {
                        replyDiv.textContent = rawReply;
                    }
                    messages.scrollTop = messages.scrollHeight;

                } else if (eventType === 'done') {
                    // Final markdown render
                    if (rawReply && typeof marked !== 'undefined') {
                        replyDiv.innerHTML = marked.parse(rawReply);
                    }
                    // Append sources 
                    if (meta.sources && meta.sources.length > 0) {
                        const srcDiv = document.createElement('div');
                        srcDiv.className = 's-chat-sources';
                        srcDiv.innerHTML = '<strong>Sources:</strong> ' + meta.sources.map(s =>
                            `<span class="s-chat-source-tag">${s}</span>`
                        ).join(' ');
                        assistantBubble.appendChild(srcDiv);
                    }
                    // Append route indicator
                    if (meta.route_used) {
                        const routeDiv = document.createElement('div');
                        routeDiv.className = 's-chat-route';
                        const routeLabel = meta.route_used === 'spec_lookup' ? '📋 Direct Lookup' : '🔍 RAG Search';
                        routeDiv.textContent = routeLabel;
                        assistantBubble.appendChild(routeDiv);
                    }
                    // Update conversation history
                    try {
                        const doneData = JSON.parse(dataStr);
                        if (doneData.context && doneData.context.length) {
                            state.chatHistory.push(...doneData.context);
                        }
                    } catch (_) { }

                } else if (eventType === 'error') {
                    if (!firstTokenReceived) {
                        if (typing.parentNode) typing.remove();
                        if (statusEl && statusEl.parentNode) statusEl.remove();
                        messages.appendChild(assistantBubble);
                    }
                    replyDiv.style.color = '#dc2626';
                    replyDiv.textContent = `Error: ${dataStr}`;
                    messages.scrollTop = messages.scrollHeight;
                }
            }
        }

        // If no token was ever received, show fallback
        if (!firstTokenReceived) {
            if (typing.parentNode) typing.remove();
            if (statusEl && statusEl.parentNode) statusEl.remove();
            replyDiv.textContent = 'No response';
            messages.appendChild(assistantBubble);
        }

    } catch (e) {
        if (typing.parentNode) typing.remove();
        if (statusEl && statusEl.parentNode) statusEl.remove();
        if (e.name === 'AbortError') {
            // Show stopped message in the current assistant bubble
            if (!firstTokenReceived) {
                messages.appendChild(assistantBubble);
            }
            const stoppedMsg = document.createElement('div');
            stoppedMsg.className = 's-chat-stopped';
            stoppedMsg.textContent = '[System] Stopped by user request.';
            assistantBubble.appendChild(stoppedMsg);
            messages.scrollTop = messages.scrollHeight;
            return;
        }
        const errorBubble = document.createElement('div');
        errorBubble.className = 's-chat-bubble assistant';
        errorBubble.style.color = '#dc2626';
        errorBubble.textContent = `Error: ${e.message}`;
        messages.appendChild(errorBubble);
    } finally {
        // Re-create AbortController for next query
        state.currentAbortController = null;

        // ── Restore Send button ──
        const sendIconRestore = $('send-icon');
        const stopIconRestore = $('stop-icon');
        sendBtn.classList.remove('stop-mode');
        if (sendIconRestore) sendIconRestore.classList.remove('hidden');
        if (stopIconRestore) stopIconRestore.classList.add('hidden');
        sendBtn.onclick = () => sendChat();

        // Re-enable inputs
        input.disabled = false;
        sendBtn.disabled = false;
        if (modelBtn) modelBtn.disabled = false;
        input.focus();
        messages.scrollTop = messages.scrollHeight;

        // Enable PDF download if chat history has at least one assistant reply
        // Also check that html2pdf library is loaded before enabling
        const hasAssistantMsg = state.chatHistory.some(m => m.role === 'assistant');
        const pdfBtn = document.getElementById('btn-pdf');
        if (pdfBtn && (hasAssistantMsg || firstTokenReceived)) {
            const html2pdfReady = typeof html2pdf !== 'undefined';
            if (html2pdfReady) {
                pdfBtn.disabled = false;
                pdfBtn.style.opacity = '1';
            } else {
                console.warn('[PDF] html2pdf library not loaded yet');
            }
        }

        // ── Mermaid rendering (post-stream) ──
        // Only process NEW code blocks (skip already-processed ones)
        const mermaidCodes = messages.querySelectorAll('pre:not([data-mermaid-processed]) code.language-mermaid');
        if (mermaidCodes.length > 0) {
            const newMermaidDivs = [];
            mermaidCodes.forEach(codeEl => {
                const pre = codeEl.parentElement;
                pre.setAttribute('data-mermaid-processed', 'true');
                // Use textContent to get unescaped characters (e.g. A->>B)
                let raw = codeEl.textContent;
                // Safety: decode any residual HTML entities (e.g. &gt; → >)
                const tmp = document.createElement('textarea');
                tmp.innerHTML = raw;
                raw = tmp.value;
                const mermaidDiv = document.createElement('div');
                mermaidDiv.className = 'mermaid';
                mermaidDiv.textContent = raw;
                pre.style.display = 'none';  // hide raw code block
                pre.insertAdjacentElement('afterend', mermaidDiv);
                newMermaidDivs.push(mermaidDiv);
            });
            if (typeof mermaid !== 'undefined') {
                try {
                    await mermaid.run({ nodes: newMermaidDivs });
                } catch (mErr) {
                    console.warn('[Mermaid] render error:', mErr);
                }
            }
        }
    }
}

function chatSuggest(btn) {
    const query = btn.dataset.query || btn.textContent.trim();
    $('chat-input').value = query;
    sendChat();
}

function resetChat() {
    const messages = $('chat-messages');
    messages.innerHTML = `
        <div class="s-chat-welcome">
            <div class="s-chat-welcome-icon">
                <svg fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5"
                        d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z" />
                </svg>
            </div>
            <h2 class="s-chat-welcome-title">USM NBI Assistant</h2>
            <p class="s-chat-welcome-desc">Ask questions about NBI specifications, compare packages, or explore API details.</p>
            <div class="s-chat-suggestions">
                <button class="s-chat-suggestion" onclick="chatSuggest(this)" data-query="Summarize USMv1 26A Verizon changes">
                    <span class="action-icon">📋</span>
                    <span class="action-title">Summarize changes</span>
                    <span class="action-desc">Compare version differences for a package</span>
                </button>
                <button class="s-chat-suggestion" onclick="chatSuggest(this)" data-query="Show USMv1 26B Verizon CM REST API specs">
                    <span class="action-icon">🔗</span>
                    <span class="action-title">REST API Specification</span>
                    <span class="action-desc">View detailed API endpoints and parameters</span>
                </button>
                <button class="s-chat-suggestion" onclick="chatSuggest(this)" data-query="USMv1 26A Verizon and 26B Verizon differences">
                    <span class="action-icon">⚖️</span>
                    <span class="action-title">Compare packages</span>
                    <span class="action-desc">Diff two package versions side by side</span>
                </button>
            </div>
        </div>`;
    $('chat-input').value = '';
    $('chat-input').style.height = 'auto';
    state.chatHistory = [];
    if (state.currentAbortController) {
        state.currentAbortController.abort();
        state.currentAbortController = null;
    }
    switchTab('chat');
}

// Auto-grow chat textarea + Command history (Arrow Up/Down)
document.addEventListener('DOMContentLoaded', () => {
    const chatInput = $('chat-input');
    if (chatInput) {
        chatInput.addEventListener('input', () => {
            chatInput.style.height = 'auto';
            chatInput.style.height = Math.min(chatInput.scrollHeight, 120) + 'px';
        });

        // ── Arrow Up/Down: cycle through command history ──
        chatInput.addEventListener('keydown', (e) => {
            if (e.key === 'ArrowUp' && state.cmdHistory.length > 0) {
                e.preventDefault();
                if (state.historyIndex > 0) state.historyIndex--;
                chatInput.value = state.cmdHistory[state.historyIndex] || '';
            } else if (e.key === 'ArrowDown' && state.cmdHistory.length > 0) {
                e.preventDefault();
                if (state.historyIndex < state.cmdHistory.length - 1) {
                    state.historyIndex++;
                    chatInput.value = state.cmdHistory[state.historyIndex];
                } else {
                    state.historyIndex = state.cmdHistory.length;
                    chatInput.value = '';
                }
            }
        });
    }
});

// ─── API Validation Helper ───────────────────────────────────
async function _validateModelApi(modelName, apiKey, sendBtn) {
    try {
        const res = await fetch('/api/llm-status?model=' + encodeURIComponent(modelName));
        if (res.ok) {
            const data = await res.json();
            console.log(`[LLM-Status] model=${modelName}, api_url='${data.api_url}'`);
            if (!data.api_url) {
                showToast(`⚠️ ${modelName} API URL is not configured. Send is disabled.`, 'warning');
                if (sendBtn) sendBtn.disabled = true;
                return;
            }
        }
    } catch (_) {
        // Fallback: can't reach server, allow anyway
    }
    if (sendBtn) sendBtn.disabled = false;
    showToast(`✅ Model set to ${modelName}`, 'info');
}

// ─── Reindex Documents ──────────────────────────────────────
async function reindexDocs() {
    showToast('Re-indexing documents...', 'info');
    try {
        const res = await fetch('/api/reindex', { method: 'POST' });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        showToast(`Indexed: ${data.processed} new, ${data.skipped} unchanged, ${data.total} total`, 'success');
    } catch (e) {
        showToast(`Reindex error: ${e.message}`, 'error');
    }
}

// ─── Chat PDF Export ─────────────────────────────────────────

/**
 * Convert a Mermaid SVG element to a rasterised <img> (PNG data-URL).
 * Returns { img, parent, svg } so the caller can restore later.
 */
function _svgToImg(svgEl) {
    return new Promise((resolve) => {
        const bbox = svgEl.getBoundingClientRect();
        const SCALE = 2;   // retina-quality raster
        const w = Math.ceil(bbox.width * SCALE);
        const h = Math.ceil(bbox.height * SCALE);

        // Serialise SVG → Blob URL
        const clone = svgEl.cloneNode(true);
        clone.setAttribute('width', bbox.width);
        clone.setAttribute('height', bbox.height);
        const xml = new XMLSerializer().serializeToString(clone);
        const blob = new Blob([xml], { type: 'image/svg+xml;charset=utf-8' });
        const url = URL.createObjectURL(blob);

        const img = new Image();
        img.onload = () => {
            const cvs = document.createElement('canvas');
            cvs.width = w;
            cvs.height = h;
            const ctx = cvs.getContext('2d');
            ctx.fillStyle = '#FFFFFF';
            ctx.fillRect(0, 0, w, h);
            ctx.drawImage(img, 0, 0, w, h);
            URL.revokeObjectURL(url);

            const pngImg = document.createElement('img');
            pngImg.src = cvs.toDataURL('image/png');
            pngImg.style.width = bbox.width + 'px';
            pngImg.style.height = bbox.height + 'px';
            pngImg.style.display = 'block';
            pngImg.className = 'pdf-rasterised-svg';
            resolve(pngImg);
        };
        img.onerror = () => {
            URL.revokeObjectURL(url);
            resolve(null);   // skip on failure
        };
        img.src = url;
    });
}

async function downloadChatPDF() {
    const chatMessages = $('chat-messages');
    if (!chatMessages) return;

    // Safety guard: warn if chat is extremely long
    const bubbleCount = chatMessages.querySelectorAll('.s-chat-bubble').length;
    if (bubbleCount > 200) {
        showToast('Chat is very long — PDF may be truncated.', 'warning');
    }

    showToast('Generating PDF...', 'info');

    // ── Save original layout state ──
    const origHeight = chatMessages.style.height;
    const origMaxHeight = chatMessages.style.maxHeight;
    const origOverflow = chatMessages.style.overflow;
    const origBg = chatMessages.style.background;
    const origScrollTop = chatMessages.scrollTop;

    // ── Unroll: make entire chat visible for capture ──
    chatMessages.style.height = 'auto';
    chatMessages.style.maxHeight = 'none';
    chatMessages.style.overflow = 'visible';
    chatMessages.style.background = '#FFFFFF';

    // ── Rasterise Mermaid SVGs → PNG <img> for html2canvas ──
    const svgSwaps = [];
    const mermaidSVGs = chatMessages.querySelectorAll('.mermaid svg');
    for (const svg of mermaidSVGs) {
        const pngImg = await _svgToImg(svg);
        if (pngImg) {
            svg.style.display = 'none';
            svg.parentElement.appendChild(pngImg);
            svgSwaps.push({ svg, pngImg });
        }
    }

    const restoreAll = () => {
        // Restore SVGs
        svgSwaps.forEach(({ svg, pngImg }) => {
            svg.style.display = '';
            if (pngImg.parentElement) pngImg.remove();
        });
        // Restore layout
        chatMessages.style.height = origHeight;
        chatMessages.style.maxHeight = origMaxHeight;
        chatMessages.style.overflow = origOverflow;
        chatMessages.style.background = origBg;
        chatMessages.scrollTop = origScrollTop;
    };

    const opt = {
        margin: [10, 10, 10, 10],
        filename: `NBI_Chat_${new Date().toISOString().slice(0, 10)}.pdf`,
        image: { type: 'png' },
        html2canvas: {
            scale: 3, useCORS: true, logging: false,
            scrollX: 0, scrollY: 0, backgroundColor: '#FFFFFF'
        },
        jsPDF: { unit: 'mm', format: 'a4', orientation: 'portrait' }
    };

    // ── Delay to let the browser fully paint ──
    setTimeout(() => {
        html2pdf().set(opt).from(chatMessages).save().then(() => {
            restoreAll();
            showToast('PDF downloaded', 'success');
        }).catch(err => {
            restoreAll();
            showToast(`PDF error: ${err.message}`, 'error');
        });
    }, 100);
}

// ─── Upload Tab Functions ────────────────────────────────────

function toggleUploadDropzone() {
    const wrapper = document.getElementById('upload-dropzone-wrapper');
    if (wrapper) wrapper.classList.toggle('collapsed');
}

function initUpload() {
    const dropzone = $('upload-dropzone');
    const fileInput = $('upload-file-input');
    if (!dropzone || !fileInput) return;

    // Click to browse
    dropzone.addEventListener('click', () => fileInput.click());

    // File input change
    fileInput.addEventListener('change', () => {
        if (fileInput.files.length) handleUploadFiles(fileInput.files);
        fileInput.value = ''; // reset for re-select
    });

    // Drag & Drop
    dropzone.addEventListener('dragover', (e) => {
        e.preventDefault();
        dropzone.classList.add('dragover');
    });
    dropzone.addEventListener('dragleave', () => {
        dropzone.classList.remove('dragover');
    });
    dropzone.addEventListener('drop', (e) => {
        e.preventDefault();
        dropzone.classList.remove('dragover');
        if (e.dataTransfer.files.length) handleUploadFiles(e.dataTransfer.files);
    });

    // Auto-grow upload chat textarea + Arrow key history
    const uploadInput = $('upload-chat-input');
    if (uploadInput) {
        uploadInput.addEventListener('input', () => {
            uploadInput.style.height = 'auto';
            uploadInput.style.height = Math.min(uploadInput.scrollHeight, 120) + 'px';
        });

        // ── Arrow Up/Down: cycle through upload command history ──
        uploadInput.addEventListener('keydown', (e) => {
            if (e.key === 'ArrowUp' && state.uploadCmdHistory.length > 0) {
                e.preventDefault();
                if (state.uploadHistoryIndex > 0) state.uploadHistoryIndex--;
                uploadInput.value = state.uploadCmdHistory[state.uploadHistoryIndex] || '';
            } else if (e.key === 'ArrowDown' && state.uploadCmdHistory.length > 0) {
                e.preventDefault();
                if (state.uploadHistoryIndex < state.uploadCmdHistory.length - 1) {
                    state.uploadHistoryIndex++;
                    uploadInput.value = state.uploadCmdHistory[state.uploadHistoryIndex];
                } else {
                    state.uploadHistoryIndex = state.uploadCmdHistory.length;
                    uploadInput.value = '';
                }
            }
        });
    }

    // Load existing files on init
    _loadExistingUploadFiles();

    // Upload Send button starts DISABLED (enabled when files are uploaded)
    const uploadSendBtn = $('btn-upload-chat-send');
    if (uploadSendBtn) uploadSendBtn.disabled = true;
}

async function _loadExistingUploadFiles() {
    try {
        const res = await fetch('/api/upload-docs');
        const data = await res.json();
        if (data.files && data.files.length > 0) {
            state.uploadFiles = data.files.map(f => ({
                name: f, progress: 100, status: 'done'
            }));
            _renderUploadFileList();
        }
    } catch (e) {
        // Silently fail — first load
    }
}

async function handleUploadFiles(fileList) {
    const files = Array.from(fileList);
    const MAX_FILES = 5;

    // Frontend validation: total count check
    const existingCount = state.uploadFiles.length;
    if (existingCount + files.length > MAX_FILES) {
        showToast(`Maximum ${MAX_FILES} files allowed. Currently ${existingCount} uploaded.`, 'error');
        return;
    }

    // Validate file types
    const validExts = ['.pdf', '.docx', '.doc'];
    for (const f of files) {
        const ext = '.' + f.name.split('.').pop().toLowerCase();
        if (!validExts.includes(ext)) {
            showToast(`Unsupported file type: ${f.name}`, 'error');
            return;
        }
    }

    // Add files to state with pending status
    files.forEach(f => {
        state.uploadFiles.push({ name: f.name, progress: 0, status: 'pending' });
    });
    _renderUploadFileList();

    // Build FormData
    const formData = new FormData();
    files.forEach(f => formData.append('files', f));

    try {
        const res = await fetch('/api/upload-docs', {
            method: 'POST',
            body: formData
        });
        const data = await res.json();

        if (!res.ok) {
            showToast(data.error || 'Upload failed', 'error');
            // Remove pending files
            state.uploadFiles = state.uploadFiles.filter(f => f.status !== 'pending');
            _renderUploadFileList();
            return;
        }

        state.uploadTaskId = data.task_id;
        showToast(`Uploading ${data.files.length} file(s)...`, 'info');

        // Start SSE progress tracking
        startUploadProgress(data.task_id);

    } catch (e) {
        showToast(`Upload error: ${e.message}`, 'error');
        state.uploadFiles = state.uploadFiles.filter(f => f.status !== 'pending');
        _renderUploadFileList();
    }
}

function startUploadProgress(taskId) {
    // Close any existing EventSource
    if (state.uploadEventSource) {
        state.uploadEventSource.close();
        state.uploadEventSource = null;
    }

    const evtSource = new EventSource(`/api/upload-progress/${taskId}`);
    state.uploadEventSource = evtSource;

    evtSource.onmessage = (event) => {
        try {
            const fileData = JSON.parse(event.data);

            if (fileData.error) {
                evtSource.close();
                state.uploadEventSource = null;
                return;
            }

            // Update state
            let allDone = true;
            for (const [filename, info] of Object.entries(fileData)) {
                const existing = state.uploadFiles.find(f => f.name === filename);
                if (existing) {
                    existing.progress = info.progress;
                    existing.status = info.status;
                    existing.error = info.error || '';
                }
                if (info.status !== 'done' && info.status !== 'error') {
                    allDone = false;
                }
            }

            // Optimization: update ONLY the progress text/bar, do NOT re-render the entire list
            _updateUploadFileProgress();

            if (allDone) {
                evtSource.close();
                state.uploadEventSource = null;
                // Do a full re-render once done (to show checkmarks + delete buttons)
                _renderUploadFileList();
                showToast('All files processed!', 'success');
            }
        } catch (e) {
            console.error('Progress parse error:', e);
        }
    };

    evtSource.onerror = () => {
        evtSource.close();
        state.uploadEventSource = null;
    };
}

/**
 * Incremental progress update — only touches text + bar width.
 * Does NOT re-render the entire file list DOM (prevents flashing).
 */
function _updateUploadFileProgress() {
    const list = document.getElementById('upload-file-list');
    if (!list) return;

    state.uploadFiles.forEach(f => {
        const row = list.querySelector(`[data-filename="${CSS.escape(f.name)}"]`);
        if (!row) return;

        const pctEl = row.querySelector('.s-upload-progress-pct');
        const fillEl = row.querySelector('.s-upload-progress-fill');

        if (f.status === 'done') {
            const statusDiv = row.querySelector('.s-upload-file-status');
            if (statusDiv) {
                statusDiv.innerHTML = `<span class="s-upload-check">
                    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M5 13l4 4L19 7"/>
                    </svg>
                </span>
                <button class="s-upload-delete-btn" onclick="deleteUploadDoc('${f.name.replace(/'/g, "\\'")}')"
                        title="Delete">
                    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                              d="M6 18L18 6M6 6l12 12"/>
                    </svg>
                </button>`;
            }
        } else if (f.status === 'error') {
            const statusDiv = row.querySelector('.s-upload-file-status');
            if (statusDiv) {
                statusDiv.innerHTML = `<span class="s-upload-error" title="${f.error || 'Error'}">Error</span>
                <button class="s-upload-delete-btn" onclick="deleteUploadDoc('${f.name.replace(/'/g, "\\'")}')"
                        title="Delete">
                    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                              d="M6 18L18 6M6 6l12 12"/>
                    </svg>
                </button>`;
            }
        } else {
            if (pctEl) pctEl.textContent = f.progress + '%';
            if (fillEl) fillEl.style.width = f.progress + '%';
        }
    });
}

function _renderUploadFileList() {
    const list = document.getElementById('upload-file-list');
    if (!list) return;

    if (!state.uploadFiles.length) {
        list.innerHTML = '';
        // Show dropzone, hide toggle when no files
        const wrapper = document.getElementById('upload-dropzone-wrapper');
        const toggle = document.getElementById('upload-dropzone-toggle');
        if (wrapper) wrapper.classList.remove('collapsed');
        if (toggle) toggle.classList.add('hidden');
        return;
    }

    // Auto-collapse dropzone and show toggle when files exist
    const wrapper = document.getElementById('upload-dropzone-wrapper');
    const toggle = document.getElementById('upload-dropzone-toggle');
    if (wrapper && !wrapper.classList.contains('collapsed')) {
        wrapper.classList.add('collapsed');
    }
    if (toggle) toggle.classList.remove('hidden');

    // ── Re-validate Send button: enabled only when >= 1 file done ──
    const hasReadyFile = state.uploadFiles.some(f => f.status === 'done');
    const uploadSendBtn = document.getElementById('btn-upload-chat-send');
    if (uploadSendBtn) uploadSendBtn.disabled = !hasReadyFile;

    list.innerHTML = state.uploadFiles.map(f => {
        let statusHtml;
        if (f.status === 'done') {
            statusHtml = `<span class="s-upload-check">
                <svg fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M5 13l4 4L19 7"/>
                </svg>
            </span>`;
        } else if (f.status === 'error') {
            statusHtml = `<span class="s-upload-error" title="${f.error || 'Error'}">Error</span>`;
        } else {
            statusHtml = `
                <span class="s-upload-progress-pct">${f.progress}%</span>
                <div class="s-upload-progress-bar">
                    <div class="s-upload-progress-fill" style="width:${f.progress}%"></div>
                </div>`;
        }

        // Delete button (X) — only show when file is done or error
        const deleteBtn = (f.status === 'done' || f.status === 'error')
            ? `<button class="s-upload-delete-btn" onclick="deleteUploadDoc('${f.name.replace(/'/g, "\\'")}')" title="Delete">
                <svg fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/>
                </svg>
              </button>`
            : '';

        return `<div class="s-upload-file-item" data-filename="${f.name}">
            <span class="s-upload-file-name" title="${f.name}">${f.name}</span>
            <div class="s-upload-file-status">${statusHtml}${deleteBtn}</div>
        </div>`;
    }).join('');
}

/**
 * Delete a single uploaded document and refresh the file list.
 */
async function deleteUploadDoc(filename) {
    try {
        const resp = await fetch(`/api/delete-doc?filename=${encodeURIComponent(filename)}`, {
            method: 'DELETE'
        });
        if (!resp.ok) {
            const data = await resp.json();
            showToast(data.error || 'Delete failed', 'error');
            return;
        }
        // Remove from state
        state.uploadFiles = state.uploadFiles.filter(f => f.name !== filename);
        _renderUploadFileList();
        showToast(`Deleted "${filename}"`, 'info');
    } catch (e) {
        console.error('Delete error:', e);
        showToast('Delete failed: ' + e.message, 'error');
    }
}

async function resetUploadDocs() {
    const btn = document.getElementById('btn-upload-reset');
    const btnText = document.getElementById('btn-upload-reset-text');

    // Show "Resetting..." and disable button
    if (btn) btn.disabled = true;
    if (btnText) btnText.textContent = 'Resetting...';

    // Close SSE connection explicitly
    if (state.uploadEventSource) {
        state.uploadEventSource.close();
        state.uploadEventSource = null;
    }

    // Abort any ongoing chat
    if (state.uploadAbortController) {
        state.uploadAbortController.abort();
        state.uploadAbortController = null;
    }

    try {
        await fetch('/api/upload-docs', { method: 'DELETE' });
    } catch (e) {
        console.error('Reset error:', e);
    }

    // Clear state
    state.uploadFiles = [];
    state.uploadChatHistory = [];
    state.uploadTaskId = null;

    // Disable Send + PDF buttons
    const uploadSendBtn = document.getElementById('btn-upload-chat-send');
    if (uploadSendBtn) uploadSendBtn.disabled = true;
    const uploadPdfBtn = document.getElementById('btn-upload-pdf');
    if (uploadPdfBtn) { uploadPdfBtn.disabled = true; uploadPdfBtn.style.opacity = '0.5'; }

    // Clear UI
    _renderUploadFileList();

    // Reset chat
    const chatMessages = document.getElementById('upload-chat-messages');
    if (chatMessages) {
        chatMessages.innerHTML = `
            <div class="s-chat-welcome">
                <div class="s-chat-welcome-icon">
                    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5"
                            d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z" />
                    </svg>
                </div>
                <h2 class="s-chat-welcome-title">Document Assistant</h2>
                <p class="s-chat-welcome-desc">Upload PDF or Word documents, then ask questions about<br> their content.</p>
            </div>`;
    }

    const chatInput = document.getElementById('upload-chat-input');
    if (chatInput) {
        chatInput.value = '';
        chatInput.style.height = 'auto';
    }

    // Restore button
    if (btn) btn.disabled = false;
    if (btnText) btnText.textContent = 'Reset';

    showToast('Upload collection reset', 'info');
}

// ─── Upload Chat (mirrors sendChat but uses mode=upload) ────────

async function sendUploadChat() {
    const input = $('upload-chat-input');
    const text = input.value.trim();
    if (!text) return;

    // ── Push to upload command history ──
    state.uploadCmdHistory.push(text);
    state.uploadHistoryIndex = state.uploadCmdHistory.length;

    const messages = $('upload-chat-messages');
    const sendBtn = $('btn-upload-chat-send');

    // Remove welcome screen if present
    const welcome = messages.querySelector('.s-chat-welcome');
    if (welcome) welcome.remove();

    // Add user bubble
    const userBubble = document.createElement('div');
    userBubble.className = 's-chat-bubble user';
    userBubble.textContent = text;
    messages.appendChild(userBubble);

    // Clear input
    input.value = '';
    input.style.height = 'auto';
    messages.scrollTop = messages.scrollHeight;

    // Show typing indicator
    const typing = document.createElement('div');
    typing.className = 's-chat-typing';
    typing.innerHTML = '<span></span><span></span><span></span>';
    messages.appendChild(typing);
    messages.scrollTop = messages.scrollHeight;

    // Disable inputs
    input.disabled = true;
    sendBtn.disabled = true;

    // Prepare assistant bubble
    const assistantBubble = document.createElement('div');
    assistantBubble.className = 's-chat-bubble assistant';
    const replyDiv = document.createElement('div');
    replyDiv.className = 's-chat-reply-text';
    assistantBubble.appendChild(replyDiv);

    // SSE state
    let meta = { sources: [], route_used: '', model_used: '' };
    let firstTokenReceived = false;
    let statusEl = null;
    let rawReply = '';

    if (typeof marked !== 'undefined') {
        marked.setOptions({ gfm: true, breaks: true });
    }

    const abortController = new AbortController();
    state.uploadAbortController = abortController;

    // Switch to stop mode
    const sendIcon = $('upload-send-icon');
    const stopIcon = $('upload-stop-icon');
    sendBtn.classList.add('stop-mode');
    if (sendIcon) sendIcon.classList.add('hidden');
    if (stopIcon) stopIcon.classList.remove('hidden');
    sendBtn.disabled = false;
    sendBtn.onclick = () => {
        abortController.abort();
        showToast('Generation stopped', 'info');
    };

    try {
        const res = await fetch('/api/chat', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                message: text,
                model: state.selectedModel,
                mode: 'upload',
                chat_history: (state.uploadChatHistory || []).slice(-6)
            }),
            signal: abortController.signal
        });

        if (!res.ok) throw new Error(`HTTP ${res.status}`);

        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';

        while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            if (abortController.signal.aborted) {
                try { await reader.cancel(); } catch (_) { }
                try { reader.releaseLock(); } catch (_) { }
                if (!firstTokenReceived) messages.appendChild(assistantBubble);
                const stoppedMsg = document.createElement('div');
                stoppedMsg.className = 's-chat-stopped';
                stoppedMsg.textContent = '[System] Stopped by user request.';
                assistantBubble.appendChild(stoppedMsg);
                messages.scrollTop = messages.scrollHeight;
                break;
            }

            buffer += decoder.decode(value, { stream: true });
            buffer = buffer.replace(/\r\n/g, '\n').replace(/\r/g, '\n');

            let boundary;
            while ((boundary = buffer.indexOf('\n\n')) !== -1) {
                const frame = buffer.slice(0, boundary);
                buffer = buffer.slice(boundary + 2);

                let eventType = 'message';
                let dataLines = [];
                for (const line of frame.split('\n')) {
                    if (line.startsWith('event: ')) eventType = line.slice(7).trim();
                    else if (line.startsWith('data: ')) dataLines.push(line.slice(6));
                    else if (line.startsWith('data:')) dataLines.push(line.slice(5));
                }
                const dataStr = dataLines.join('\n');

                if (eventType === 'status') {
                    if (typing.parentNode) typing.remove();
                    if (!statusEl) {
                        statusEl = document.createElement('div');
                        statusEl.className = 's-chat-typing';
                        statusEl.style.cssText = 'font-size:0.8rem;color:var(--s-text-muted);padding:8px 14px;';
                        messages.appendChild(statusEl);
                    }
                    statusEl.textContent = dataStr;
                    messages.scrollTop = messages.scrollHeight;

                } else if (eventType === 'meta') {
                    try { meta = JSON.parse(dataStr); } catch (_) { }

                } else if (eventType === 'token') {
                    if (!firstTokenReceived) {
                        firstTokenReceived = true;
                        if (typing.parentNode) typing.remove();
                        if (statusEl && statusEl.parentNode) statusEl.remove();
                        messages.appendChild(assistantBubble);
                    }
                    rawReply += dataStr;
                    if (typeof marked !== 'undefined') {
                        replyDiv.innerHTML = marked.parse(rawReply);
                    } else {
                        replyDiv.textContent = rawReply;
                    }
                    messages.scrollTop = messages.scrollHeight;

                } else if (eventType === 'done') {
                    try {
                        const doneData = JSON.parse(dataStr);
                        if (doneData.context) {
                            state.uploadChatHistory.push(...doneData.context);
                        }
                    } catch (_) { }

                } else if (eventType === 'error') {
                    if (typing.parentNode) typing.remove();
                    if (statusEl && statusEl.parentNode) statusEl.remove();
                    if (!firstTokenReceived) {
                        firstTokenReceived = true;
                        messages.appendChild(assistantBubble);
                    }
                    replyDiv.innerHTML = `<span style="color:var(--s-red)">⚠️ ${dataStr}</span>`;
                }
            }
        }

        // Append sources if available
        if (meta.sources && meta.sources.length > 0 && assistantBubble.parentNode) {
            const sourcesDiv = document.createElement('div');
            sourcesDiv.className = 's-chat-sources';
            sourcesDiv.innerHTML = '<strong>Sources:</strong> ' +
                meta.sources.map(s => `<span class="s-chat-source-tag">${s}</span>`).join(' ');
            assistantBubble.appendChild(sourcesDiv);
        }

    } catch (e) {
        if (e.name === 'AbortError') {
            // Show stopped message in the current assistant bubble
            if (!firstTokenReceived) {
                messages.appendChild(assistantBubble);
            }
            const stoppedMsg = document.createElement('div');
            stoppedMsg.className = 's-chat-stopped';
            stoppedMsg.textContent = '[System] Stopped by user request.';
            assistantBubble.appendChild(stoppedMsg);
            messages.scrollTop = messages.scrollHeight;
        } else {
            if (typing.parentNode) typing.remove();
            if (statusEl && statusEl.parentNode) statusEl.remove();
            showToast(`Chat error: ${e.message}`, 'error');
        }
    } finally {
        // Restore send mode
        input.disabled = false;
        sendBtn.disabled = false;
        sendBtn.classList.remove('stop-mode');
        const si = $('upload-send-icon');
        const sti = $('upload-stop-icon');
        if (si) si.classList.remove('hidden');
        if (sti) sti.classList.add('hidden');
        sendBtn.onclick = () => sendUploadChat();
        state.uploadAbortController = null;

        // ── Enable PDF button after first exchange ──
        if (firstTokenReceived) {
            const pdfBtn = document.getElementById('btn-upload-pdf');
            if (pdfBtn && typeof html2pdf !== 'undefined') {
                pdfBtn.disabled = false;
                pdfBtn.style.opacity = '1';
            }
        }

        // ── Mermaid rendering (post-stream) — mirror main Chat logic ──
        const mermaidCodes = messages.querySelectorAll('pre:not([data-mermaid-processed]) code.language-mermaid');
        if (mermaidCodes.length > 0) {
            const newMermaidDivs = [];
            mermaidCodes.forEach(codeEl => {
                const pre = codeEl.parentElement;
                pre.setAttribute('data-mermaid-processed', 'true');
                let raw = codeEl.textContent;
                const tmp = document.createElement('textarea');
                tmp.innerHTML = raw;
                raw = tmp.value;
                const mermaidDiv = document.createElement('div');
                mermaidDiv.className = 'mermaid';
                mermaidDiv.textContent = raw;
                pre.style.display = 'none';
                pre.insertAdjacentElement('afterend', mermaidDiv);
                newMermaidDivs.push(mermaidDiv);
            });
            if (typeof mermaid !== 'undefined') {
                try {
                    await mermaid.run({ nodes: newMermaidDivs });
                } catch (mErr) {
                    console.warn('[Upload Mermaid] render error:', mErr);
                }
            }
        }
    }
}

// ─── Upload Chat PDF Download ───────────────────────────────
async function downloadUploadChatPDF() {
    const chatMessages = $('upload-chat-messages');
    if (!chatMessages) return;

    showToast('Generating PDF...', 'info');

    // Save original layout state
    const origHeight = chatMessages.style.height;
    const origMaxHeight = chatMessages.style.maxHeight;
    const origOverflow = chatMessages.style.overflow;
    const origBg = chatMessages.style.background;
    const origScrollTop = chatMessages.scrollTop;

    // Unroll: make entire chat visible for capture
    chatMessages.style.height = 'auto';
    chatMessages.style.maxHeight = 'none';
    chatMessages.style.overflow = 'visible';
    chatMessages.style.background = '#FFFFFF';

    // Rasterise Mermaid SVGs → PNG <img> for html2canvas
    const svgSwaps = [];
    const mermaidSVGs = chatMessages.querySelectorAll('.mermaid svg');
    for (const svg of mermaidSVGs) {
        const pngImg = await _svgToImg(svg);
        if (pngImg) {
            svg.style.display = 'none';
            svg.parentElement.appendChild(pngImg);
            svgSwaps.push({ svg, pngImg });
        }
    }

    const restoreAll = () => {
        svgSwaps.forEach(({ svg, pngImg }) => {
            svg.style.display = '';
            if (pngImg.parentElement) pngImg.remove();
        });
        chatMessages.style.height = origHeight;
        chatMessages.style.maxHeight = origMaxHeight;
        chatMessages.style.overflow = origOverflow;
        chatMessages.style.background = origBg;
        chatMessages.scrollTop = origScrollTop;
    };

    const opt = {
        margin: [10, 10, 10, 10],
        filename: `Upload_Chat_${new Date().toISOString().slice(0, 10)}.pdf`,
        image: { type: 'png' },
        html2canvas: {
            scale: 3, useCORS: true, logging: false,
            scrollX: 0, scrollY: 0, backgroundColor: '#FFFFFF'
        },
        jsPDF: { unit: 'mm', format: 'a4', orientation: 'portrait' }
    };

    setTimeout(() => {
        html2pdf().set(opt).from(chatMessages).save().then(() => {
            restoreAll();
            showToast('PDF downloaded', 'success');
        }).catch(err => {
            restoreAll();
            showToast(`PDF error: ${err.message}`, 'error');
        });
    }, 100);
}

// ─── Global Navigation (GNB) ─────────────────────────────────
function initGlobalNav() {
    // Listen for hash changes
    window.addEventListener('hashchange', () => {
        const view = getViewFromHash();
        switchView(view);
    });

    // Initial route
    const initialView = getViewFromHash();
    switchView(initialView);
}

function getViewFromHash() {
    const hash = window.location.hash || '#/';
    const map = {
        '#/': 'dashboard',
        '#/nbi': 'nbi',
        '#/rfp': 'rfp',
        '#/dimensioning': 'dimensioning',
        '#/statistics': 'statistics'
    };
    return map[hash] || 'dashboard';
}

function switchView(viewName) {
    state.activeView = viewName;

    // Hide all views
    const views = ['dashboard', 'nbi', 'rfp', 'dimensioning', 'statistics'];
    views.forEach(v => {
        const el = $(`view-${v}`);
        if (el) {
            el.classList.toggle('hidden', v !== viewName);
        }
    });

    // Update GNB active state
    $$('.s-gnb-item').forEach(item => {
        item.classList.toggle('active', item.dataset.view === viewName);
    });

    // Restore last active sub-tab (or default to first)
    if (viewName !== 'dashboard') {
        const lastSubtab = _moduleTabState[viewName];
        if (lastSubtab) {
            switchModuleTab(viewName, lastSubtab, true);
        }
    }

    // NBI-specific: hide header actions unless we're in NBI with nbi-tab active
    const nbiActions = $('nbi-header-actions');
    if (nbiActions) {
        const showNbiButtons = (viewName === 'nbi' && _moduleTabState.nbi === 'nbi-tab');
        nbiActions.classList.toggle('hidden', !showNbiButtons);
    }

    // Close mobile menu after navigation
    const gnbInner = document.querySelector('.s-gnb-inner');
    if (gnbInner) gnbInner.classList.remove('gnb-mobile-open');
}

function navigateTo(viewName) {
    const map = {
        'dashboard': '#/',
        'nbi': '#/nbi',
        'rfp': '#/rfp',
        'dimensioning': '#/dimensioning',
        'statistics': '#/statistics'
    };
    window.location.hash = map[viewName] || '#/';
}

function toggleGnbMobile() {
    const inner = document.querySelector('.s-gnb-inner');
    if (inner) inner.classList.toggle('gnb-mobile-open');
}

// ─── Module Sub-Tab Switching ─────────────────────────────────

// Default sub-tabs per module + last-visited memory
const _moduleTabDefaults = {
    nbi: 'unified-tab',
    rfp: 'draft-tab',
    dimensioning: 'autodim-tab',
    statistics: 'overview-tab'
};
const _moduleTabState = { ...(_moduleTabDefaults) };

function switchModuleTab(moduleName, subtabId, isRestore) {
    // Update remembered state
    _moduleTabState[moduleName] = subtabId;

    // Toggle tab bar active state
    const tabBar = document.querySelector(`.s-module-tabs[data-module="${moduleName}"]`);
    if (tabBar) {
        tabBar.querySelectorAll('.s-module-tab').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.subtab === subtabId);
        });
    }

    // Toggle panels
    document.querySelectorAll(`.s-module-panel[data-module="${moduleName}"]`).forEach(panel => {
        const match = panel.id === `${moduleName}-panel-${subtabId}`;
        panel.classList.toggle('hidden', !match);
    });

    // NBI-specific: only show header actions when NBI sub-tab is active
    const nbiActions = $('nbi-header-actions');
    if (nbiActions) {
        const showNbiButtons = (state.activeView === 'nbi' && _moduleTabState.nbi === 'nbi-tab');
        nbiActions.classList.toggle('hidden', !showNbiButtons);
    }
}

// Navigate to a module AND activate a specific sub-tab (for dashboard chip clicks)
function navigateToSubTab(moduleName, subtabId) {
    _moduleTabState[moduleName] = subtabId;
    navigateTo(moduleName);
}

// ═══════════════════════════════════════════════════════════
// RFP MANAGEMENT MODULE
// ═══════════════════════════════════════════════════════════

// ─── Mock Data ────────────────────────────────────────────
const RFP_MOCK_DATA = [
    {
        id: 'rfp-001',
        operator: 'Verizon',
        year: '2026',
        title: 'Verizon 5G Core Network Expansion RFP 2026',
        requirements: [
            { id: 'REQ-001', requirement: 'Support for 5G SA Core with 3GPP R16 compliance', answer: 'Compliant', owner: 'J. Kim', jiraId: 'VRZN-1001', resultType: '', metadata: {} },
            { id: 'REQ-002', requirement: 'Network slicing with min 100 slice instances per node', answer: 'Partial', owner: 'S. Park', jiraId: 'VRZN-1002', resultType: '', metadata: {} },
            { id: 'REQ-003', requirement: 'E2E latency ≤ 10ms for URLLC slice category', answer: 'Under Review', owner: 'M. Lee', jiraId: 'VRZN-1003', resultType: '', metadata: {} },
            { id: 'REQ-004', requirement: 'Northbound RESTCONF/YANG interface for all CM operations', answer: 'Compliant', owner: 'J. Kim', jiraId: 'VRZN-1004', resultType: '', metadata: {} },
            { id: 'REQ-005', requirement: 'Auto-scaling based on real-time traffic load with ≤ 30s reaction time', answer: 'Partial', owner: 'H. Choi', jiraId: 'VRZN-1005', resultType: '', metadata: {} },
            { id: 'REQ-006', requirement: 'Support for IPv6 single-stack and IPv4/v6 dual-stack operation', answer: 'Compliant', owner: 'S. Park', jiraId: 'VRZN-1006', resultType: '', metadata: {} },
            { id: 'REQ-007', requirement: 'Multi-vendor interoperability testing certification (O-RAN Alliance)', answer: 'Under Review', owner: 'M. Lee', jiraId: 'VRZN-1007', resultType: '', metadata: {} },
            { id: 'REQ-008', requirement: 'Real-time PM/FM data export via Kafka streaming interface', answer: 'Compliant', owner: 'J. Kim', jiraId: 'VRZN-1008', resultType: '', metadata: {} },
        ]
    },
    {
        id: 'rfp-002',
        operator: 'Verizon',
        year: '2025',
        title: 'Verizon Edge Computing Platform Deployment',
        requirements: [
            { id: 'REQ-010', requirement: 'MEC platform integration with ETSI MEC 003 API framework', answer: 'Compliant', owner: 'D. Hong', jiraId: 'VRZN-2010', resultType: '', metadata: {} },
            { id: 'REQ-011', requirement: 'Container orchestration with Kubernetes 1.28+ support', answer: 'Compliant', owner: 'S. Park', jiraId: 'VRZN-2011', resultType: '', metadata: {} },
            { id: 'REQ-012', requirement: 'GPU workload scheduling for AI/ML edge inference', answer: 'Partial', owner: 'H. Choi', jiraId: 'VRZN-2012', resultType: '', metadata: {} },
        ]
    },
    {
        id: 'rfp-003',
        operator: 'AT&T',
        year: '2026',
        title: 'AT&T Unified Management System Modernization Program',
        requirements: [
            { id: 'REQ-020', requirement: 'Single-pane-of-glass management for 4G/5G converged network', answer: 'Compliant', owner: 'J. Kim', jiraId: 'ATT-3001', resultType: '', metadata: {} },
            { id: 'REQ-021', requirement: 'Role-based access control with LDAP/SAML integration', answer: 'Compliant', owner: 'M. Lee', jiraId: 'ATT-3002', resultType: '', metadata: {} },
            { id: 'REQ-022', requirement: 'Fault correlation engine with ≥ 90% root cause accuracy', answer: 'Under Review', owner: 'H. Choi', jiraId: 'ATT-3003', resultType: '', metadata: {} },
            { id: 'REQ-023', requirement: 'Closed-loop automation for self-healing network scenarios', answer: 'Partial', owner: 'S. Park', jiraId: 'ATT-3004', resultType: '', metadata: {} },
            { id: 'REQ-024', requirement: 'Compliance with SOC 2 Type II and FedRAMP Moderate baseline', answer: 'Under Review', owner: 'D. Hong', jiraId: 'ATT-3005', resultType: '', metadata: {} },
        ]
    },
    {
        id: 'rfp-004',
        operator: 'AT&T',
        year: '2025',
        title: 'AT&T Network Analytics and Assurance Platform RFP',
        requirements: [
            { id: 'REQ-030', requirement: 'Real-time network KPI dashboard with sub-second refresh', answer: 'Compliant', owner: 'J. Kim', jiraId: 'ATT-4001', resultType: '', metadata: {} },
            { id: 'REQ-031', requirement: 'Predictive analytics for capacity planning with ML models', answer: 'Under Review', owner: 'H. Choi', jiraId: 'ATT-4002', resultType: '', metadata: {} },
        ]
    },
    {
        id: 'rfp-005',
        operator: 'T-Mobile',
        year: '2026',
        title: 'T-Mobile Open RAN Management and Orchestration Requirements',
        requirements: [
            { id: 'REQ-040', requirement: 'O-RAN SMO compliant with O-RAN WG1/WG2 specifications', answer: 'Compliant', owner: 'S. Park', jiraId: 'TMO-5001', resultType: '', metadata: {} },
            { id: 'REQ-041', requirement: 'Support for near-RT RIC with xApp lifecycle management', answer: 'Partial', owner: 'M. Lee', jiraId: 'TMO-5002', resultType: '', metadata: {} },
            { id: 'REQ-042', requirement: 'Automated RAN feature activation and rollback capability', answer: 'Under Review', owner: 'H. Choi', jiraId: 'TMO-5003', resultType: '', metadata: {} },
            { id: 'REQ-043', requirement: 'Multi-vendor RU/DU/CU management with unified topology view', answer: 'Compliant', owner: 'J. Kim', jiraId: 'TMO-5004', resultType: '', metadata: {} },
        ]
    }
];

// ─── RFP State ────────────────────────────────────────────
const rfpState = {
    selectedRfpId: null,
    selectedReqId: null,
    requirements: [],
    searchTerm: '',
    treeSearchTerm: '',
    history: {}  // keyed by req id
};

// ─── LocalStorage Persistence ─────────────────────────────
const RFP_STORAGE_KEY = 'usm_rfp_data';

function rfpLoadFromStorage() {
    try {
        const raw = localStorage.getItem(RFP_STORAGE_KEY);
        if (!raw) return;
        const saved = JSON.parse(raw);
        // Merge saved requirement metadata back into mock data
        if (saved.requirements) {
            for (const rfp of RFP_MOCK_DATA) {
                for (const req of rfp.requirements) {
                    if (saved.requirements[req.id]) {
                        Object.assign(req, saved.requirements[req.id]);
                    }
                }
            }
        }
        if (saved.history) {
            rfpState.history = saved.history;
        }
    } catch (e) {
        console.warn('[RFP] Failed to load from localStorage:', e);
    }
}

function rfpSaveToStorage() {
    try {
        const reqMap = {};
        for (const rfp of RFP_MOCK_DATA) {
            for (const req of rfp.requirements) {
                if (req.resultType) {
                    reqMap[req.id] = {
                        resultType: req.resultType,
                        metadata: req.metadata
                    };
                }
            }
        }
        localStorage.setItem(RFP_STORAGE_KEY, JSON.stringify({
            requirements: reqMap,
            history: rfpState.history
        }));
    } catch (e) {
        console.warn('[RFP] Failed to save to localStorage:', e);
    }
}

// ─── Init ─────────────────────────────────────────────────
function initRfp() {
    rfpLoadFromStorage();
    renderRfpTree();

    // Tree search
    const treeSearch = $('rfp-tree-search');
    if (treeSearch) {
        treeSearch.addEventListener('input', () => {
            rfpState.treeSearchTerm = treeSearch.value.trim().toLowerCase();
            renderRfpTree();
        });
    }

    // Requirement search
    const reqSearch = $('rfp-req-search');
    if (reqSearch) {
        reqSearch.addEventListener('input', () => {
            rfpState.searchTerm = reqSearch.value.trim().toLowerCase();
            renderRfpGrid();
        });
    }

    // Result Type change handler
    const resultType = $('rfp-result-type');
    if (resultType) {
        resultType.addEventListener('change', rfpOnResultTypeChange);
    }

    // PC Type change handler
    const pcType = $('rfp-pc-type');
    if (pcType) {
        pcType.addEventListener('change', rfpOnPcTypeChange);
    }

    // Keyboard navigation on the grid body
    const gridBody = $('rfp-grid-body');
    if (gridBody) {
        gridBody.setAttribute('tabindex', '0');
        gridBody.addEventListener('keydown', rfpGridKeyHandler);
    }
}

// ─── Tree Rendering ───────────────────────────────────────
function renderRfpTree() {
    const container = $('rfp-tree-container');
    if (!container) return;

    // Group by operator → year
    const grouped = {};
    for (const rfp of RFP_MOCK_DATA) {
        // Tree search filter
        if (rfpState.treeSearchTerm) {
            const match = rfp.title.toLowerCase().includes(rfpState.treeSearchTerm) ||
                rfp.operator.toLowerCase().includes(rfpState.treeSearchTerm);
            if (!match) continue;
        }
        if (!grouped[rfp.operator]) grouped[rfp.operator] = {};
        if (!grouped[rfp.operator][rfp.year]) grouped[rfp.operator][rfp.year] = [];
        grouped[rfp.operator][rfp.year].push(rfp);
    }

    if (Object.keys(grouped).length === 0) {
        container.innerHTML = '<div class="rfp-tree-empty">No RFPs found</div>';
        return;
    }

    const chevronSvg = '<svg fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/></svg>';

    let html = '';
    for (const [operator, years] of Object.entries(grouped)) {
        html += `<div class="rfp-tree-operator">`;
        html += `<div class="rfp-tree-operator-label" onclick="this.classList.toggle('collapsed'); this.nextElementSibling.classList.toggle('collapsed')">${chevronSvg} ${operator}</div>`;
        html += `<div class="rfp-tree-children">`;

        // Sort years descending
        const sortedYears = Object.keys(years).sort((a, b) => b - a);
        for (const year of sortedYears) {
            html += `<div class="rfp-tree-year">`;
            html += `<div class="rfp-tree-year-label" onclick="this.classList.toggle('collapsed'); this.nextElementSibling.classList.toggle('collapsed')">${chevronSvg} ${year}</div>`;
            html += `<div class="rfp-tree-children">`;

            for (const rfp of years[year]) {
                const isActive = rfpState.selectedRfpId === rfp.id ? ' active' : '';
                const escapedTitle = rfp.title.replace(/'/g, "\\'").replace(/"/g, '&quot;');
                html += `<div class="rfp-tree-item${isActive}" data-tooltip="${rfp.title.replace(/"/g, '&quot;')}" onclick="rfpSelectRfp('${rfp.id}')">${rfp.title}</div>`;
            }

            html += `</div></div>`; // close year children + year
        }

        html += `</div></div>`; // close operator children + operator
    }

    container.innerHTML = html;
}

// ─── RFP Selection ────────────────────────────────────────
function rfpSelectRfp(rfpId) {
    rfpState.selectedRfpId = rfpId;
    // Reset requirement selection & detail panel
    rfpState.selectedReqId = null;
    rfpShowDetailEmpty();

    const rfp = RFP_MOCK_DATA.find(r => r.id === rfpId);
    if (!rfp) return;

    rfpState.requirements = rfp.requirements;

    // Update grid title
    const titleEl = $('rfp-grid-title');
    if (titleEl) titleEl.textContent = rfp.title;

    // Re-render tree to update active state
    renderRfpTree();
    renderRfpGrid();
}

// ─── Grid Rendering ───────────────────────────────────────
function renderRfpGrid() {
    const table = $('rfp-table');
    const empty = $('rfp-grid-empty');
    const tbody = $('rfp-table-body');
    const countEl = $('rfp-grid-count');

    if (!rfpState.requirements.length) {
        if (table) table.classList.add('hidden');
        if (empty) empty.classList.remove('hidden');
        if (countEl) countEl.textContent = '';
        return;
    }

    if (table) table.classList.remove('hidden');
    if (empty) empty.classList.add('hidden');

    // Filter by search term
    let filtered = rfpState.requirements;
    if (rfpState.searchTerm) {
        filtered = rfpState.requirements.filter(r =>
            r.requirement.toLowerCase().includes(rfpState.searchTerm) ||
            r.jiraId.toLowerCase().includes(rfpState.searchTerm) ||
            r.owner.toLowerCase().includes(rfpState.searchTerm)
        );
    }

    if (countEl) countEl.textContent = `${filtered.length} items`;

    if (!tbody) return;
    tbody.innerHTML = filtered.map((req, i) => {
        // Use ID-based matching for selection
        const isSelected = rfpState.selectedReqId === req.id ? ' rfp-row-selected' : '';
        const statusClass = req.resultType === 'PC' ? 'status-pc' : req.resultType === 'NC' ? 'status-nc' : 'status-none';
        const statusLabel = req.resultType || '—';

        return `<tr class="${isSelected}" data-req-id="${req.id}" onclick="rfpSelectReq('${req.id}')">
            <td class="rfp-td-num">${i + 1}</td>
            <td>${req.requirement}</td>
            <td>${req.answer}</td>
            <td>${req.owner}</td>
            <td class="rfp-td-jira">${req.jiraId}</td>
            <td style="text-align:center;"><span class="rfp-status-badge ${statusClass}">${statusLabel}</span></td>
        </tr>`;
    }).join('');
}

// ─── Requirement Selection ────────────────────────────────
function rfpSelectReq(reqId) {
    rfpState.selectedReqId = reqId;
    renderRfpGrid(); // Re-render to update highlight
    renderRfpDetail();

    // Focus grid body for keyboard navigation
    const gridBody = $('rfp-grid-body');
    if (gridBody) gridBody.focus();
}

// ─── Detail Panel Rendering ───────────────────────────────
function rfpShowDetailEmpty() {
    const empty = $('rfp-detail-empty');
    const content = $('rfp-detail-content');
    if (empty) empty.classList.remove('hidden');
    if (content) content.classList.add('hidden');
}

function renderRfpDetail() {
    const req = rfpState.requirements.find(r => r.id === rfpState.selectedReqId);
    if (!req) {
        rfpShowDetailEmpty();
        return;
    }

    const empty = $('rfp-detail-empty');
    const content = $('rfp-detail-content');
    if (empty) empty.classList.add('hidden');
    if (content) content.classList.remove('hidden');

    // Header
    const jiraBadge = $('rfp-detail-jira');
    const titleEl = $('rfp-detail-title');
    if (jiraBadge) jiraBadge.textContent = req.jiraId;
    if (titleEl) titleEl.textContent = req.requirement;

    // Result Type
    const resultType = $('rfp-result-type');
    if (resultType) resultType.value = req.resultType || '';

    // Reset conditional fields
    rfpOnResultTypeChange();

    // Populate saved metadata
    if (req.metadata) {
        if (req.resultType === 'PC') {
            const pcType = $('rfp-pc-type');
            if (pcType) pcType.value = req.metadata.pcType || '';
            rfpOnPcTypeChange();
            if (req.metadata.pcType === 'Planned Development') {
                const schedule = $('rfp-planned-schedule');
                if (schedule) schedule.value = req.metadata.plannedSchedule || '';
            } else if (req.metadata.pcType === 'Partial Support') {
                const supported = $('rfp-supported-scope');
                const unsupported = $('rfp-unsupported-scope');
                const reason = $('rfp-partial-reason');
                if (supported) supported.value = req.metadata.supportedScope || '';
                if (unsupported) unsupported.value = req.metadata.unsupportedScope || '';
                if (reason) reason.value = req.metadata.partialReason || '';
            }
        } else if (req.resultType === 'NC') {
            const ncReason = $('rfp-nc-reason');
            if (ncReason) ncReason.value = req.metadata.ncReason || '';
        }
    }

    // Comment
    const comment = $('rfp-comment');
    if (comment) comment.value = req.metadata?.comment || '';

    // Render history
    renderRfpTimeline(req.id);
}

// ─── Conditional Form Logic ───────────────────────────────
function rfpOnResultTypeChange() {
    const val = ($('rfp-result-type') || {}).value || '';
    const pcFields = $('rfp-pc-fields');
    const ncFields = $('rfp-nc-fields');

    if (pcFields) pcFields.classList.toggle('hidden', val !== 'PC');
    if (ncFields) ncFields.classList.toggle('hidden', val !== 'NC');

    // Reset sub-fields when type changes
    if (val !== 'PC') {
        const pcType = $('rfp-pc-type');
        if (pcType) pcType.value = '';
        rfpOnPcTypeChange();
    }
    if (val !== 'NC') {
        const ncReason = $('rfp-nc-reason');
        if (ncReason) ncReason.value = '';
    }
}

function rfpOnPcTypeChange() {
    const val = ($('rfp-pc-type') || {}).value || '';
    const planned = $('rfp-planned-dev');
    const partial = $('rfp-partial-support');

    if (planned) planned.classList.toggle('hidden', val !== 'Planned Development');
    if (partial) partial.classList.toggle('hidden', val !== 'Partial Support');
}

// ─── Save Metadata ────────────────────────────────────────
function rfpSaveMetadata() {
    const req = rfpState.requirements.find(r => r.id === rfpState.selectedReqId);
    if (!req) return;

    const resultType = ($('rfp-result-type') || {}).value || '';
    const comment = ($('rfp-comment') || {}).value || '';
    const oldType = req.resultType;

    req.resultType = resultType;
    req.metadata = req.metadata || {};
    req.metadata.comment = comment;

    if (resultType === 'PC') {
        const pcType = ($('rfp-pc-type') || {}).value || '';
        req.metadata.pcType = pcType;
        if (pcType === 'Planned Development') {
            req.metadata.plannedSchedule = ($('rfp-planned-schedule') || {}).value || '';
        } else if (pcType === 'Partial Support') {
            req.metadata.supportedScope = ($('rfp-supported-scope') || {}).value || '';
            req.metadata.unsupportedScope = ($('rfp-unsupported-scope') || {}).value || '';
            req.metadata.partialReason = ($('rfp-partial-reason') || {}).value || '';
        }
    } else if (resultType === 'NC') {
        req.metadata.ncReason = ($('rfp-nc-reason') || {}).value || '';
    }

    // Add to revision history
    const action = oldType !== resultType
        ? (resultType ? `Changed to ${resultType}` : 'Cleared result type')
        : `Updated ${resultType || 'metadata'}`;

    if (!rfpState.history[req.id]) rfpState.history[req.id] = [];
    rfpState.history[req.id].unshift({
        timestamp: new Date().toISOString(),
        author: 'Current User',
        action: action,
        comment: comment || ''
    });

    // Persist to localStorage
    rfpSaveToStorage();

    // Re-render
    renderRfpGrid();
    renderRfpTimeline(req.id);

    showToast('Metadata saved successfully', 'success');
}

// ─── Reset Form ───────────────────────────────────────────
function rfpResetForm() {
    const resultType = $('rfp-result-type');
    if (resultType) resultType.value = '';
    rfpOnResultTypeChange();

    const comment = $('rfp-comment');
    if (comment) comment.value = '';

    const schedule = $('rfp-planned-schedule');
    if (schedule) schedule.value = '';

    const supported = $('rfp-supported-scope');
    if (supported) supported.value = '';

    const unsupported = $('rfp-unsupported-scope');
    if (unsupported) unsupported.value = '';

    showToast('Form reset', 'info');
}

// ─── Revision History Timeline ────────────────────────────
function renderRfpTimeline(reqId) {
    const timeline = $('rfp-timeline');
    if (!timeline) return;

    const entries = rfpState.history[reqId] || [];

    if (!entries.length) {
        timeline.innerHTML = '<div class="rfp-timeline-empty">No history yet</div>';
        return;
    }

    timeline.innerHTML = entries.map(e => {
        const date = new Date(e.timestamp);
        const timeStr = date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' }) +
            ' ' + date.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });

        return `<div class="rfp-timeline-entry">
            <div class="rfp-timeline-meta">
                <span class="rfp-timeline-author">${e.author}</span>
                <span>·</span>
                <span>${timeStr}</span>
            </div>
            <div class="rfp-timeline-action">${e.action}</div>
            ${e.comment ? `<div class="rfp-timeline-comment">${e.comment}</div>` : ''}
        </div>`;
    }).join('');
}

// ─── Keyboard Navigation ──────────────────────────────────
function rfpGridKeyHandler(e) {
    if (e.key !== 'ArrowUp' && e.key !== 'ArrowDown') return;
    e.preventDefault(); // Prevent browser from scrolling the page

    // Get filtered list (same as rendered)
    let filtered = rfpState.requirements;
    if (rfpState.searchTerm) {
        filtered = rfpState.requirements.filter(r =>
            r.requirement.toLowerCase().includes(rfpState.searchTerm) ||
            r.jiraId.toLowerCase().includes(rfpState.searchTerm) ||
            r.owner.toLowerCase().includes(rfpState.searchTerm)
        );
    }

    if (!filtered.length) return;

    // Find current index by ID
    const currentIdx = filtered.findIndex(r => r.id === rfpState.selectedReqId);
    let newIdx;

    if (e.key === 'ArrowDown') {
        newIdx = currentIdx < filtered.length - 1 ? currentIdx + 1 : 0;
    } else {
        newIdx = currentIdx > 0 ? currentIdx - 1 : filtered.length - 1;
    }

    rfpSelectReq(filtered[newIdx].id);

    // Scroll selected row into view
    const selectedRow = document.querySelector(`tr[data-req-id="${filtered[newIdx].id}"]`);
    if (selectedRow) {
        selectedRow.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
    }
}

// ─── Jira Sync (Mock) ─────────────────────────────────────
function rfpJiraSync() {
    const btn = $('rfp-sync-btn');
    const btnText = $('rfp-sync-btn-text');
    if (!btn || btn.classList.contains('syncing')) return;

    btn.classList.add('syncing');
    btn.disabled = true;
    if (btnText) btnText.textContent = 'Syncing...';

    // Simulate API delay
    setTimeout(() => {
        btn.classList.remove('syncing');
        btn.disabled = false;
        if (btnText) btnText.textContent = 'Jira API Sync';

        // Re-render the grid with current data
        renderRfpGrid();
        showToast('Jira sync completed — all requirements up to date', 'success');
    }, 1500);
}
