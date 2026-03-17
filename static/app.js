function _apiHeaders(extra = {}) {
    const key = localStorage.getItem('aegis_api_key');
    const h = { ...extra };
    if (key) h['Authorization'] = `Bearer ${key}`;
    return h;
}

const DOMElements = {
    providerSelect: document.getElementById('provider_select'),
    modelSelect: document.getElementById('model_select'),
    chatHistory: document.getElementById('chat_history'),
    input: document.getElementById('intent_input'),
    runBtn: document.getElementById('run_btn'),
    tracePanel: document.getElementById('trace_panel'),
    errorContainer: document.getElementById('error_container'),
    errorTitle: document.getElementById('error_title'),
    errorMessage: document.getElementById('error_message'),
    resultsContainer: document.getElementById('results_container'),
    resultsHead: document.getElementById('results_head'),
    resultsBody: document.getElementById('results_body'),
    rowCount: document.getElementById('row_count'),
    executionLatency: document.getElementById('execution_latency'),
    compilerLatency: document.getElementById('compiler_latency'),
    jsonRequest: document.getElementById('json_request'),
    jsonResponse: document.getElementById('json_response'),
    jsonTabContent: document.getElementById('json_tab_content'),

    // Trace nodes
    ragOutcome: document.getElementById('rag_outcome'),
    ragMatches: document.getElementById('rag_matches'),
    ragReason: document.getElementById('rag_reason'),
    schemaIncluded: document.getElementById('schema_included'),
    schemaExcluded: document.getElementById('schema_excluded'),
    llmProvider: document.getElementById('llm_provider'),
    promptSystem: document.getElementById('prompt_system'),
    llmRawResponse: document.getElementById('llm_raw_response'),
    llmAbstract: document.getElementById('llm_abstract'),
    paramSql: document.getElementById('param_sql'),
    bindParams: document.getElementById('bind_params'),

    // Tabs
    tabResults: document.getElementById('tab_results'),
    tabTrace: document.getElementById('tab_trace'),
    tabJson: document.getElementById('tab_json'),

    // Panels (for resizing)
    inputPanel: document.getElementById('input_panel'),
    rightPanel: document.getElementById('right_panel'),
    resizeHandle: document.getElementById('resize_handle'),
};

let currentSessionId = null;
let _pendingSourceDatabase = null; // set by DB pills; consumed once per request
let _lastIntent = null;            // preserved so pills can re-run the last query

// ─── Per-turn snapshot store ──────────────────────────────────────────────────
// Each snapshot mirrors the full right-pane state for one query turn.
// Nothing is ever sent to the backend when restoring — purely visual.
const turnSnapshots = [];

function captureSnapshot(jsonReq, jsonRes, errorData, successData, explainData) {
    turnSnapshots.push({ jsonReq, jsonRes, errorData, successData, explainData });
    return turnSnapshots.length - 1;
}

function restoreSnapshot(idx) {
    const snap = turnSnapshots[idx];
    if (!snap) return;

    // JSON Tab
    DOMElements.jsonRequest.textContent = snap.jsonReq;
    DOMElements.jsonResponse.textContent = snap.jsonRes;

    // Error pane
    if (snap.errorData) {
        DOMElements.errorContainer.classList.remove('hidden');
        DOMElements.errorTitle.textContent = snap.errorData.title;
        DOMElements.errorMessage.textContent = snap.errorData.message;
    } else {
        DOMElements.errorContainer.classList.add('hidden');
    }

    // Results pane
    if (snap.successData) {
        applySuccessData(snap.successData);
    } else {
        DOMElements.resultsHead.innerHTML = '';
        DOMElements.resultsBody.innerHTML = '';
        DOMElements.rowCount.textContent = '0';
        DOMElements.executionLatency.textContent = '';
    }

    // Compilation pipeline pane
    if (snap.explainData) {
        renderExplainability(snap.explainData);
    } else {
        resetTraceUI();
    }

    // Visual flash: remove → force reflow → re-add so animation restarts
    DOMElements.rightPanel.classList.remove('panel-flash');
    void DOMElements.rightPanel.offsetWidth;
    DOMElements.rightPanel.classList.add('panel-flash');
    setTimeout(() => DOMElements.rightPanel.classList.remove('panel-flash'), 800);

    // Highlight the active turn pair in the chat history
    document.querySelectorAll('.turn-pair').forEach(el => el.classList.remove('turn-selected'));
    const active = document.querySelector(`.turn-pair[data-turn-idx="${idx}"]`);
    if (active) active.classList.add('turn-selected');
}

// ─── Tab switching ─────────────────────────────────────────────────────────────
const TAB_MAP = () => ({
    results: { btn: DOMElements.tabResults, content: DOMElements.resultsContainer },
    trace: { btn: DOMElements.tabTrace, content: DOMElements.tracePanel },
    json: { btn: DOMElements.tabJson, content: DOMElements.jsonTabContent },
});

function switchTab(tab) {
    Object.entries(TAB_MAP()).forEach(([key, { btn, content }]) => {
        btn.classList.toggle('active', key === tab);
        content.classList.toggle('active', key === tab);
        content.classList.toggle('hidden', key !== tab);
    });
}

DOMElements.tabResults.addEventListener('click', () => switchTab('results'));
DOMElements.tabTrace.addEventListener('click', () => switchTab('trace'));
DOMElements.tabJson.addEventListener('click', () => switchTab('json'));

// ─── Resizable panes ──────────────────────────────────────────────────────────
let isResizing = false;
let resizeStartX = 0;
let resizeStartLW = 0;
let resizeStartRW = 0;

DOMElements.resizeHandle.addEventListener('mousedown', e => {
    isResizing = true;
    resizeStartX = e.clientX;
    resizeStartLW = DOMElements.inputPanel.getBoundingClientRect().width;
    resizeStartRW = DOMElements.rightPanel.getBoundingClientRect().width;
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
    e.preventDefault();
});

document.addEventListener('mousemove', e => {
    if (!isResizing) return;
    const dx = e.clientX - resizeStartX;
    const total = resizeStartLW + resizeStartRW;
    const minW = 300;
    const newLeft = Math.max(minW, Math.min(resizeStartLW + dx, total - minW));
    DOMElements.inputPanel.style.flex = 'none';
    DOMElements.inputPanel.style.width = newLeft + 'px';
    DOMElements.rightPanel.style.flex = 'none';
    DOMElements.rightPanel.style.width = (total - newLeft) + 'px';
});

document.addEventListener('mouseup', () => {
    if (!isResizing) return;
    isResizing = false;
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
});

// ─── Provider / Model dropdowns ───────────────────────────────────────────────
const providerModels = {
    'openai': ['gpt-4o', 'gpt-4o-mini', 'gpt-3.5-turbo'],
    'anthropic': ['claude-3-5-sonnet-20241022', 'claude-3-5-haiku-20241022', 'claude-3-haiku-20240307'],
    'google': ['gemini-1.5-pro', 'gemini-1.5-flash', 'gemini-1.5-flash-8b'],
    'xai': ['grok-2', 'grok-2-mini', 'grok-1'],
    'ollama': ['llama3'],
};

function updateModelDropdown() {
    const provider = DOMElements.providerSelect.value;
    DOMElements.modelSelect.innerHTML = '';
    (providerModels[provider] || []).forEach(model => {
        const opt = document.createElement('option');
        opt.value = opt.textContent = model;
        DOMElements.modelSelect.appendChild(opt);
    });
}

DOMElements.providerSelect.addEventListener('change', updateModelDropdown);

// ─── Input listeners ──────────────────────────────────────────────────────────
DOMElements.input.addEventListener('keydown', e => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); runCompilation(); }
});
DOMElements.runBtn.addEventListener('click', runCompilation);

// ─── Boot ──────────────────────────────────────────────────────────────────────
async function loadActiveVersion() {
    try {
        const res = await fetch('/api/v1/metadata/active', { headers: _apiHeaders() });
        const data = await res.json();
        document.getElementById('active_version').textContent =
            (data && data.version_id) ? data.version_id : 'Development Sandbox';
    } catch (e) { console.error('Failed to load active schema version', e); }
}

document.addEventListener('DOMContentLoaded', () => {
    loadActiveVersion();
    updateModelDropdown();
    switchTab('results');
});

// ─── UI reset ─────────────────────────────────────────────────────────────────
function resetUI() {
    DOMElements.errorContainer.classList.add('hidden');
    const pills = document.getElementById('db_pills');
    if (pills) pills.remove();
    DOMElements.resultsHead.innerHTML = '';
    DOMElements.resultsBody.innerHTML = '';
    DOMElements.rowCount.textContent = '0';
    DOMElements.executionLatency.textContent = '';
    resetTraceUI();
}

function resetTraceUI() {
    DOMElements.ragOutcome.textContent = '-';
    DOMElements.ragOutcome.className = 'outcome-badge outcome-neutral';
    DOMElements.ragMatches.textContent = '-';
    DOMElements.ragReason.textContent = '-';
    DOMElements.ragReason.className = 'text-val';
    DOMElements.schemaIncluded.textContent = '-';
    DOMElements.schemaExcluded.textContent = '-';
    DOMElements.llmProvider.textContent = 'provider';
    DOMElements.promptSystem.textContent = '...';
    DOMElements.llmRawResponse.textContent = '...';
    DOMElements.llmAbstract.textContent = '...';
    DOMElements.paramSql.textContent = '...';
    DOMElements.bindParams.textContent = '{}';
    DOMElements.compilerLatency.textContent = '';
}

// ─── Main compilation entry point ─────────────────────────────────────────────
async function runCompilation() {
    const intent = DOMElements.input.value.trim();
    if (!intent) return;
    _lastIntent = intent;

    resetUI();
    DOMElements.runBtn.disabled = true;
    DOMElements.runBtn.textContent = 'Compiling...';

    const activeProvider = DOMElements.providerSelect.value;
    const activeModel = DOMElements.modelSelect.value;

    // Build the turn-pair container before clearing input
    const turnPair = document.createElement('div');
    turnPair.className = 'turn-pair';

    const userMsg = document.createElement('div');
    userMsg.className = 'chat-message user-message';
    userMsg.innerHTML = `<strong>User:</strong> ${intent}`;
    turnPair.appendChild(userMsg);
    DOMElements.chatHistory.appendChild(turnPair);
    DOMElements.input.value = '';

    const payload = {
        intent,
        explain: true, // pipeline always enabled
        schema_hints: [],
        provider_id: `${activeProvider}:${activeModel}`,
        session_id: currentSessionId,
        source_database: _pendingSourceDatabase,
    };
    _pendingSourceDatabase = null; // consumed — clear before fetch

    const jsonReqStr = JSON.stringify(payload, null, 2);
    DOMElements.jsonRequest.textContent = jsonReqStr;
    DOMElements.jsonResponse.textContent = 'Awaiting response...';

    try {
        const response = await fetch('/api/v1/query/execute', {
            method: 'POST',
            headers: _apiHeaders({ 'Content-Type': 'application/json' }),
            body: JSON.stringify(payload),
        });

        if (response.status === 401) {
            handleError({ title: 'Authentication Required (401)', message: 'No valid API key. Set aegis_api_key in localStorage and reload.' });
            return;
        }

        const data = await response.json();
        if (data.session_id) currentSessionId = data.session_id;

        const jsonResStr = JSON.stringify(data, null, 2);
        DOMElements.jsonResponse.textContent = jsonResStr;

        let errorData = null;
        let successData = null;

        if (!response.ok) {
            errorData = buildErrorData(data);
            handleError(errorData);

            const assistantMsg = document.createElement('div');
            assistantMsg.className = 'chat-message assistant-message error';
            assistantMsg.innerHTML =
                `<strong>Assistant (${activeProvider}:${activeModel}):</strong> ${data.message || 'Execution failed.'}`;
            turnPair.appendChild(assistantMsg);
        } else {
            successData = buildSuccessData(data);
            applySuccessData(successData);
            if (data.explainability) renderExplainability(data.explainability);

            const dbBadge = data.source_database_used
                ? ` <span style="font-size:11px;padding:1px 6px;border-radius:3px;background:#1e3a5f;color:#7ec8f0;vertical-align:middle;">${data.source_database_used}</span>`
                : '';
            const assistantMsg = document.createElement('div');
            assistantMsg.className = 'chat-message assistant-message success';
            assistantMsg.innerHTML =
                `<strong>Assistant (${activeProvider}:${activeModel}):</strong>${dbBadge}` +
                `<pre style="margin:5px 0 0;white-space:pre-wrap;font-family:monospace;font-size:13px;">${data.sql}</pre>`;
            turnPair.appendChild(assistantMsg);
        }

        // ── Capture & wire snapshot ───────────────────────────────────────────
        const snapIdx = captureSnapshot(
            jsonReqStr,
            jsonResStr,
            errorData,
            successData,
            data.explainability || null
        );

        turnPair.dataset.turnIdx = snapIdx;
        turnPair.title = 'Click to restore this turn\'s results';
        turnPair.addEventListener('click', () => restoreSnapshot(snapIdx));

        // Auto-select the newest turn
        document.querySelectorAll('.turn-pair').forEach(el => el.classList.remove('turn-selected'));
        turnPair.classList.add('turn-selected');

    } catch (err) {
        handleError({ title: 'Network Error', message: 'Could not reach the API.' });
    } finally {
        DOMElements.runBtn.disabled = false;
        DOMElements.runBtn.textContent = 'Run Compilation & Execute';
        DOMElements.chatHistory.scrollTop = DOMElements.chatHistory.scrollHeight;
    }
}

// ─── Error helpers ────────────────────────────────────────────────────────────
function buildErrorData(data) {
    const candidates = data.explainability?.candidates;
    if (data.code === 400 && candidates?.length > 0) {
        return {
            title: 'Ambiguous Database (400)',
            message: 'Multiple databases matched. Select one to retry:',
            candidates,
        };
    }
    let title;
    if (data.code === 400 && data.message?.includes('Unknown source_database')) title = 'Unknown Database (400)';
    else if (data.code === 400 && data.message?.includes('RAG')) title = 'Semantic RAG Failure (400)';
    else if (data.code === 403) title = 'Safety Policy Violation (403)';
    else if (data.code === 400 && data.message?.includes('Translation')) title = 'LLM Syntax Malformation (400)';
    else if (data.code === 502) title = 'LLM Generation Error (502)';
    else title = `Execution Halted (${data.code || 'Error'})`;
    return { title, message: data.message || 'Unknown error.' };
}

function handleError({ title, message, candidates }) {
    DOMElements.errorContainer.classList.remove('hidden');
    DOMElements.errorTitle.textContent = title;
    DOMElements.errorMessage.textContent = message;

    // Remove any previous pill row
    const existing = document.getElementById('db_pills');
    if (existing) existing.remove();

    if (candidates?.length > 0) {
        const pillBox = document.createElement('div');
        pillBox.id = 'db_pills';
        pillBox.style.cssText = 'margin-top:8px;display:flex;gap:6px;flex-wrap:wrap;';
        candidates.forEach(db => {
            const pill = document.createElement('button');
            pill.textContent = db;
            pill.style.cssText =
                'padding:3px 10px;border-radius:12px;border:1px solid #4a9eda;' +
                'background:#1a2e42;color:#7ec8f0;cursor:pointer;font-size:12px;';
            pill.addEventListener('click', () => {
                _pendingSourceDatabase = db;
                DOMElements.input.value = _lastIntent || '';
                runCompilation();
            });
            pillBox.appendChild(pill);
        });
        DOMElements.errorContainer.appendChild(pillBox);
    }
}

// ─── Success data helpers ─────────────────────────────────────────────────────
function buildSuccessData(data) {
    return { rowCount: data.row_count, latency: data.execution_latency_ms, results: data.results || [], sql: data.sql };
}

function applySuccessData(sd) {
    DOMElements.rowCount.textContent = sd.rowCount;
    DOMElements.executionLatency.textContent = `${(sd.latency || 0).toFixed(2)}ms DB exec`;
    DOMElements.resultsHead.innerHTML = '';
    DOMElements.resultsBody.innerHTML = '';

    if (sd.results && sd.results.length > 0) {
        const cols = Object.keys(sd.results[0]);
        const headerRow = document.createElement('tr');
        cols.forEach(col => { const th = document.createElement('th'); th.textContent = col; headerRow.appendChild(th); });
        DOMElements.resultsHead.appendChild(headerRow);

        sd.results.forEach(row => {
            const tr = document.createElement('tr');
            cols.forEach(col => {
                const td = document.createElement('td');
                const val = row[col];
                if (typeof val === 'boolean') { td.textContent = val ? 'TRUE' : 'FALSE'; td.className = 'dim'; }
                else if (val === null) { td.textContent = 'NULL'; td.className = 'dim'; }
                else { td.textContent = val; }
                tr.appendChild(td);
            });
            DOMElements.resultsBody.appendChild(tr);
        });
    } else {
        DOMElements.resultsBody.innerHTML = '<tr><td colspan="100%" class="dim text-center">No rows matched criteria.</td></tr>';
    }
}

// ─── Explainability renderer ──────────────────────────────────────────────────
function renderExplainability(exp) {
    let totalMs = 0;
    if (exp.llm) totalMs += exp.llm.latency_ms;
    DOMElements.compilerLatency.textContent = `~${totalMs.toFixed(2)}ms Pipeline`;

    if (exp.rag) {
        DOMElements.ragOutcome.textContent = exp.rag.outcome.replace(/_/g, ' ');
        if (exp.rag.outcome === 'SINGLE_HIGH_CONFIDENCE_MATCH') { DOMElements.ragOutcome.className = 'outcome-badge outcome-success'; DOMElements.ragMatches.textContent = exp.rag.matches.join(', '); }
        else if (exp.rag.outcome === 'AMBIGUOUS_MATCH') { DOMElements.ragOutcome.className = 'outcome-badge outcome-warn'; DOMElements.ragMatches.textContent = exp.rag.matches.join(', '); }
        else { DOMElements.ragOutcome.className = 'outcome-badge'; DOMElements.ragMatches.textContent = '[]'; }
        DOMElements.ragReason.textContent = exp.rag.reason || 'N/A';
    }

    if (exp.schema_filter) {
        DOMElements.schemaIncluded.textContent = `[${exp.schema_filter.included_aliases.join(', ')}]`;
        DOMElements.schemaExcluded.textContent = `[${exp.schema_filter.excluded_aliases.join(', ')}]`;
    }

    if (exp.prompt) {
        DOMElements.promptSystem.textContent = (!exp.prompt.system_prompt_redacted && exp.prompt.raw_system)
            ? exp.prompt.raw_system : '[REDACTED]';
    }

    if (exp.llm) {
        DOMElements.llmProvider.textContent = `${exp.llm.provider} - ${exp.llm.latency_ms.toFixed(0)}ms`;
        DOMElements.llmRawResponse.textContent = exp.llm.raw_response || '...';
    }

    if (exp.translation) {
        DOMElements.llmAbstract.textContent = exp.translation.llm_abstract_query || 'N/A';
        DOMElements.paramSql.textContent = exp.translation.parameterized_sql || 'N/A';
        DOMElements.bindParams.textContent = JSON.stringify(exp.translation.parameters, null, 2);
    }
}

// ─── Copy to clipboard ────────────────────────────────────────────────────────
window.copyToClipboard = function (elementId) {
    const el = document.getElementById(elementId);
    if (!el) return;
    navigator.clipboard.writeText(el.textContent).then(() => {
        const btn = el.parentElement?.previousElementSibling?.querySelector('.btn-copy');
        if (btn) {
            const orig = btn.textContent;
            btn.textContent = 'Copied!';
            setTimeout(() => { btn.textContent = orig; }, 2000);
        }
    }).catch(err => console.error('Failed to copy:', err));
};
