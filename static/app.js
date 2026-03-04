const DOMElements = {
    providerSelect: document.getElementById('provider_select'),
    modelSelect: document.getElementById('model_select'),
    chatHistory: document.getElementById('chat_history'),
    input: document.getElementById('intent_input'),
    runBtn: document.getElementById('run_btn'),
    explainToggle: document.getElementById('explain_toggle'),
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
    drawerToggle: document.getElementById('drawer_toggle'),
    drawerContent: document.getElementById('drawer_content'),
    jsonRequest: document.getElementById('json_request'),
    jsonResponse: document.getElementById('json_response'),

    // Trace Nodes
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
    bindParams: document.getElementById('bind_params')
};

let currentSessionId = null;

// Toggle Explainability UI
DOMElements.explainToggle.addEventListener('change', (e) => {
    if (e.target.checked) {
        DOMElements.tracePanel.classList.remove('hidden');
    } else {
        DOMElements.tracePanel.classList.add('hidden');
    }
});

// Toggle Drawer UI
DOMElements.drawerToggle.addEventListener('click', () => {
    DOMElements.drawerContent.classList.toggle('hidden');
    DOMElements.drawerToggle.classList.toggle('open');
});

// Allow Enter to submit
DOMElements.input.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        runCompilation();
    }
});

DOMElements.runBtn.addEventListener('click', runCompilation);

async function loadActiveVersion() {
    try {
        const res = await fetch('/api/v1/metadata/active');
        const data = await res.json();
        const verEl = document.getElementById('active_version');
        if (data && data.version_id) {
            verEl.textContent = data.version_id;
        } else {
            verEl.textContent = 'Development Sandbox';
        }
    } catch (e) {
        console.error("Failed to load active schema version", e);
    }
}

document.addEventListener('DOMContentLoaded', () => {
    loadActiveVersion();
    updateModelDropdown();
});

const providerModels = {
    'openai': ['gpt-4o', 'gpt-4o-mini', 'gpt-3.5-turbo'],
    'anthropic': ['claude-3-5-sonnet-20241022', 'claude-3-5-haiku-20241022', 'claude-3-haiku-20240307'],
    'google': ['gemini-1.5-pro', 'gemini-1.5-flash', 'gemini-1.5-flash-8b'],
    'xai': ['grok-2', 'grok-2-mini', 'grok-1'],
    'ollama': ['llama3']
};

function updateModelDropdown() {
    const provider = DOMElements.providerSelect.value;
    const models = providerModels[provider] || [];
    DOMElements.modelSelect.innerHTML = '';
    models.forEach(model => {
        const opt = document.createElement('option');
        opt.value = model;
        opt.textContent = model;
        DOMElements.modelSelect.appendChild(opt);
    });
}

DOMElements.providerSelect.addEventListener('change', updateModelDropdown);
function resetUI() {
    DOMElements.errorContainer.classList.add('hidden');
    DOMElements.resultsContainer.classList.add('hidden');

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
    DOMElements.executionLatency.textContent = '';
}

async function runCompilation() {
    const intent = DOMElements.input.value.trim();
    if (!intent) return;

    resetUI();
    DOMElements.runBtn.disabled = true;
    DOMElements.runBtn.textContent = 'Compiling...';

    // Append to Chat UI immediately
    const userMsg = document.createElement("div");
    userMsg.className = "chat-message user-message";
    userMsg.style.cssText = "background: rgba(255,255,255,0.05); padding: 8px; border-radius: 4px; border-left: 3px solid #64B5F6;";
    userMsg.innerHTML = `<strong>User:</strong> ${intent}`;
    DOMElements.chatHistory.appendChild(userMsg);

    // Clear intent
    DOMElements.input.value = "";

    const payload = {
        intent: intent,
        explain: DOMElements.explainToggle.checked,
        schema_hints: [],
        provider_id: `${DOMElements.providerSelect.value}:${DOMElements.modelSelect.value}`,
        session_id: currentSessionId
    };

    DOMElements.jsonRequest.textContent = JSON.stringify(payload, null, 2);
    DOMElements.jsonResponse.textContent = 'Awaiting response...';

    try {
        const response = await fetch('/api/v1/query/execute', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        const data = await response.json();

        if (data.session_id) {
            currentSessionId = data.session_id;
        }

        DOMElements.jsonResponse.textContent = JSON.stringify(data, null, 2);

        if (!response.ok) {
            handleError(data);

            // Append explicit error feedback
            const assistantMsg = document.createElement("div");
            assistantMsg.className = "chat-message assistant-message error";
            assistantMsg.style.cssText = "background: rgba(255,0,0,0.1); padding: 8px; border-radius: 4px; border-left: 3px solid #ff5252; color: #ff5252;";
            assistantMsg.innerHTML = `<strong>Assistant (${DOMElements.providerSelect.value}:${DOMElements.modelSelect.value}):</strong> ${data.message || 'Execution failed.'}`;
            DOMElements.chatHistory.appendChild(assistantMsg);

        } else {
            handleSuccess(data);
            if (data.explainability) {
                renderExplainability(data.explainability);
            }
            // Append LLM compilation trace
            const assistantMsg = document.createElement("div");
            assistantMsg.className = "chat-message assistant-message success";
            assistantMsg.style.cssText = "background: rgba(0,255,0,0.05); padding: 8px; border-radius: 4px; border-left: 3px solid #4CAF50;";
            assistantMsg.innerHTML = `<strong>Assistant (${DOMElements.providerSelect.value}:${DOMElements.modelSelect.value}):</strong> <pre style="margin: 5px 0 0 0; white-space: pre-wrap; font-family: monospace; font-size: 13px;">${data.sql}</pre>`;
            DOMElements.chatHistory.appendChild(assistantMsg);
        }
    } catch (err) {
        handleError({ code: 0, message: "Network Error: Could not reach the API." });
    } finally {
        DOMElements.runBtn.disabled = false;
        DOMElements.runBtn.textContent = 'Run Compilation & Execute';
        // Auto-scroll to bottom of chat history
        DOMElements.chatHistory.scrollTop = DOMElements.chatHistory.scrollHeight;
    }
}

function handleError(errorData) {
    DOMElements.errorContainer.classList.remove('hidden');
    // Interpret common proxy HTTP codes into clear UX domains
    if (errorData.code === 400 && errorData.message.includes('RAG')) {
        DOMElements.errorTitle.textContent = 'Semantic RAG Failure (400)';
    } else if (errorData.code === 403) {
        DOMElements.errorTitle.textContent = 'Safety Policy Violation (403)';
    } else if (errorData.code === 400 && errorData.message.includes('Translation')) {
        DOMElements.errorTitle.textContent = 'LLM Syntax Malformation (400)';
    } else if (errorData.code === 502) {
        DOMElements.errorTitle.textContent = 'LLM Generation Error (502)';
    } else {
        DOMElements.errorTitle.textContent = `Execution Halted (${errorData.code || 'Error'})`;
    }

    DOMElements.errorMessage.textContent = errorData.message;

    if (errorData.explainability) {
        renderExplainability(errorData.explainability);
    }
}

function handleSuccess(data) {
    DOMElements.resultsContainer.classList.remove('hidden');
    DOMElements.rowCount.textContent = data.row_count;
    DOMElements.executionLatency.textContent = `${data.execution_latency_ms.toFixed(2)}ms DB exec`;

    // Build Table
    DOMElements.resultsHead.innerHTML = '';
    DOMElements.resultsBody.innerHTML = '';

    if (data.results && data.results.length > 0) {
        // Headers
        const cols = Object.keys(data.results[0]);
        const headerRow = document.createElement('tr');
        cols.forEach(col => {
            const th = document.createElement('th');
            th.textContent = col;
            headerRow.appendChild(th);
        });
        DOMElements.resultsHead.appendChild(headerRow);

        // Body
        data.results.forEach(row => {
            const tr = document.createElement('tr');
            cols.forEach(col => {
                const td = document.createElement('td');
                const val = row[col];
                if (typeof val === 'boolean') {
                    td.textContent = val ? 'TRUE' : 'FALSE';
                    td.className = 'dim';
                } else if (val === null) {
                    td.textContent = 'NULL';
                    td.className = 'dim';
                } else {
                    td.textContent = val;
                }
                tr.appendChild(td);
            });
            DOMElements.resultsBody.appendChild(tr);
        });
    } else {
        DOMElements.resultsBody.innerHTML = '<tr><td colspan="100%" class="dim text-center">No rows matched criteria.</td></tr>';
    }
}

function renderExplainability(exp) {
    // Latency approximation (Query pipeline without physical execution)
    let totalMs = 0;
    if (exp.llm) totalMs += exp.llm.latency_ms;
    DOMElements.compilerLatency.textContent = `~${totalMs.toFixed(2)}ms Pipeline`;

    // 1. RAG
    if (exp.rag) {
        DOMElements.ragOutcome.textContent = exp.rag.outcome.replace(/_/g, ' ');
        if (exp.rag.outcome === 'SINGLE_HIGH_CONFIDENCE_MATCH') {
            DOMElements.ragOutcome.className = 'outcome-badge outcome-success';
            DOMElements.ragMatches.textContent = exp.rag.matches.join(', ');
        } else if (exp.rag.outcome === 'AMBIGUOUS_MATCH') {
            DOMElements.ragOutcome.className = 'outcome-badge outcome-warn';
            DOMElements.ragMatches.textContent = exp.rag.matches.join(', ');
        } else {
            DOMElements.ragOutcome.className = 'outcome-badge';
            DOMElements.ragMatches.textContent = '[]';
        }
        DOMElements.ragReason.textContent = exp.rag.reason || 'N/A';
    }

    // 2. Schema
    if (exp.schema_filter) {
        DOMElements.schemaIncluded.textContent = `[${exp.schema_filter.included_aliases.join(', ')}]`;
        DOMElements.schemaExcluded.textContent = `[${exp.schema_filter.excluded_aliases.join(', ')}]`;
    }

    // 3. Prompt
    if (exp.prompt) {
        if (!exp.prompt.system_prompt_redacted && exp.prompt.raw_system) {
            DOMElements.promptSystem.textContent = exp.prompt.raw_system;
        } else {
            DOMElements.promptSystem.textContent = '[REDACTED]';
        }
    }

    // 4. LLM
    if (exp.llm) {
        DOMElements.llmProvider.textContent = `${exp.llm.provider} - ${exp.llm.latency_ms.toFixed(0)}ms`;
        DOMElements.llmRawResponse.textContent = exp.llm.raw_response || '...';
    }

    // 5. Translation
    if (exp.translation) {
        DOMElements.llmAbstract.textContent = exp.translation.llm_abstract_query || 'N/A';
        DOMElements.paramSql.textContent = exp.translation.parameterized_sql || 'N/A';
        DOMElements.bindParams.textContent = JSON.stringify(exp.translation.parameters, null, 2);
    }
}

// Global copy handler for the raw JSON payload Drawer
window.copyToClipboard = function (elementId) {
    const el = document.getElementById(elementId);
    if (!el) return;

    // Copy the text content safely
    navigator.clipboard.writeText(el.textContent)
        .then(() => {
            // Find the button within the preceding header row
            const headerRow = el.parentElement.previousElementSibling;
            const btn = headerRow ? headerRow.querySelector('.btn-copy') : null;

            if (btn) {
                const originalText = btn.textContent;
                btn.textContent = 'Copied!';
                btn.style.backgroundColor = 'var(--bg-success)';
                btn.style.color = 'var(--text-main)';
                setTimeout(() => {
                    btn.textContent = originalText;
                    btn.style.backgroundColor = '';
                    btn.style.color = '';
                }, 2000);
            }
        })
        .catch(err => {
            console.error('Failed to copy text: ', err);
        });
};
