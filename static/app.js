const DOMElements = {
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

    const payload = {
        intent: intent,
        explain: DOMElements.explainToggle.checked,
        schema_hints: []
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
        DOMElements.jsonResponse.textContent = JSON.stringify(data, null, 2);

        if (!response.ok) {
            handleError(data);
        } else {
            handleSuccess(data);
            if (data.explainability) {
                renderExplainability(data.explainability);
            }
        }
    } catch (err) {
        handleError({ code: 0, message: "Network Error: Could not reach the API." });
    } finally {
        DOMElements.runBtn.disabled = false;
        DOMElements.runBtn.textContent = 'Run Compilation & Execute';
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
