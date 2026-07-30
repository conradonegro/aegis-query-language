const architectureData = {
    api: {
        id: 'api',
        title: 'API Layer',
        icon: '🌐',
        desc: 'FastAPI entrypoint — query endpoints, metadata CRUD, auth, and the Steward web console.',
        x: 15, y: 50,
        features: [
            'Hosts the public REST API for query generation and execution under /api/v1/query/*',
            'Exposes full metadata CRUD endpoints under /api/v1/metadata/* — versions, tables, columns, relationships, policies',
            'Compiles a draft metadata version into a signed RegistrySchema artifact and triggers a hot-reload of the live schema',
            'Authenticates tenants via API key and propagates the tenant context down through the compiler and execution layers',
            'Serves the static Steward UI from /static — the in-browser console where non-engineers author the semantic schema',
            'Defines all Pydantic request and response models and wires FastAPI dependencies (registry, compiler, executor, audit sink)'
        ],
        subcomponents: [
            { name: 'Query Router', desc: 'POST /api/v1/query/generate and /api/v1/query/execute. Wraps user input into a UserIntent and dispatches to the compiler, then optionally to the executor.' },
            { name: 'Metadata Router', desc: 'CRUD over MetadataVersion, MetadataTable, MetadataColumn, and MetadataRelationship — drives the draft → compile → promote lifecycle of the semantic schema.' },
            { name: 'Metadata Compiler', desc: 'Turns a draft metadata version into a signed RegistrySchema artifact that the Steward loader can verify and hot-reload at runtime.' },
            { name: 'Auth & Tenancy', desc: 'API-key validation and tenant-scope propagation, so every downstream call knows whose data it is operating on.' },
            { name: 'Steward UI Mount', desc: 'Serves the static web console used by data stewards to edit the semantic schema without touching code or filing a deploy ticket.' }
        ]
    },
    compiler: {
        id: 'compiler',
        title: 'Compiler Pipeline',
        icon: '⚙️',
        desc: 'Natural-language-to-SQL pipeline. Strictly forbidden from opening a database connection.',
        x: 50, y: 50,
        features: [
            'Receives a UserIntent and runs a fixed 7-step pipeline: RAG hints → schema filter → prompt build → LLM call → AST parse → safety check → translate',
            'Builds a PromptEnvelope that contains only abstract aliases — physical table and column names never leave this module in a form the LLM can see',
            'Pluggable LLM gateway: OpenAI, Anthropic, Google, xAI, Ollama (local models), and a generic CLI-driven provider for tools without an HTTP API',
            'Validates LLM output as a parsed AST via sqlglot — rejects DDL, DML, CTEs, subqueries, and any function not on the explicit whitelist',
            'Enforces per-column SafetyClassification rules from the registry (not-projectable, not-filterable-by-literal, PII, etc.) before the query can be translated',
            'Translates abstract aliases to physical targets and extracts every literal value into a $1-style bound parameter — output is a fully-resolved ExecutableQuery with zero string interpolation',
            'Uses immutable copy-on-write AST rewrites (ValidatedAST) so the provenance of each transformation step is preserved and auditable'
        ],
        subcomponents: [
            { name: 'Compiler Engine', desc: 'engine.py — orchestrates the seven pipeline stages end-to-end and is the single entry point that the API layer calls.' },
            { name: 'Schema Filter', desc: 'Deterministic filter that scopes the full RegistrySchema down to the tables and columns plausibly relevant to the user intent, keeping prompts small and focused.' },
            { name: 'Prompt Builder', desc: 'Assembles the PromptEnvelope: user intent + filtered schema (as abstract aliases) + RAG hints. Strictly no physical targets, no connection strings, no environment hints.' },
            { name: 'LLM Gateway', desc: 'Provider-agnostic interface (gateway.py) with adapters for OpenAI, Anthropic, Google, xAI, Ollama, and a CLI-based provider — chosen per-request or via LLM_PROVIDER env var.' },
            { name: 'AST Parser', desc: 'sqlglot-based parser (parser.py) that turns the LLM string into a tree and rejects anything outside the SELECT whitelist before it reaches the safety engine.' },
            { name: 'Safety Engine', desc: 'Walks the validated AST and checks every column reference against the per-column safety classifications carried in the RegistrySchema.' },
            { name: 'Translator', desc: 'Maps abstract aliases to physical targets and extracts literals into bound parameters — emits the final ExecutableQuery that the executor will run.' },
            { name: 'RAG Hints Adapter', desc: 'hints.py / backend_hints.py — calls into the RAG engine and packages the results into compiler-friendly column- and value-level hints for the prompt builder.' },
            { name: 'Session Store', desc: 'Tracks per-conversation context so follow-up questions ("and by region?") can be resolved against the prior intent without re-prompting from scratch.' }
        ]
    },
    execution: {
        id: 'execution',
        title: 'Execution Engine',
        icon: '🗄️',
        desc: 'The only layer permitted to open a database connection. Runs parameterized SQL on PostgreSQL.',
        x: 85, y: 50,
        features: [
            'Sole owner of physical PostgreSQL connections — import-linter blocks every other module from importing asyncpg',
            'Executes parameterized queries only; literals arrive pre-bound from the compiler and are never interpolated into SQL strings',
            'Appends SET LOCAL statement_timeout on every query, so a runaway query can never pin a connection indefinitely',
            'Uses least-privilege Postgres roles: DB_URL_RUNTIME for user queries, separate roles for registry reads and steward authoring',
            'Pure asyncpg — no ORM expression builders, on purpose, so the SQL hitting the database is exactly the SQL in the code',
            'Returns rows in a typed result envelope with timing metadata; failures are surfaced with enough context for the audit log without leaking SQL details to the caller'
        ],
        subcomponents: [
            { name: 'Executor', desc: 'executor.py — accepts an ExecutableQuery, picks the right role-scoped connection pool, applies the timeout, and runs the parameterized query.' },
            { name: 'Connection Wrapper', desc: 'Thin asyncpg pool manager that handles checkout/return and injects statement_timeout and tenant context on every connection use.' },
            { name: 'Result Models', desc: 'Typed result structures returned to the API layer — carry rows plus timing and row-count metadata used by the audit sink.' }
        ]
    },
    steward: {
        id: 'steward',
        title: 'Steward Registry',
        icon: '🛡️',
        desc: 'Owner of the RegistrySchema — the in-memory, signed semantic schema the compiler reads on every request.',
        x: 50, y: 15,
        features: [
            'Owns RegistrySchema, the only object shared across bounded contexts — every other module reads it, none mutates it directly',
            'Loads signed registry artifacts produced by the metadata compiler and verifies them before swapping the live schema',
            'Hot-reloads on POST /api/v1/metadata/compile/{version_id} — schema changes take effect with no restart and no downtime',
            'Defines the abstract table/column/relationship model that the compiler reasons in; physical targets are only attached during load',
            'Carries the per-column SafetyClassification rules that the compiler safety engine enforces at query time',
            'Provides fast in-memory lookup APIs that the schema filter, prompt builder, and translator hit on every request'
        ],
        subcomponents: [
            { name: 'Registry Loader', desc: 'loader.py — reads compiled artifacts from the registry database, verifies signatures, and atomically swaps the active RegistrySchema reference.' },
            { name: 'In-Memory Registry', desc: 'The hot lookup surface used by the compiler — tables, columns, relationships, and policies indexed for O(1) access from the request path.' },
            { name: 'Schema Models', desc: 'Pydantic models (models.py) for tables, columns, relationships, and policies — the canonical type system shared across compiler, RAG, and audit.' }
        ]
    },
    rag: {
        id: 'rag',
        title: 'RAG Engine',
        icon: '🧠',
        desc: 'In-memory vector store for column- and value-level semantic hints. Stands for Retrieval-Augmented Generation.',
        x: 50, y: 85,
        features: [
            'Indexes column names, descriptions, and known value vocabularies as embeddings at registry load time',
            'Resolves natural-language phrasings ("signed up", "active customers", "EMEA") to specific columns and value tokens in the registry',
            'Runs entirely in-process — small working set, no external vector database to operate, deploy, or back up',
            'Outputs hints consumed by the compiler prompt builder before the LLM call, so the model sees focused vocabulary instead of having to guess',
            'Rebuilds automatically when the RegistrySchema hot-reloads, so hints never drift out of sync with the live semantic schema'
        ],
        subcomponents: [
            { name: 'Vector Store', desc: 'store.py — in-memory embedding index over column metadata and known value vocabularies, queried on every compile.' },
            { name: 'Embedding Client', desc: 'Generates embeddings both for indexed registry items at load time and for incoming user phrasings at query time.' },
            { name: 'Normalizer', desc: 'normalizer.py — cleans and canonicalizes user phrasings before lookup so casing, punctuation, and whitespace do not hurt recall.' },
            { name: 'Builder', desc: 'builder.py — wires a freshly-loaded RegistrySchema into the vector store at startup and on every hot-reload.' }
        ]
    },
    audit: {
        id: 'audit',
        title: 'Audit Telemetry',
        icon: '📊',
        desc: 'Out-of-band telemetry sink. Logs the full query lifecycle without blocking or breaking the user-facing response.',
        x: 15, y: 85,
        features: [
            'Asynchronous and fire-and-forget — by contract, must never raise an exception that affects the user-facing API response',
            'Consumes QueryAuditEvent structs covering user intent, abstract SQL, final parameterized SQL, timing breakdown, and outcome',
            'Captures safety-engine rejections and translator failures, not just successes — invaluable signal for tuning the semantic layer over time',
            'Persists records to a dedicated audit store, decoupled from the runtime query path so heavy logging cannot back-pressure user queries',
            'Supports event chaining for tamper-evident audit trails — each record references the hash of the previous record',
            'Produces a complete record per query: prompt, model response, generated SQL, who ran it, when, and how long each stage took'
        ],
        subcomponents: [
            { name: 'Audit Logger', desc: 'logger.py — the public interface that the API, compiler, and executor call to record events; offloads all work to the background worker.' },
            { name: 'Append Worker', desc: 'append.py — background task that drains queued events and writes them to the audit store without blocking the request path.' },
            { name: 'Event Chaining', desc: 'chaining.py — links each audit record to the hash of the previous one, so any retroactive edit to the log is detectable.' },
            { name: 'Event Models', desc: 'models.py — typed QueryAuditEvent structs that describe the full lifecycle of a single query from intent through result.' }
        ]
    }
};

const connections = [
    { from: 'api', to: 'compiler' },
    { from: 'compiler', to: 'execution' },
    { from: 'steward', to: 'compiler' },
    { from: 'rag', to: 'compiler' },
    { from: 'api', to: 'audit' },
    { from: 'compiler', to: 'audit' },
    { from: 'execution', to: 'audit' }
];

const queryFlowData = [
    {
        id: 'step-1',
        title: 'User Request',
        componentId: 'api',
        icon: '🗣️',
        desc: 'The user submits a natural language question via the API.',
        code: '{"intent": "how many users signed up last month?"}',
        detail: 'The API receives the request, authenticates the user, and forwards the intent to the Compiler.'
    },
    {
        id: 'step-2',
        title: 'Schema Filter',
        componentId: 'steward',
        icon: '🛡️',
        desc: 'Scopes the schema registry to find tables relevant to the intent.',
        code: 'Found relevant tables from in-memory registry:\n- users (T1)\n- signups (T2)',
        detail: 'The Steward Registry provides the compiled schema artifact. The compiler filters it down to minimize context window usage.'
    },
    {
        id: 'step-3',
        title: 'Semantic Retrieval',
        componentId: 'rag',
        icon: '🧠',
        desc: 'Retrieves column and value semantic hints.',
        code: 'Vector Match for "signed up":\n-> mapping to column `created_at`\n-> context: "refers to user registration date"',
        detail: 'The RAG Engine provides semantic context, helping the LLM map user phrasing to specific database values without hardcoding.'
    },
    {
        id: 'step-4',
        title: 'Prompt Builder',
        componentId: 'compiler',
        icon: '📝',
        desc: 'Builds an LLM prompt with abstract table and column aliases.',
        code: 'Table: T1 (users)\nColumns: C1 (id), C2 (created_at)\nSemantic Hint: "signed up" -> C2\nIntent: how many users signed up last month?',
        detail: 'The Prompt Builder fuses the user intent, filtered schema (as abstract aliases), and RAG hints. The physical schema is never exposed to the LLM.'
    },
    {
        id: 'step-5',
        title: 'LLM Gateway',
        componentId: 'compiler',
        icon: '🤖',
        desc: 'Calls the LLM provider to generate abstract SQL.',
        code: 'SELECT COUNT(*) FROM T1 WHERE C2 > NOW() - INTERVAL 1 MONTH',
        detail: 'The LLM returns an SQL query using only the abstract aliases provided in the prompt. It relies entirely on the semantic hints to understand the schema.'
    },
    {
        id: 'step-6',
        title: 'AST Parser & Safety Engine',
        componentId: 'compiler',
        icon: '🔐',
        desc: 'Parses the SQL to ensure it is safe (no DML) and complies with column-level policies.',
        code: 'Validating AST...\nNo DML/DDL found.\nChecking policy for C2: READ ALLOWED -> OK',
        detail: 'The AST Parser strictly blocks modifications (subqueries, non-whitelisted functions). The Safety Engine verifies column-level policies.'
    },
    {
        id: 'step-7',
        title: 'Translator',
        componentId: 'compiler',
        icon: '🔄',
        desc: 'Maps the abstract aliases back to physical table names and parameterizes literals.',
        code: 'SELECT COUNT(*) FROM public.users WHERE created_at > $1',
        detail: 'Reconstructs the final SQL. The literal (1 MONTH) is extracted and parameterized ($1) to prevent SQL injection.'
    },
    {
        id: 'step-8',
        title: 'Execution Engine',
        componentId: 'execution',
        icon: '⚡',
        desc: 'Executes the parameterized SQL against the physical database.',
        code: 'Executing async query with params: [$1="1 month"]\nstatement_timeout: 5000ms',
        detail: 'The only module that touches the database. It enforces statement timeouts to protect against runaway queries.'
    },
    {
        id: 'step-9',
        title: 'Audit Logging',
        componentId: 'audit',
        icon: '📊',
        desc: 'Asynchronously logs the entire query lifecycle.',
        code: 'Log Entry:\n- Intent: how many users signed up last month?\n- LLM Time: 850ms\n- Exec Time: 12ms\n- Status: Success',
        detail: 'Telemetry is captured non-blockingly, ensuring performance and compliance tracking without delaying the user response.'
    },
    {
        id: 'step-10',
        title: 'Query Results',
        componentId: 'api',
        icon: '✅',
        desc: 'Results are returned to the user.',
        code: '{\n  "status": "success",\n  "data": [{"count": 42}]\n}',
        detail: 'The API formats the database result and returns it, completing the request lifecycle.'
    }
];

document.addEventListener('DOMContentLoaded', () => {
    // Tab switching logic
    const tabs = document.querySelectorAll('.view-tab');
    const views = document.querySelectorAll('.view-container');

    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            tabs.forEach(t => t.classList.remove('active'));
            views.forEach(v => v.classList.remove('active'));
            
            tab.classList.add('active');
            const targetView = document.getElementById(`${tab.dataset.view}-view`);
            targetView.classList.add('active');

            if (tab.dataset.view === 'architecture') {
                setTimeout(drawConnections, 50); // Redraw connections when unhidden
            }
        });
    });

    // --- ARCHITECTURE MAP LOGIC ---
    const diagramArea = document.getElementById('diagram-area');
    const svgLayer = document.getElementById('connections');
    const detailsPanel = document.getElementById('details-panel');
    const panelContent = document.getElementById('panel-content');
    const closeBtn = document.getElementById('close-panel');

    // Render nodes
    Object.values(architectureData).forEach(nodeData => {
        const nodeEl = document.createElement('div');
        nodeEl.className = 'node glass-panel';
        nodeEl.id = `node-${nodeData.id}`;
        nodeEl.style.left = `${nodeData.x}%`;
        nodeEl.style.top = `${nodeData.y}%`;

        nodeEl.innerHTML = `
            <div class="node-icon">${nodeData.icon}</div>
            <div class="node-title">${nodeData.title}</div>
            <div class="node-desc">${nodeData.desc}</div>
        `;

        nodeEl.addEventListener('click', () => {
            selectNode(nodeData.id);
        });

        diagramArea.appendChild(nodeEl);
    });

    // Draw connections
    function drawConnections() {
        if (!document.getElementById('architecture-view').classList.contains('active')) return;
        
        svgLayer.innerHTML = ''; // Clear existing
        const areaRect = diagramArea.getBoundingClientRect();
        if (areaRect.width === 0) return; // Not visible

        connections.forEach(conn => {
            const fromNode = document.getElementById(`node-${conn.from}`);
            const toNode = document.getElementById(`node-${conn.to}`);

            if (!fromNode || !toNode) return;

            const fromX = (fromNode.offsetLeft / areaRect.width) * 100;
            const fromY = (fromNode.offsetTop / areaRect.height) * 100;
            const toX = (toNode.offsetLeft / areaRect.width) * 100;
            const toY = (toNode.offsetTop / areaRect.height) * 100;

            const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
            path.classList.add('connection-line');
            path.id = `conn-${conn.from}-${conn.to}`;
            
            const isHorizontal = Math.abs(fromY - toY) < 10;
            const isVertical = Math.abs(fromX - toX) < 10;

            let d = '';
            if (isHorizontal || isVertical) {
                d = `M ${fromX}% ${fromY}% L ${toX}% ${toY}%`;
            } else {
                const controlX = fromX + (toX - fromX) / 2;
                d = `M ${fromX}% ${fromY}% C ${controlX}% ${fromY}%, ${controlX}% ${toY}%, ${toX}% ${toY}%`;
            }

            path.setAttribute('d', d);
            svgLayer.appendChild(path);
        });
    }

    drawConnections();
    window.addEventListener('resize', drawConnections);

    function selectNode(id) {
        document.querySelectorAll('.node').forEach(n => n.classList.remove('active'));
        document.querySelectorAll('.connection-line').forEach(l => l.classList.remove('active'));

        document.getElementById(`node-${id}`).classList.add('active');

        connections.forEach(conn => {
            if (conn.from === id || conn.to === id) {
                const line = document.getElementById(`conn-${conn.from}-${conn.to}`);
                if (line) line.classList.add('active');
            }
        });

        const data = architectureData[id];
        
        let featuresHtml = data.features.map(f => `<li>${f}</li>`).join('');
        let subcomponentsHtml = data.subcomponents.map(s => `<li><span class="sub-title">${s.name}</span>${s.desc}</li>`).join('');

        panelContent.innerHTML = `
            <div class="detail-header">
                <div class="detail-icon">${data.icon}</div>
                <div class="detail-title">${data.title}</div>
            </div>
            <div class="detail-desc">${data.desc}</div>
            
            <div class="section-title">Key Features</div>
            <ul class="feature-list">
                ${featuresHtml}
            </ul>

            <div class="section-title">Subcomponents</div>
            <ul class="subcomponent-list">
                ${subcomponentsHtml}
            </ul>
        `;

        detailsPanel.classList.remove('hidden');
    }

    closeBtn.addEventListener('click', () => {
        detailsPanel.classList.add('hidden');
        document.querySelectorAll('.node').forEach(n => n.classList.remove('active'));
        document.querySelectorAll('.connection-line').forEach(l => l.classList.remove('active'));
    });

    // --- QUERY FLOW LOGIC ---
    const flowTimeline = document.getElementById('flow-timeline');
    const flowVisualization = document.getElementById('flow-visualization');

    function renderFlowSteps() {
        queryFlowData.forEach((step, index) => {
            const stepEl = document.createElement('div');
            stepEl.className = 'flow-step';
            stepEl.id = step.id;
            
            stepEl.innerHTML = `
                <div class="step-number">${index + 1}</div>
                <div class="step-content">
                    <h3>${step.title}</h3>
                    <p>${step.desc}</p>
                </div>
            `;

            stepEl.addEventListener('click', () => {
                selectFlowStep(index);
            });

            flowTimeline.appendChild(stepEl);
        });
    }

    function selectFlowStep(index) {
        document.querySelectorAll('.flow-step').forEach(s => s.classList.remove('active'));
        
        const stepData = queryFlowData[index];
        const stepEl = document.getElementById(stepData.id);
        if (stepEl) {
            stepEl.classList.add('active');
            stepEl.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
        }

        const archComponent = architectureData[stepData.componentId];

        flowVisualization.innerHTML = `
            <div class="vis-container">
                <div class="vis-header">
                    <div class="vis-icon">${stepData.icon}</div>
                    <div class="vis-title">${stepData.title}</div>
                    <div class="vis-component-badge">
                        <span class="badge-icon">${archComponent.icon}</span>
                        ${archComponent.title}
                    </div>
                </div>
                
                <div class="vis-component-card glass-panel">
                    <div class="comp-title">Component Role in Architecture</div>
                    <div class="comp-desc">${archComponent.desc}</div>
                    <ul class="comp-features">
                        ${archComponent.features.slice(0, 2).map(f => `<li>${f}</li>`).join('')}
                    </ul>
                </div>

                <div class="vis-code">
                    <div class="code-header">Data Payload & Actions</div>
                    ${stepData.code}
                </div>
                <div class="vis-detail">${stepData.detail}</div>
            </div>
        `;
    }

    renderFlowSteps();
    // Select first step by default
    if (queryFlowData.length > 0) {
        selectFlowStep(0);
    }
});
