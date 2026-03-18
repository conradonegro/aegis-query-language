import { API } from './api.js';
import { RagValuesDrawer } from './rag-values-drawer.js';

export class SchemaView {
    constructor(containerId, onSelectTable) {
        this.container = document.getElementById(containerId);
        this.onSelectTable = onSelectTable;
        this.currentSchema = null;
        this._drawer = new RagValuesDrawer();
    }

    async loadSchema(versionId) {
        this.container.innerHTML = '<h3 style="text-align: center; margin-top: 50px;">Loading Schema Metadata...</h3>';
        try {
            this.currentSchema = await API.fetchSchema(versionId);
            this.render();
        } catch (e) {
            this.container.innerHTML = `<h3 class="error" style="color: #f7768e; text-align: center;">Failed to load schema for ${versionId}</h3>`;
        }
    }

    render() {
        if (!this.currentSchema) return;

        let html = `<div style="position: sticky; top: 0; background: #1a1b26; z-index: 10; padding-bottom: 15px; border-bottom: 1px solid #333; margin-bottom: 15px; padding-top: 10px;">`;
        html += `<div style="display: flex; justify-content: space-between; align-items: center;">`;
        html += `<h2 style="margin: 0;">Schema Editor <span class="dim" style="font-size: 14px;">(Version: ${this.currentSchema.version_id})</span></h2>`;
        html += `<button id="btn_toggle_graph" class="btn btn-primary btn-sm">Toggle Graph Panel</button>`;
        html += `</div>`;
        html += `<div style="margin-top: 15px;"><input type="text" id="table_filter" placeholder="Filter tables by alias or physical name..." style="width: 100%; padding: 10px; background: #1a1b26; color: #c0caf5; border: 1px solid #414868; border-radius: 4px;"></div>`;
        html += `</div>`;
        html += `<div class="tables-grid">`;

        if (this.currentSchema.tables.length === 0) {
            html += `<p class="dim">No tables in this schema version.</p>`;
        }

        this.currentSchema.tables.forEach(t => {
            html += `
                <div class="schema-table card" data-table-id="${t.table_id}" style="cursor: pointer; border: 2px solid transparent; transition: border-color 0.2s;">
                    <div class="card-header" style="display: flex; flex-direction: column; align-items: flex-start; gap: 10px; margin-bottom: 10px; width: 100%;">
                        <div style="display: flex; align-items: center; justify-content: space-between; width: 100%;">
                            <div style="display: flex; align-items: center; gap: 10px;">
                                <span class="dim">Table Alias: </span>
                                <input type="text" class="edit-table-alias" data-id="${t.table_id}" data-original="${t.alias}" value="${t.alias}" />
                                <span class="real-name dim" style="font-size: 12px;">(Physical: ${t.real_name})</span>
                            </div>
                            <button class="btn btn-sm btn-toggle-table" style="background: transparent; color: #c0caf5; border: none; font-size: 16px; cursor: pointer;">▼</button>
                        </div>
                        <div style="display: flex; align-items: center; gap: 10px; width: 100%;">
                            <span class="dim">Description: </span>
                            <input type="text" class="edit-table-description" data-id="${t.table_id}" data-original="${t.description || ''}" value="${t.description || ''}" style="width: 100%; flex: 1;" placeholder="Table description..." />
                        </div>
                    </div>
                    <div class="card-body">
                        <table class="column-list">
                            <thead>
                                <tr>
                                    <th>Column Alias / Description</th>
                                    <th>Physical Context</th>
                                    <th>Data Type</th>
                                    <th>Safety Classifications</th>
                                    <th>RAG Configuration</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${t.columns.map(c => `
                                    <tr>
                                        <td>
                                            <div style="display: flex; flex-direction: column; gap: 5px;">
                                                <input type="text" class="edit-col-alias" data-id="${c.column_id}" data-original="${c.alias}" value="${c.alias}" placeholder="Alias" />
                                                <input type="text" class="edit-col-description" data-id="${c.column_id}" data-original="${c.description || ''}" value="${c.description || ''}" placeholder="Description..." style="font-size: 11px; padding: 4px;" />
                                            </div>
                                        </td>
                                        <td class="dim">${c.real_name}</td>
                                        <td class="dim">${c.data_type} ${c.is_primary_key ? '<strong style="color: #bb9af7;">(PK)</strong>' : ''}</td>
                                        <td style="display: flex; gap: 10px;">
                                            <label><input type="checkbox" class="col-flag" data-target="allowed_in_select" data-id="${c.column_id}" ${c.allowed_in_select ? 'checked' : ''}> Select <span class="dim">(read)</span></label>
                                            <label><input type="checkbox" class="col-flag" data-target="allowed_in_filter" data-id="${c.column_id}" ${c.allowed_in_filter ? 'checked' : ''}> Filter <span class="dim">(where)</span></label>
                                            <label><input type="checkbox" class="col-flag" data-target="allowed_in_join" data-id="${c.column_id}" ${c.allowed_in_join ? 'checked' : ''}> Join <span class="dim">(on)</span></label>
                                        </td>
                                        <td>
                                            <div style="display: flex; flex-direction: column; gap: 6px; font-size: 12px;">
                                                <label><input type="checkbox" class="col-flag" data-target="rag_enabled" data-id="${c.column_id}" ${c.rag_enabled ? 'checked' : ''}> Enabled</label>
                                                <label>Cardinality:
                                                    <select class="col-rag-cardinality" data-id="${c.column_id}" data-original="${c.rag_cardinality_hint || 'low'}" style="background: #1a1b26; color: #c0caf5; border: 1px solid #414868; border-radius: 3px; padding: 2px 4px;">
                                                        <option value="low" ${(!c.rag_cardinality_hint || c.rag_cardinality_hint === 'low') ? 'selected' : ''}>Low</option>
                                                        <option value="medium" ${c.rag_cardinality_hint === 'medium' ? 'selected' : ''}>Medium</option>
                                                        <option value="high" ${c.rag_cardinality_hint === 'high' ? 'selected' : ''}>High</option>
                                                    </select>
                                                </label>
                                                <label>Limit:
                                                    <input type="number" class="col-rag-limit" data-id="${c.column_id}" data-original="${c.rag_limit ?? 10}" value="${c.rag_limit ?? 10}" min="1" max="100" style="width: 55px; background: #1a1b26; color: #c0caf5; border: 1px solid #414868; border-radius: 3px; padding: 2px 4px;" />
                                                </label>
                                                <button class="btn-manage-values btn btn-sm"
                                                    data-col-id="${c.column_id}"
                                                    style="margin-top: 2px; padding: 3px 8px; font-size: 11px; background: #32344a; border: 1px solid #414868;">
                                                    ⚙ Values
                                                </button>
                                            </div>
                                        </td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>
            `;
        });
        html += `</div>`;
        this.container.innerHTML = html;
        this.bindEvents();
    }

    bindEvents() {
        const toggleGraphBtn = this.container.querySelector('#btn_toggle_graph');
        if (toggleGraphBtn) {
            toggleGraphBtn.addEventListener('click', () => {
                const graphCol = document.getElementById('graph_container');
                if (graphCol.style.display === 'none') {
                    graphCol.style.display = 'block';
                } else {
                    graphCol.style.display = 'none';
                }
            });
        }

        const filterInput = this.container.querySelector('#table_filter');
        if (filterInput) {
            filterInput.addEventListener('input', (e) => {
                const term = e.target.value.toLowerCase();
                this.container.querySelectorAll('.schema-table').forEach(card => {
                    const text = card.textContent.toLowerCase();
                    const inputs = Array.from(card.querySelectorAll('input[type="text"]')).map(i => i.value.toLowerCase()).join(' ');
                    card.style.display = (text.includes(term) || inputs.includes(term)) ? 'block' : 'none';
                });
            });
        }

        this.container.querySelectorAll('.schema-table').forEach(card => {
            const toggleBtn = card.querySelector('.btn-toggle-table');
            const cardBody = card.querySelector('.card-body');

            if (toggleBtn && cardBody) {
                toggleBtn.addEventListener('click', (e) => {
                    e.stopPropagation(); // Prevent triggering card selection
                    if (cardBody.style.display === 'none') {
                        cardBody.style.display = 'block';
                        toggleBtn.textContent = '▼';
                    } else {
                        cardBody.style.display = 'none';
                        toggleBtn.textContent = '▶';
                    }
                });
            }

            card.addEventListener('click', (e) => {
                if (e.target.tagName.toLowerCase() === 'input' || e.target.tagName.toLowerCase() === 'label' || e.target.tagName.toLowerCase() === 'button') return;

                const isSelected = card.style.borderColor === 'rgb(187, 154, 247)' || card.style.borderColor === '#bb9af7';

                this.container.querySelectorAll('.schema-table').forEach(c => c.style.borderColor = 'transparent');

                if (!isSelected) {
                    card.style.borderColor = '#bb9af7';
                }

                if (this.onSelectTable) {
                    this.onSelectTable(card.dataset.tableId);
                }
            });
        });

        const handleUpdate = async (e, apiCall, payloadKey, entityName) => {
            const id = e.target.dataset.id;
            const oldVal = e.target.dataset.original || '';
            const newVal = e.target.value;

            if (oldVal === newVal) return; // No change

            if (!confirm(`Save changes to ${entityName}?\n\nOld value: "${oldVal}"\nNew value: "${newVal}"`)) {
                e.target.value = oldVal;
                return;
            }

            e.target.style.background = '#32344a'; // Loading indicator
            try {
                const payload = {};
                payload[payloadKey] = newVal;
                await apiCall(id, payload);

                // Success feedback
                e.target.style.background = '#9ece6a'; // Green
                e.target.style.color = '#1a1b26';
                e.target.dataset.original = newVal;

                setTimeout(() => {
                    e.target.style.background = '#1a1b26';
                    e.target.style.color = '#c0caf5';
                }, 1000);
            } catch (err) {
                alert(`Failed to update ${entityName}.`);
                e.target.value = oldVal;
                e.target.style.background = '#1a1b26';
            }
        };

        this.container.querySelectorAll('.edit-table-alias').forEach(input => {
            input.addEventListener('change', (e) => handleUpdate(e, API.updateTable.bind(API), 'alias', 'Table Alias'));
        });

        this.container.querySelectorAll('.edit-table-description').forEach(input => {
            input.addEventListener('change', (e) => handleUpdate(e, API.updateTable.bind(API), 'description', 'Table Description'));
        });

        this.container.querySelectorAll('.edit-col-alias').forEach(input => {
            input.addEventListener('change', (e) => handleUpdate(e, API.updateColumn.bind(API), 'alias', 'Column Alias'));
        });

        this.container.querySelectorAll('.edit-col-description').forEach(input => {
            input.addEventListener('change', (e) => handleUpdate(e, API.updateColumn.bind(API), 'description', 'Column Description'));
        });

        this.container.querySelectorAll('.col-flag').forEach(checkbox => {
            checkbox.addEventListener('change', async (e) => {
                const id = e.target.dataset.id;
                const target = e.target.dataset.target;
                const payload = {};
                payload[target] = e.target.checked;
                await API.updateColumn(id, payload);
            });
        });

        this.container.querySelectorAll('.col-rag-cardinality').forEach(select => {
            select.addEventListener('change', async (e) => {
                const id = e.target.dataset.id;
                const oldVal = e.target.dataset.original;
                const newVal = e.target.value;
                if (oldVal === newVal) return;
                e.target.dataset.original = newVal;
                await API.updateColumn(id, { rag_cardinality_hint: newVal });
            });
        });

        this.container.querySelectorAll('.col-rag-limit').forEach(input => {
            input.addEventListener('change', async (e) => {
                const id = e.target.dataset.id;
                const oldVal = e.target.dataset.original;
                const newVal = e.target.value;
                if (oldVal === newVal) return;
                e.target.dataset.original = newVal;
                await API.updateColumn(id, { rag_limit: parseInt(newVal, 10) });
            });
        });

        this.container.querySelectorAll('.btn-manage-values').forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                const colId = btn.dataset.colId;
                // Find the column and its table from the loaded schema
                for (const table of this.currentSchema.tables) {
                    const col = table.columns.find(c => c.column_id === colId);
                    if (col) {
                        this._drawer.open(col, table.columns);
                        return;
                    }
                }
            });
        });
    }
}
