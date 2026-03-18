import { API } from './api.js';

const STRATEGIES = [
    { value: 'distinct',      label: 'Distinct'      },
    { value: 'top_n_by',      label: 'Top N by column' },
    { value: 'most_frequent', label: 'Most frequent'  },
];

export class RagValuesDrawer {
    constructor() {
        this._el = null;
        this._col = null;
        this._tableColumns = [];
        this._values = [];
        this._init();
    }

    _init() {
        const el = document.createElement('div');
        el.id = 'rag_drawer';
        el.style.cssText = `
            position: fixed; top: 0; right: -480px; width: 460px; height: 100vh;
            background: #1a1b26; border-left: 1px solid #414868;
            box-shadow: -4px 0 24px rgba(0,0,0,0.5);
            display: flex; flex-direction: column;
            transition: right 0.25s ease; z-index: 1000;
            font-family: monospace; font-size: 13px; color: #c0caf5;
        `;
        el.innerHTML = `
            <div style="padding:16px 20px; border-bottom:1px solid #333; display:flex; justify-content:space-between; align-items:center;">
                <span id="drawer_title" style="font-size:14px; font-weight:bold; color:#bb9af7;"></span>
                <button id="drawer_close" style="background:none; border:none; color:#565f89; font-size:18px; cursor:pointer;">✕</button>
            </div>

            <div style="padding:16px 20px; border-bottom:1px solid #333; display:flex; flex-direction:column; gap:10px;">
                <div style="display:flex; gap:10px; align-items:center;">
                    <label style="min-width:90px; color:#565f89;">Strategy</label>
                    <select id="drawer_strategy" style="flex:1; background:#24283b; color:#c0caf5; border:1px solid #414868; border-radius:4px; padding:5px 8px;"></select>
                </div>
                <div id="drawer_order_row" style="display:none; gap:10px; align-items:center;">
                    <label style="min-width:90px; color:#565f89;">Order by</label>
                    <select id="drawer_order_col" style="flex:1; background:#24283b; color:#c0caf5; border:1px solid #414868; border-radius:4px; padding:5px 8px;"></select>
                    <select id="drawer_order_dir" style="width:80px; background:#24283b; color:#c0caf5; border:1px solid #414868; border-radius:4px; padding:5px 8px;">
                        <option value="desc">DESC</option>
                        <option value="asc">ASC</option>
                    </select>
                </div>
                <div style="display:flex; gap:10px; align-items:center;">
                    <label style="min-width:90px; color:#565f89;">Limit</label>
                    <input id="drawer_limit" type="number" min="1" max="1000" value="100"
                        style="width:80px; background:#24283b; color:#c0caf5; border:1px solid #414868; border-radius:4px; padding:5px 8px;" />
                    <label style="display:flex; align-items:center; gap:6px; color:#565f89; cursor:pointer; margin-left:auto;">
                        <input id="drawer_refresh" type="checkbox" />
                        refresh on compile
                    </label>
                </div>
                <div style="display:flex; gap:8px; justify-content:flex-end;">
                    <button id="drawer_save_strategy" class="btn btn-sm" style="background:#32344a; color:#c0caf5; border:1px solid #414868;">Save strategy</button>
                    <button id="drawer_preview" class="btn btn-sm" style="background:#32344a; color:#c0caf5; border:1px solid #414868;">Preview sample</button>
                    <button id="drawer_auto_populate" class="btn btn-sm btn-primary">Auto-populate</button>
                </div>
            </div>

            <div style="padding:12px 20px; border-bottom:1px solid #333;">
                <div style="display:flex; gap:8px; align-items:center;">
                    <input id="drawer_add_input" type="text" placeholder="Add a value…"
                        style="flex:1; background:#24283b; color:#c0caf5; border:1px solid #414868; border-radius:4px; padding:6px 8px;" />
                    <button id="drawer_add_btn" class="btn btn-sm btn-primary">Add</button>
                </div>
                <textarea id="drawer_bulk_input" placeholder="Bulk paste — one value per line…"
                    style="margin-top:8px; width:100%; box-sizing:border-box; height:60px; background:#24283b; color:#c0caf5; border:1px solid #414868; border-radius:4px; padding:6px 8px; resize:vertical;"></textarea>
                <div style="display:flex; justify-content:flex-end; margin-top:4px;">
                    <button id="drawer_bulk_btn" class="btn btn-sm" style="background:#32344a; color:#c0caf5; border:1px solid #414868;">Import bulk</button>
                </div>
            </div>

            <div style="padding:10px 20px; border-bottom:1px solid #333; display:flex; justify-content:space-between; align-items:center;">
                <span id="drawer_count" style="color:#565f89;"></span>
                <button id="drawer_clear_all" class="btn btn-sm" style="background:transparent; color:#f7768e; border:1px solid #f7768e;">Clear all</button>
            </div>

            <div id="drawer_status" style="padding:8px 20px; font-size:12px; min-height:22px;"></div>

            <div id="drawer_values_list" style="flex:1; overflow-y:auto; padding:0 20px 20px;"></div>
        `;
        document.body.appendChild(el);
        this._el = el;
        this._bindStaticEvents();
    }

    open(col, tableColumns) {
        this._col = col;
        this._tableColumns = tableColumns;
        this._el.querySelector('#drawer_title').textContent =
            `RAG Values — ${col.real_name}`;
        this._populateStrategyUI();
        this._el.style.right = '0';
        this._loadValues();
    }

    close() {
        this._el.style.right = '-480px';
        this._col = null;
    }

    _populateStrategyUI() {
        const col = this._col;
        const stratSel = this._el.querySelector('#drawer_strategy');
        stratSel.innerHTML = STRATEGIES.map(s =>
            `<option value="${s.value}" ${col.rag_sample_strategy === s.value ? 'selected' : ''}>${s.label}</option>`
        ).join('');

        const orderColSel = this._el.querySelector('#drawer_order_col');
        orderColSel.innerHTML = this._tableColumns.map(c =>
            `<option value="${c.real_name}" ${col.rag_order_by_column === c.real_name ? 'selected' : ''}>${c.real_name}</option>`
        ).join('');

        const orderDirSel = this._el.querySelector('#drawer_order_dir');
        orderDirSel.value = col.rag_order_direction || 'desc';

        this._el.querySelector('#drawer_limit').value = col.rag_limit || 100;
        this._el.querySelector('#drawer_refresh').checked = col.refresh_on_compile || false;

        this._toggleOrderRow(col.rag_sample_strategy === 'top_n_by');
    }

    _toggleOrderRow(show) {
        this._el.querySelector('#drawer_order_row').style.display = show ? 'flex' : 'none';
    }

    async _loadValues() {
        this._setStatus('Loading…');
        try {
            this._values = await API.listColumnValues(this._col.column_id);
            this._renderValues();
            this._setStatus('');
        } catch {
            this._setStatus('Failed to load values.', true);
        }
    }

    _renderValues() {
        const list = this._el.querySelector('#drawer_values_list');
        this._el.querySelector('#drawer_count').textContent =
            `${this._values.length} value${this._values.length !== 1 ? 's' : ''}`;

        if (this._values.length === 0) {
            list.innerHTML = '<p style="color:#565f89; margin-top:12px;">No values yet.</p>';
            return;
        }
        list.innerHTML = this._values.map(v => `
            <div style="display:flex; justify-content:space-between; align-items:center;
                        padding:6px 0; border-bottom:1px solid #24283b;">
                <span style="overflow:hidden; text-overflow:ellipsis; white-space:nowrap; max-width:380px;">${_esc(v.value)}</span>
                <button data-vid="${v.value_id}" class="del-val-btn"
                    style="background:none; border:none; color:#565f89; cursor:pointer; font-size:14px; flex-shrink:0;">✕</button>
            </div>
        `).join('');

        list.querySelectorAll('.del-val-btn').forEach(btn => {
            btn.addEventListener('click', async () => {
                const vid = btn.dataset.vid;
                btn.disabled = true;
                await API.deleteColumnValue(this._col.column_id, vid);
                this._values = this._values.filter(v => v.value_id !== vid);
                this._renderValues();
            });
        });
    }

    _setStatus(msg, isError = false) {
        const el = this._el.querySelector('#drawer_status');
        el.textContent = msg;
        el.style.color = isError ? '#f7768e' : '#9ece6a';
    }

    _bindStaticEvents() {
        this._el.querySelector('#drawer_close').addEventListener('click', () => this.close());

        this._el.querySelector('#drawer_strategy').addEventListener('change', e => {
            this._toggleOrderRow(e.target.value === 'top_n_by');
        });

        this._el.querySelector('#drawer_save_strategy').addEventListener('click', async () => {
            await this._saveStrategy();
        });

        this._el.querySelector('#drawer_preview').addEventListener('click', async () => {
            await this._runPreview();
        });

        this._el.querySelector('#drawer_auto_populate').addEventListener('click', async () => {
            if (!confirm('This will clear all existing values and replace them with the sample. Continue?')) return;
            await this._autoPopulate();
        });

        this._el.querySelector('#drawer_add_btn').addEventListener('click', async () => {
            await this._addSingle();
        });

        this._el.querySelector('#drawer_add_input').addEventListener('keydown', async e => {
            if (e.key === 'Enter') await this._addSingle();
        });

        this._el.querySelector('#drawer_bulk_btn').addEventListener('click', async () => {
            await this._importBulk();
        });

        this._el.querySelector('#drawer_clear_all').addEventListener('click', async () => {
            if (!confirm('Delete all values for this column?')) return;
            this._setStatus('Clearing…');
            await API.clearColumnValues(this._col.column_id);
            this._values = [];
            this._renderValues();
            this._setStatus('Cleared.');
        });
    }

    async _saveStrategy() {
        const strategy = this._el.querySelector('#drawer_strategy').value;
        const orderCol = this._el.querySelector('#drawer_order_col').value;
        const orderDir = this._el.querySelector('#drawer_order_dir').value;
        const limit = parseInt(this._el.querySelector('#drawer_limit').value, 10);
        const refresh = this._el.querySelector('#drawer_refresh').checked;
        this._setStatus('Saving…');
        try {
            await API.updateColumn(this._col.column_id, {
                rag_sample_strategy: strategy,
                rag_order_by_column: strategy === 'top_n_by' ? orderCol : null,
                rag_order_direction: orderDir,
                rag_limit: limit,
                refresh_on_compile: refresh,
            });
            this._col = { ...this._col, rag_sample_strategy: strategy,
                rag_order_by_column: orderCol, rag_order_direction: orderDir,
                rag_limit: limit, refresh_on_compile: refresh };
            this._setStatus('Strategy saved.');
        } catch (e) {
            this._setStatus(`Save failed: ${e.message}`, true);
        }
    }

    async _runPreview() {
        await this._saveStrategy();
        this._setStatus('Sampling from DB…');
        try {
            const values = await API.sampleColumnValues(this._col.column_id);
            this._setStatus(`Preview: ${values.length} values from DB (not saved).`);
            const list = this._el.querySelector('#drawer_values_list');
            list.innerHTML = `<p style="color:#7aa2f7; margin:8px 0 4px;">Preview (not saved):</p>` +
                values.map(v => `<div style="padding:3px 0; border-bottom:1px solid #24283b;">${_esc(v)}</div>`).join('');
        } catch (e) {
            this._setStatus(`Preview failed: ${e.message}`, true);
        }
    }

    async _autoPopulate() {
        await this._saveStrategy();
        this._setStatus('Sampling from DB…');
        try {
            const values = await API.sampleColumnValues(this._col.column_id);
            this._setStatus('Clearing existing values…');
            await API.clearColumnValues(this._col.column_id);
            this._setStatus(`Importing ${values.length} values…`);
            const result = await API.bulkAddColumnValues(this._col.column_id, values);
            this._setStatus(
                `Done. ${result.imported} imported, ${result.skipped_duplicate} duplicates skipped.`
            );
            await this._loadValues();
        } catch (e) {
            this._setStatus(`Auto-populate failed: ${e.message}`, true);
        }
    }

    async _addSingle() {
        const input = this._el.querySelector('#drawer_add_input');
        const value = input.value.trim();
        if (!value) return;
        this._setStatus('Adding…');
        try {
            const v = await API.addColumnValue(this._col.column_id, value);
            this._values.push(v);
            this._renderValues();
            input.value = '';
            this._setStatus('Added.');
        } catch (e) {
            this._setStatus(`Failed: ${e.message}`, true);
        }
    }

    async _importBulk() {
        const textarea = this._el.querySelector('#drawer_bulk_input');
        const values = textarea.value.split('\n').map(v => v.trim()).filter(Boolean);
        if (!values.length) return;
        this._setStatus(`Importing ${values.length} values…`);
        try {
            const result = await API.bulkAddColumnValues(this._col.column_id, values);
            this._setStatus(
                `${result.imported} imported, ${result.skipped_duplicate} duplicates, ${result.skipped_invalid} invalid.`
            );
            textarea.value = '';
            await this._loadValues();
        } catch (e) {
            this._setStatus(`Bulk import failed: ${e.message}`, true);
        }
    }
}

function _esc(str) {
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}
