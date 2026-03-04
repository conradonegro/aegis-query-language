import { API } from './api.js';

export class VersionManager {
    constructor(containerId, onSelectVersion) {
        this.container = document.getElementById(containerId);
        this.onSelectVersion = onSelectVersion;
        this.versions = [];
    }

    async init() {
        await this.loadVersions();
        this.bindEvents();
    }

    async loadVersions() {
        this.versions = await API.fetchVersions();
        this.render();
    }

    bindEvents() {
        document.getElementById('btn_create_draft').addEventListener('click', async () => {
            const activeVersion = this.versions.find(v => v.status === 'active') || this.versions[0];
            const defaultId = activeVersion ? activeVersion.version_id : '';
            const baseline = prompt("Enter a Baseline Version ID to clone its schema (retaining tables and edges):", defaultId);

            if (baseline === null) return; // Cancelled

            try {
                const newVersion = await API.createDraft(baseline || null);
                await this.loadVersions();
                alert(`Created new draft: ${newVersion.version_id}`);
            } catch (e) {
                alert("Failed to create draft.");
            }
        });
    }

    render() {
        let html = '<ul class="version-list">';
        this.versions.forEach(v => {
            html += `
                <li class="version-item ${v.status}" data-id="${v.version_id}">
                    <div class="v-header" style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 5px;">
                        <strong style="font-size: 11px;">${v.version_id}</strong>
                        <span class="badge badge-${v.status}">${v.status.toUpperCase()}</span>
                    </div>
                    <div class="v-date dim" style="font-size: 12px;">${new Date(v.created_at).toLocaleString()}</div>
                    <div style="display: flex; gap: 5px; margin-top: 10px;">
                        <button class="btn btn-sm btn-select-v">View Schema Graph</button>
                        <button class="btn btn-sm btn-obfuscate" style="background: #32344a; color: #ff9e64;">Auto-Obfuscate</button>
                    </div>
                </li>
            `;
        });
        html += '</ul>';
        this.container.innerHTML = html;

        this.container.querySelectorAll('.btn-select-v').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const li = e.target.closest('.version-item');

                // Visual selection state
                this.container.querySelectorAll('.version-item').forEach(i => i.style.background = 'transparent');
                li.style.background = '#24283b';

                this.onSelectVersion(li.dataset.id);
            });
        });

        this.container.querySelectorAll('.btn-obfuscate').forEach(btn => {
            btn.addEventListener('click', async (e) => {
                const li = e.target.closest('.version-item');
                const vId = li.dataset.id;
                if (!confirm(`WARNING: This will permanently overwrite all table and column aliases in version ${vId.substring(0, 8)} with sequential IDs (e.g., table0001, col0001) for Zero-Knowledge anonymity.\n\nContinue?`)) return;

                btn.textContent = 'Working...';
                try {
                    const stats = await API.obfuscateSchema(vId);
                    alert(`Success: Obfuscated ${stats.tables_obfuscated} tables and ${stats.columns_obfuscated} columns.`);

                    // Force refresh the schema view if it is currently selected
                    if (li.style.background === 'rgb(36, 40, 59)' || li.style.background === '#24283b') {
                        this.onSelectVersion(vId);
                    }
                } catch (err) {
                    alert('Failed to obfuscate schema.');
                }
                btn.textContent = 'Auto-Obfuscate';
            });
        });
    }
}
