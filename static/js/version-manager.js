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

    _lifecycleButtons(status) {
        switch (status) {
            case 'draft':
                return `<button class="btn btn-sm btn-transition" data-target="pending_review" style="background: #7aa2f7; color: #1a1b26;">Submit for Review</button>`;
            case 'pending_review':
                return `
                    <button class="btn btn-sm btn-transition" data-target="active" style="background: #9ece6a; color: #1a1b26;">Approve</button>
                    <button class="btn btn-sm btn-transition" data-target="draft" style="background: #32344a; color: #ff9e64;">Return to Draft</button>
                `;
            case 'active':
                return `
                    <button class="btn btn-sm btn-compile" style="background: #bb9af7; color: #1a1b26;">Compile &amp; Deploy</button>
                    <button class="btn btn-sm btn-transition" data-target="archived" style="background: #32344a; color: #565f89;">Archive</button>
                `;
            default:
                return '';
        }
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
                    <div style="display: flex; gap: 5px; margin-top: 10px; flex-wrap: wrap;">
                        <button class="btn btn-sm btn-select-v">View Schema Graph</button>
                        <button class="btn btn-sm btn-obfuscate" style="background: #32344a; color: #ff9e64;">Auto-Obfuscate</button>
                        ${this._lifecycleButtons(v.status)}
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

        this.container.querySelectorAll('.btn-transition').forEach(btn => {
            btn.addEventListener('click', async (e) => {
                const li = e.target.closest('.version-item');
                const vId = li.dataset.id;
                const target = btn.dataset.target;
                const label = btn.textContent.trim();

                if (!confirm(`Transition version ${vId.substring(0, 8)} to '${target}'?`)) return;

                btn.textContent = 'Working...';
                btn.disabled = true;
                try {
                    await API.transitionStatus(vId, target);
                    await this.loadVersions();
                } catch (err) {
                    alert(`Failed: ${err.message}`);
                    btn.textContent = label;
                    btn.disabled = false;
                }
            });
        });

        this.container.querySelectorAll('.btn-compile').forEach(btn => {
            btn.addEventListener('click', async (e) => {
                const li = e.target.closest('.version-item');
                const vId = li.dataset.id;

                if (!confirm(`Compile and hot-deploy version ${vId.substring(0, 8)}? This will replace the active runtime schema.`)) return;

                btn.textContent = 'Compiling...';
                btn.disabled = true;
                try {
                    const artifact = await API.compileVersion(vId);
                    alert(`Compiled successfully.\nArtifact: ${artifact.artifact_id}\nHash: ${artifact.artifact_hash.substring(0, 16)}...`);
                    await this.loadVersions();
                } catch (err) {
                    alert(`Compilation failed: ${err.message}`);
                    btn.textContent = 'Compile & Deploy';
                    btn.disabled = false;
                }
            });
        });
    }
}
