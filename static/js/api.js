const _STORAGE_KEY = 'aegis_api_key';

function _apiFetch(url, options = {}) {
    const key = localStorage.getItem(_STORAGE_KEY);
    const headers = { ...(options.headers || {}) };
    if (key) headers['Authorization'] = `Bearer ${key}`;

    return fetch(url, { ...options, headers }).then(res => {
        if (res.status === 401) {
            localStorage.removeItem(_STORAGE_KEY);
            window.dispatchEvent(new CustomEvent('aegis:auth-required'));
        } else if (res.status === 403) {
            window.dispatchEvent(new CustomEvent('aegis:auth-forbidden'));
        }
        return res;
    });
}

export const API = {
    async fetchVersions() {
        const res = await _apiFetch('/api/v1/metadata/versions');
        return res.json();
    },
    async fetchSchema(versionId) {
        const res = await _apiFetch(`/api/v1/metadata/versions/${versionId}/schema`);
        return res.json();
    },
    async createDraft(baselineId = null) {
        const res = await _apiFetch('/api/v1/metadata/versions', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ baseline_version_id: baselineId })
        });
        if (!res.ok) {
            const err = await res.json();
            throw new Error(err.detail || 'Failed to create draft.');
        }
        return res.json();
    },
    async updateTable(tableId, data) {
        const res = await _apiFetch(`/api/v1/metadata/tables/${tableId}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });
        return res.json();
    },
    async updateColumn(columnId, data) {
        const res = await _apiFetch(`/api/v1/metadata/columns/${columnId}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });
        return res.json();
    },
    async obfuscateSchema(versionId) {
        const res = await _apiFetch(`/api/v1/metadata/versions/${versionId}/obfuscate`, {
            method: 'POST'
        });
        return res.json();
    },
    async transitionStatus(versionId, status, reason = null) {
        const res = await _apiFetch(`/api/v1/metadata/versions/${versionId}/status`, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ status, reason })
        });
        if (!res.ok) {
            const err = await res.json();
            throw new Error(err.detail || `Transition to '${status}' failed.`);
        }
        return res.json();
    },
    async compileVersion(versionId) {
        const res = await _apiFetch(`/api/v1/metadata/compile/${versionId}`, {
            method: 'POST'
        });
        if (!res.ok) {
            const err = await res.json();
            throw new Error(err.detail || 'Compilation failed.');
        }
        return res.json();
    }
};
