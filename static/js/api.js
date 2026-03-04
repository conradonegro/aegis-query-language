export const API = {
    async fetchVersions() {
        const res = await fetch('/api/v1/metadata/versions');
        return res.json();
    },
    async fetchSchema(versionId) {
        const res = await fetch(`/api/v1/metadata/versions/${versionId}/schema`);
        return res.json();
    },
    async createDraft(baselineId = null) {
        const res = await fetch('/api/v1/metadata/versions', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ baseline_version_id: baselineId })
        });
        return res.json();
    },
    async updateTable(tableId, data) {
        const res = await fetch(`/api/v1/metadata/tables/${tableId}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });
        return res.json();
    },
    async updateColumn(columnId, data) {
        const res = await fetch(`/api/v1/metadata/columns/${columnId}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });
        return res.json();
    },
    async obfuscateSchema(versionId) {
        const res = await fetch(`/api/v1/metadata/versions/${versionId}/obfuscate`, {
            method: 'POST'
        });
        return res.json();
    }
};
