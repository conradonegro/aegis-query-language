export class GraphView {
    constructor(containerId) {
        this.container = document.getElementById(containerId);
        this.currentSchema = null;
        this.selectedTableId = null;
    }

    render(schema) {
        this.currentSchema = schema || this.currentSchema;
        this.selectedTableId = null;
        this.drawEdges();
    }

    filterByTable(tableId) {
        if (this.selectedTableId === tableId) {
            this.selectedTableId = null; // Toggle off
        } else {
            this.selectedTableId = tableId;
        }
        this.drawEdges();
    }

    drawEdges() {
        const schema = this.currentSchema;
        if (!schema || !schema.relationships) {
            this.container.innerHTML = '';
            return;
        }

        let edges = schema.relationships;
        if (this.selectedTableId) {
            edges = edges.filter(e => e.source_table_id === this.selectedTableId || e.target_table_id === this.selectedTableId);
        }

        let html = `<h3 style="margin-top: 0; font-size: 16px;">Table Relationships <span class="dim" style="font-weight: normal;">(${edges.length} edges shown)</span></h3>`;

        if (edges.length === 0) {
            html += `<p class="dim" style="font-size: 14px;">No relationships to display.</p>`;
        } else {
            html += `<ul class="edge-list">`;
            edges.forEach(edge => {
                const srcTable = schema.tables.find(t => t.table_id === edge.source_table_id);
                const tgtTable = schema.tables.find(t => t.table_id === edge.target_table_id);

                const srcName = srcTable ? srcTable.alias : edge.source_table_id;
                const tgtName = tgtTable ? tgtTable.alias : edge.target_table_id;

                const srcStyle = (this.selectedTableId === edge.source_table_id) ? 'background: #bb9af7; color: #1a1b26;' : '';
                const tgtStyle = (this.selectedTableId === edge.target_table_id) ? 'background: #bb9af7; color: #1a1b26;' : '';

                html += `
                    <li class="edge-item">
                        <span class="edge-node src" style="${srcStyle}">${srcName}</span>
                        <span class="edge-link">${edge.relationship_type.toUpperCase()} <span class="dim">(${edge.cardinality})</span> ➔ </span>
                        <span class="edge-node tgt" style="${tgtStyle}">${tgtName}</span>
                    </li>
                `;
            });
            html += `</ul>`;
        }

        this.container.innerHTML = html;
    }
}
