export class TableRenderer {
  constructor(columns, data, cellRenderers) {
    this.columns = columns;
    this.data = data;
    this.cellRenderers = cellRenderers || {};
    this.tBody = null;
    this.tHead = null;
    this.table = null;
    this.selectedRows = new Set();
    this.initialize();
  }

  initialize() {
    this.table = document.createElement("table");
    this.table.style.width = "100%";
    this.table.style.borderCollapse = "collapse";

    this.tHead = document.createElement("thead");
    this.tBody = document.createElement("tbody");

    this.table.appendChild(this.tHead);
    this.table.appendChild(this.tBody);
  }

  setTable(table) {
    this.table = table;
    return this;
  }

  getTable() {
    return this.table;
  }

  renderBody(rowIndices = null) {
    this.tBody.innerHTML = "";
    const indices = rowIndices || [...Array(this.data.length).keys()];

    indices.forEach((i) => {
      const row = this.data[i];
      const tr = document.createElement("tr");
      tr.style.color = "grey"; // Default unselected state

      this.columns.forEach((col) => {
        const td = document.createElement("td");
        td.textContent = row[col.column];
        td.style.padding = "4px 8px";
        tr.appendChild(td);
      });

      // Restore selection state if row was previously selected
      if (this.selectedRows.has(i)) {
        tr.style.fontWeight = "bold";
        tr.style.color = "black";
        tr.selected = true;
      }

      this.tBody.appendChild(tr);
    });
  }

  updateSelection(selectedIndices) {
    this.selectedRows = new Set(selectedIndices);
    Array.from(this.tBody.children).forEach((tr, idx) => {
      if (this.selectedRows.has(idx)) {
        tr.style.fontWeight = "bold";
        tr.style.color = "black";
        tr.selected = true;
      } else {
        tr.style.fontWeight = "normal";
        tr.style.color = "grey";
        tr.selected = false;
      }
    });
  }

  getSelectedRowIndices() {
    return Array.from(this.selectedRows);
  }
}
