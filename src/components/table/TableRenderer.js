export class TableRenderer {
  constructor(
    columns,
    data,
    cellRenderers = {},
    selectRow,
    unselectRow,
    getRowIndex
  ) {
    this.columns = columns;
    this.data = data;
    this.cellRenderers = cellRenderers;
    this.selectRow = selectRow;
    this.unselectRow = unselectRow;
    this.getRowIndex = getRowIndex;

    // Initialize table parts as null
    this.table = null;
    this.tHead = null;
    this.tBody = null;
  }

  // Add setter for table element
  setTable(table) {
    this.table = table;
    // Create thead and tbody if they don't exist
    if (!this.tHead) {
      this.tHead = document.createElement("thead");
      this.table.appendChild(this.tHead);
    }
    if (!this.tBody) {
      this.tBody = document.createElement("tbody");
      this.table.appendChild(this.tBody);
    }
  }

  // Get table element
  getTable() {
    return this.table;
  }

  // Method to render table rows
  renderRows(dataInd, table, additionalLines) {
    // Create tbody element if it doesn't exist
    if (!this.tBody) {
      this.tBody = document.createElement("tbody");
      table.appendChild(this.tBody);
    }

    let lastLineAdded = -1;
    let addingRows = false;

    const addTableRows = (howMany) => {
      if (addingRows) {
        return; // Prevent overlapping calls
      }
      addingRows = true;

      let min = lastLineAdded + 1; // Corrected: Start from the next line
      let max = Math.min(min + howMany - 1, dataInd.length - 1); // Corrected: Use Math.min to avoid exceeding dataInd.length

      for (let row = min; row <= max; row++) {
        let dataIndex = dataInd[row]; // Adjust index for dataInd
        if (dataIndex === undefined) continue;

        let tr = document.createElement("tr");
        tr.selected = false;
        Object.assign(tr.style, {
          color: "grey",
          borderBottom: "1px solid #ddd",
        });
        this.tBody.appendChild(tr);

        this.columns.forEach((c) => {
          let td = document.createElement("td");

          // Use custom renderer if available for this column
          if (typeof this.cellRenderers[c.column] === "function") {
            td.innerHTML = "";
            td.appendChild(
              this.cellRenderers[c.column](
                this.data[dataIndex][c.column],
                this.data[dataIndex]
              )
            );
          } else {
            td.innerText = this.data[dataIndex][c.column]; // Default: Set text content
          }

          tr.appendChild(td);
          td.style.color = "inherit";
          td.style.fontWidth = "inherit";
        });

        // Add event listeners for row selection
        tr.addEventListener("click", (event) => {
          let rowIndex = this.getRowIndex(tr);

          if (this.shiftDown) {
            // SHIFT-CLICK (select range)
            let s = this.getSelection().map((s) => s.index);
            if (s.length == 0) s = [rowIndex]; // If nothing selected, use current row index
            let minSelIndex = Math.min(...s);
            let maxSelIndex = Math.max(...s);

            if (rowIndex <= minSelIndex) {
              for (let i = rowIndex; i < minSelIndex; i++) {
                const trToSelect = this.tBody.querySelectorAll("tr")[i];
                if (trToSelect) this.selectRow(trToSelect);
              }
            } else if (rowIndex >= maxSelIndex) {
              for (let i = maxSelIndex + 1; i <= rowIndex; i++) {
                const trToSelect = this.tBody.querySelectorAll("tr")[i];
                if (trToSelect) this.selectRow(trToSelect);
              }
            }
          } else if (this.ctrlDown) {
            // CTRL-CLICK (toggle individual row selection)
            if (tr.selected) {
              this.unselectRow(tr);
            } else {
              this.selectRow(tr);
            }
          } else {
            // NORMAL CLICK (clear selection and select clicked row)
            this.clearSelection();
            this.selectRow(tr);
          }

          this.selectionUpdated();
        });

        // Add hover effect for rows
        tr.addEventListener("mouseover", () => {
          tr.style.backgroundColor = "#f0f0f0"; // Highlight on hover
        });

        tr.addEventListener("mouseout", () => {
          tr.style.backgroundColor = ""; // Reset background color
        });

        // this.lastLineAdded++;
        lastLineAdded = row; // Update the last line added
      }

      addingRows = false;
    };
  }
}
