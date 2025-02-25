import * as d3 from "npm:d3";
import { ColumnManager } from "./ColumnManager.js";
import { TableRenderer } from "./TableRenderer.js";
import { SortController } from "./SortController.js";
import { ColShiftController } from "./ColShiftController.js";
import { HistogramController } from "./HistogramController.js";
import { DuckDBBinningService } from "./DuckDBBinningService.js";
import { FilterService } from "./FilterService.js";
import { DuckDBDataProcessor } from "../_base/duckdb-processor.js";

export class SorterTable {
  constructor(data, columnNames, changed, options = {}) {
    if (!new.target) {
      throw new Error("SorterTable must be instantiated with new");
    }

    // Return a promise that resolves to the instance after async initialization
    return (async () => {
      try {
        this.changed = changed;
        this.options = {
          containerHeight: options.height || "400px",
          containerWidth: options.width || "100%",
          rowsPerPage: options.rowsPerPage || 50,
          loadMoreThreshold: options.loadMoreThreshold || 100,
          retryAttempts: options.retryAttempts || 3,
          retryDelay: options.retryDelay || 1000,
          maxOrdinalBins: options.maxOrdinalBins || 20,
        };

        this.normalizedColumns = this.normalizeColumnDefinitions(columnNames);
        this.table = this.createTableElement();
        this.duckDBTableName = "main_table";
        this.duckDBProcessor = new DuckDBDataProcessor(
          null,
          this.duckDBTableName
        );

        // Retry DuckDB connection
        let attempts = 0;
        while (attempts < this.options.retryAttempts) {
          try {
            await this.duckDBProcessor.connect();
            this.binningService = new DuckDBBinningService(
              this.duckDBProcessor
            );
            break;
          } catch (error) {
            attempts++;
            if (attempts >= this.options.retryAttempts) {
              throw new Error(
                `Failed to initialize DuckDB after ${attempts} attempts: ${error.message}`
              );
            }
            console.warn(
              `DuckDB initialization attempt ${attempts} failed, retrying...`
            );
            await new Promise((resolve) =>
              setTimeout(resolve, this.options.retryDelay)
            );
          }
        }

        // Initialize state
        this.data = null;
        this.filterService = null;
        this.initialColumns = null;
        this._isUndoing = false;
        this.dataInd = [];
        this.sortControllers = [];
        this.visControllers = [];
        this.compoundSorting = {};
        this.selectedRows = new Set();
        this.selectedColumn = null;
        this.history = [];
        this.ctrlDown = false;
        this.shiftDown = false;
        this.lastRowSelected = 0;
        this.defaultLines = 1000;
        this.lastLineAdded = -1;
        this.additionalLines = 500;
        this.addingRows = false;

        await this.initializeTable(data, this.normalizedColumns, options);
        return this;
      } catch (error) {
        if (this.duckDBProcessor) {
          await this.duckDBProcessor.cleanup().catch(console.error);
        }
        throw error;
      }
    })();
  }

  normalizeColumnDefinitions(columns) {
    return columns.map((col) => {
      if (typeof col === "string") return { column: col };
      if (typeof col === "object" && col?.column) return { ...col };
      throw new Error(`Invalid column definition: ${JSON.stringify(col)}`);
    });
  }

  createTableElement() {
    const table = document.createElement("table");
    table.classList.add("sorter-table");
    Object.assign(table.style, {
      width: "100%",
      borderCollapse: "collapse",
      fontFamily: "Arial, sans-serif",
      fontSize: "14px",
      userSelect: "none",
    });
    return table;
  }

  async initializeTable(data, columnDefinitions, options) {
    await this.duckDBProcessor.loadData(data, this.detectDataFormat(data));
    this.filterService = new FilterService(this.duckDBProcessor);

    this.data = await this.duckDBProcessor.query(
      `SELECT * FROM ${this.duckDBTableName} LIMIT ${this.options.rowsPerPage}`
    );
    if (!this.data?.length) throw new Error("No data loaded from DuckDB");

    const columnTypes = await Promise.all(
      columnDefinitions.map(async (colDef) => {
        const colName = colDef.column;

        // For unique columns, skip type inference and binning
        if (colDef.unique) {
          return {
            ...colDef,
            type: "unique",
            bins: [],
          };
        }

        // For non-unique columns, proceed with type inference and binning
        const type =
          colDef.type ||
          (await this.duckDBProcessor.getTypeFromDuckDB(colName));
        let bins = [];

        // Only create bins for non-unique columns that are continuous or ordinal
        if (!colDef.unique && (type === "continuous" || type === "ordinal")) {
          bins = await this.binningService.getBinningForColumn(
            colName,
            type,
            this.options.maxOrdinalBins || 20
          );
        }

        return { ...colDef, type, bins };
      })
    );

    this.dataInd = d3.range(this.data.length);
    this.columnManager = new ColumnManager(
      columnTypes,
      this.data,
      this.binningService
    );
    this.initialColumns = JSON.parse(
      JSON.stringify(this.columnManager.columns)
    );

    this.tableRenderer = new TableRenderer(
      this.columnManager.columns,
      this.data,
      options.cellRenderers,
      this.selectRow.bind(this),
      this.unselectRow.bind(this),
      this.getRowIndex.bind(this)
    );
    this.tableRenderer.setTable(this.table);

    await this.initializeVisualizations();
    this.createHeader();
    this.createTable();
  }

  detectDataFormat(data) {
    if (Array.isArray(data)) return "array";
    if (typeof data === "string") return "csv";
    throw new Error("Unsupported data format");
  }

  async initializeVisualizations() {
    this.visControllers = await Promise.all(
      this.columnManager.columns.map(async (col) => {
        let config;

        // Handle unique columns - skip binning entirely
        if (col.unique) {
          // Get the total count of rows for the unique column
          const countQuery = `SELECT COUNT(*) as total FROM ${this.duckDBTableName}`;
          const countResult = await this.duckDBProcessor.query(countQuery);
          config = {
            type: "unique",
            uniqueCount: countResult[0].total,
            columnName: col.column,
          };
        } else if (
          col.type === "continuous" ||
          col.type === "ordinal" ||
          col.type === "string"
        ) {
          try {
            const bins = await this.binningService.getBinningForColumn(
              col.column,
              col.type,
              this.options.maxOrdinalBins || 20
            );

            config = {
              type: col.type,
              bins: bins,
              columnName: col.column,
              tableName: this.duckDBTableName,
            };
          } catch (error) {
            console.error(`Error getting bins for ${col.column}:`, error);
            config = {
              type: col.type || "ordinal",
            };
          }
        } else {
          config = {
            type: col.type || "ordinal",
          };
        }

        const columnData = this.dataInd.map((i) => this.data[i][col.column]);
        const controller = new HistogramController(columnData, config);
        controller.table = this;
        controller.columnName = col.column;
        controller.duckDBProcessor = this.duckDBProcessor;

        return controller;
      })
    );
  }

  // Helper method to get unique count for a column
  async getUniqueCountForColumn(column) {
    try {
      const uniqueQuery = `
        SELECT COUNT(DISTINCT "${column}") as unique_count 
        FROM ${this.duckDBTableName}
      `;
      const uniqueResult = await this.duckDBProcessor.query(uniqueQuery);
      return uniqueResult[0].unique_count;
    } catch (error) {
      console.error(`Error getting unique count for ${column}:`, error);
      return 0;
    }
  }

  createHeader() {
    if (!this.tableRenderer.table) this.tableRenderer.setTable(this.table);
    if (this.tableRenderer.tHead) this.tableRenderer.tHead.remove();

    this.tableRenderer.tHead = document.createElement("thead");
    this.table.appendChild(this.tableRenderer.tHead);
    const headerRow = document.createElement("tr");
    this.tableRenderer.tHead.appendChild(headerRow);

    this.sortControllers = [];

    this.columnManager.columns.forEach((c, idx) => {
      const th = document.createElement("th");
      Object.assign(th.style, {
        textAlign: "center",
        padding: "8px",
        borderBottom: "1px solid #e0e0e0",
        background: "#f8f9fa",
        verticalAlign: "top",
      });
      headerRow.appendChild(th);

      // Column name
      const nameSpan = document.createElement("span");
      nameSpan.innerText = c.alias || c.column;
      Object.assign(nameSpan.style, {
        fontWeight: "600",
        cursor: "pointer",
        padding: "4px 8px",
        display: "block",
        color: "#2c3e50",
      });
      th.appendChild(nameSpan);

      // Event listeners for sorting and selection
      let longPressTimer;
      nameSpan.addEventListener("mousedown", (e) => {
        if (e.button === 0) {
          longPressTimer = setTimeout(() => this.selectColumn(c.column), 500);
        }
      });
      nameSpan.addEventListener("mouseup", () => clearTimeout(longPressTimer));
      nameSpan.addEventListener("click", (e) => {
        if (e.button === 0 && !e.defaultPrevented) {
          const sortCtrl = this.sortControllers[idx];
          sortCtrl.toggleDirection();
          this.sortChanged(sortCtrl);
        }
      });

      // Controls container
      const controlsDiv = document.createElement("div");
      Object.assign(controlsDiv.style, {
        display: "flex",
        justifyContent: "space-around",
        padding: "4px 0",
      });
      th.appendChild(controlsDiv);

      const shiftCtrl = new ColShiftController(
        c.column,
        this.shiftCol.bind(this)
      );
      controlsDiv.appendChild(shiftCtrl.getNode());

      const sortCtrl = new SortController(
        c.column,
        this.sortChanged.bind(this)
      );
      this.sortControllers.push(sortCtrl);
      controlsDiv.appendChild(sortCtrl.getNode());

      // Visualization
      const visDiv = document.createElement("div");
      th.appendChild(visDiv);
      visDiv.appendChild(this.visControllers[idx].getNode());
    });

    Object.assign(this.tableRenderer.tHead.style, {
      position: "sticky",
      top: "0",
      backgroundColor: "#ffffff",
      zIndex: "1",
    });
  }

  createTable() {
    if (!this.tableRenderer.table) this.tableRenderer.setTable(this.table);
    if (this.tableRenderer.tBody) this.tableRenderer.tBody.remove();

    this.tableRenderer.tBody = document.createElement("tbody");
    this.table.appendChild(this.tableRenderer.tBody);
    this.lastLineAdded = -1;
    this.addTableRows(this.defaultLines);
  }

  async addTableRows(howMany) {
    if (this.addingRows) return;
    this.addingRows = true;

    try {
      const offset = this.lastLineAdded + 1;
      const orderClause = Object.keys(this.compoundSorting).length
        ? `ORDER BY ${this.duckDBProcessor.safeColumnName(
            Object.keys(this.compoundSorting)[0]
          )} ${
            this.compoundSorting[Object.keys(this.compoundSorting)[0]].how ===
            "up"
              ? "ASC"
              : "DESC"
          }`
        : "";
      const query = `
        SELECT * FROM ${this.duckDBTableName}
        ${orderClause}
        LIMIT ${howMany} OFFSET ${offset}
      `;
      const newData = await this.duckDBProcessor.query(query);

      newData.forEach((rowData, i) => {
        const tr = document.createElement("tr");
        Object.assign(tr.style, { borderBottom: "1px solid #ddd" });
        this.tableRenderer.tBody.appendChild(tr);

        this.columnManager.columns.forEach((c) => {
          const td = document.createElement("td");
          td.innerText = rowData[c.column] ?? "";
          tr.appendChild(td);
        });

        tr.addEventListener("click", (e) => {
          if (this.shiftDown) {
            const start = Math.min(this.lastRowSelected, offset + i);
            const end = Math.max(this.lastRowSelected, offset + i);
            for (let j = start; j <= end; j++)
              this.selectRow(this.tableRenderer.tBody.children[j]);
          } else if (this.ctrlDown) {
            tr.selected ? this.unselectRow(tr) : this.selectRow(tr);
          } else {
            this.clearSelection();
            this.selectRow(tr);
          }
          this.lastRowSelected = offset + i;
          this.selectionUpdated();
        });
      });

      this.data.push(...newData);
      this.dataInd.push(...newData.map((_, i) => offset + i));
      this.lastLineAdded += newData.length;
    } finally {
      this.addingRows = false;
    }
  }

  async sortChanged(controller) {
    const colName = controller.getColumn();
    const how = controller.getDirection();
    this.compoundSorting = { [colName]: { how, order: 0 } };

    const query = `
      SELECT * FROM ${this.duckDBTableName}
      ORDER BY ${this.duckDBProcessor.safeColumnName(colName)} ${
      how === "up" ? "ASC" : "DESC"
    }
      LIMIT ${this.options.rowsPerPage}
    `;
    this.data = await this.duckDBProcessor.query(query);
    this.dataInd = d3.range(this.data.length);

    await Promise.all(
      this.visControllers.map((vc, i) => {
        const columnData = this.dataInd.map(
          (j) => this.data[j][this.columnManager.columns[i].column]
        );
        return vc.updateData(columnData);
      })
    );

    this.createTable();
    this.changed({
      type: "sort",
      sort: this.compoundSorting,
      indeces: this.dataInd,
    });
  }

  async filter() {
    const filterClause = await this.filterService.applyFilter(
      Array.from(this.selectedRows).map((i) => this.data[i]),
      Object.keys(this.compoundSorting)[0]
    );
    if (!filterClause) return;

    this.data = await this.duckDBProcessor.query(`
      SELECT * FROM ${this.duckDBTableName}
      WHERE ${filterClause}
      LIMIT ${this.options.rowsPerPage}
    `);
    this.dataInd = d3.range(this.data.length);

    await Promise.all(
      this.visControllers.map((vc, i) => {
        const columnData = this.dataInd.map(
          (j) => this.data[j][this.columnManager.columns[i].column]
        );
        return vc.updateData(columnData);
      })
    );

    this.createTable();
    this.changed({
      type: "filter",
      indeces: this.dataInd,
      rule: this.getSelectionRule(),
    });
  }

  shiftCol(columnName, dir) {
    const colIndex = this.columnManager.columns.findIndex(
      (c) => c.column === columnName
    );
    const targetIndex = dir === "left" ? colIndex - 1 : colIndex + 1;

    if (targetIndex >= 0 && targetIndex < this.columnManager.columns.length) {
      [
        this.columnManager.columns,
        this.visControllers,
        this.sortControllers,
      ].forEach((arr) => {
        const [item] = arr.splice(colIndex, 1);
        arr.splice(targetIndex, 0, item);
      });

      this.createHeader();
      this.createTable();
    }
  }

  selectRow(tr) {
    if (!tr || tr.selected) return;
    const rowIndex = this.getRowIndex(tr);
    tr.selected = true;
    tr.style.fontWeight = "bold";
    tr.style.color = "black";
    this.selectedRows.add(rowIndex);
  }

  unselectRow(tr) {
    tr.selected = false;
    tr.style.fontWeight = "normal";
    tr.style.color = "grey";
    this.selectedRows.delete(this.getRowIndex(tr));
  }

  getRowIndex(tr) {
    return Array.from(this.tableRenderer.tBody.children).indexOf(tr);
  }

  clearSelection() {
    this.selectedRows.clear();
    Array.from(this.tableRenderer.tBody.children).forEach((tr) => {
      tr.selected = false;
      tr.style.fontWeight = "normal";
      tr.style.color = "grey";
    });
    this.visControllers.forEach((vc) => vc.resetSelection?.());
  }

  selectionUpdated() {
    // Get all selected data
    const selectedData = Array.from(this.selectedRows).map((i) => this.data[i]);

    // Update each histogram visualization
    this.visControllers.forEach((vc) => {
      // Skip updating unique columns since they don't show data distributions
      if (vc.options.type === "unique") return;

      // Extract the relevant column data from the selection
      const selectedColumnData = selectedData
        .map((row) => row[vc.columnName])
        .filter((v) => v != null);

      // If there's no selection, highlight all data
      vc.highlightedData =
        this.selectedRows.size > 0
          ? selectedColumnData
          : this.data.map((row) => row[vc.columnName]);
      vc.render();
    });

    // Notify about selection change
    this.changed({
      type: "selection",
      indeces: Array.from(this.selectedRows),
      selection: this.getSelection(),
      rule: this.getSelectionRule(),
    });
  }

  getSelection() {
    return Array.from(this.selectedRows)
      .filter((i) => i >= 0 && i < this.data.length)
      .map((i) => ({ index: i, data: this.data[i] }));
  }

  getSelectionRule() {
    if (!this.selectedRows.size) return null;
    const rules = [];
    this.columnManager.columns.forEach((col) => {
      const values = new Set(
        [...this.selectedRows]
          .map((i) => this.data[i][col.column])
          .filter((v) => v != null)
      );
      if (values.size) {
        rules.push(
          col.type === "continuous"
            ? `${col.column} between ${d3.min(values)} and ${d3.max(values)}`
            : `${col.column} in (${Array.from(values).join(", ")})`
        );
      }
    });
    return rules.length ? rules : null;
  }

  selectColumn(columnName) {
    this.selectedColumn = columnName;
    this.tableRenderer.tHead.querySelectorAll("th").forEach((th, i) => {
      th.classList.toggle(
        "selected-column",
        this.columnManager.columns[i].column === columnName
      );
    });
    this.changed({ type: "columnSelection", selectedColumn: columnName });
  }

  handleHistogramSelection(selectedValues, sourceColumn) {
    // Clear previous selection in the table
    this.clearSelection();

    // Find rows that match the selected values in the source column
    const matchingRows = [];
    const sourceData = selectedValues;

    // For continuous data, handle range selection
    const isRange =
      selectedValues &&
      typeof selectedValues[0] === "object" &&
      "min" in selectedValues[0] &&
      "max" in selectedValues[0];

    for (let i = 0; i < this.data.length; i++) {
      const rowValue = this.data[i][sourceColumn];

      if (isRange) {
        const range = selectedValues[0];
        if (rowValue >= range.min && rowValue <= range.max) {
          matchingRows.push(i);
        }
      } else if (selectedValues.includes(rowValue)) {
        matchingRows.push(i);
      }
    }

    // Select matching rows in the table UI
    matchingRows.forEach((idx) => {
      const rowElement = this.tableRenderer.tBody.children[idx];
      if (rowElement) {
        this.selectRow(rowElement);
      }
      this.selectedRows.add(idx);
    });

    // Update all histogram visualizations with the selected data
    this.visControllers.forEach((controller) => {
      if (controller.columnName !== sourceColumn) {
        const selectedColumnData = matchingRows.map(
          (idx) => this.data[idx][controller.columnName]
        );
        controller.highlightedData = selectedColumnData;
        controller.render();
      }
    });

    // Notify about selection change
    this.selectionUpdated();
  }

  getNode() {
    const container = document.createElement("div");
    Object.assign(container.style, {
      height: this.options.containerHeight,
      width: this.options.containerWidth,
      overflow: "auto",
      display: "flex",
      flexDirection: "row",
    });

    const sidebar = document.createElement("div");
    Object.assign(sidebar.style, {
      width: "35px",
      padding: "5px",
      borderRight: "1px solid #ccc",
    });
    container.appendChild(sidebar);

    ["fa-filter", "fa-undo", "fa-sync-alt"].forEach((iconClass, i) => {
      const icon = document.createElement("i");
      icon.classList.add("fas", iconClass);
      Object.assign(icon.style, {
        cursor: "pointer",
        marginBottom: "15px",
        color: "gray",
      });
      icon.addEventListener(
        "click",
        [this.filter, this.undo, this.resetTable][i].bind(this)
      );
      sidebar.appendChild(icon);
    });

    const tableContainer = document.createElement("div");
    Object.assign(tableContainer.style, { flex: "1", overflowX: "auto" });
    tableContainer.appendChild(this.tableRenderer.getTable());
    container.appendChild(tableContainer);

    container.addEventListener("keydown", (e) => {
      this.shiftDown = e.shiftKey;
      this.ctrlDown = e.ctrlKey;
    });
    container.addEventListener("keyup", () => {
      this.shiftDown = this.ctrlDown = false;
    });
    container.setAttribute("tabindex", "0");

    container.addEventListener("scroll", () => {
      if (
        container.scrollTop + container.clientHeight >=
        container.scrollHeight - this.options.loadMoreThreshold
      ) {
        this.addTableRows(this.additionalLines);
      }
    });

    container.addEventListener("click", (e) => {
      if (!e.target.closest("tr")) {
        this.clearSelection();
        this.selectionUpdated();
      }
    });

    return container;
  }

  undo() {
    const action = this.history.pop();
    if (!action) return;

    if (action.type === "shiftcol") {
      this._isUndoing = true;
      this.shiftCol(
        action.columnName,
        action.dir === "left" ? "right" : "left"
      );
      this._isUndoing = false;
    }
  }

  resetTable() {
    this.dataInd = d3.range(this.data.length);
    this.selectedRows.clear();
    this.compoundSorting = {};
    this.history = [];
    this.columnManager.columns = JSON.parse(
      JSON.stringify(this.initialColumns)
    );
    this.createHeader();
    this.createTable();
    this.changed({ type: "reset" });
  }

  async destroy() {
    await this.duckDBProcessor?.dropTable();
    await this.duckDBProcessor?.close();
    await this.duckDBProcessor?.terminate();
  }
}
