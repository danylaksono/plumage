import * as d3 from "npm:d3";
import { ColumnManager } from "./ColumnManager.js";
import { TableRenderer } from "./TableRenderer.js";
import { SortController } from "./SortController.js";
import { ColShiftController } from "./ColShiftController.js";
import { HistogramController } from "./HistogramController.js";
import { BinningService } from "./BinningService.js";
import { FilterService } from "./FilterService.js"; // Import FilterService
import { DuckDBDataProcessor } from "../_base/duckdb-processor.js"; // Add this import

export class SorterTable {
  constructor(data, columnNames, changed, options = {}) {
    if (new.target) {
      // This ensures proper async initialization when using 'new'
      return (async () => {
        this.changed = changed;
        this.options = {
          containerHeight: options.height || "400px",
          containerWidth: options.width || "100%",
          rowsPerPage: options.rowsPerPage || 50,
          loadMoreThreshold: options.loadMoreThreshold || 100,
        };

        // Create table element first
        this.table = this.createTableElement();

        this.duckDBTableName = "main_table";
        this.duckDBProcessor = null;
        this.data = null;
        this.binningService = new BinningService();
        this.initialColumns = null;
        this._isUndoing = false;
        this.dataInd = [];
        this.sortControllers = [];
        this.visControllers = [];
        this.compoundSorting = {};
        this.selected = [];
        this.selectedColumn = null;
        this.history = [];
        this.ctrlDown = false;
        this.shiftDown = false;
        this.lastRowSelected = 0;
        this.defaultLines = 1000;
        this.lastLineAdded = 0;
        this.additionalLines = 500;
        this.addingRows = false;
        this.rules = [];
        this.selectedRows = new Set();
        this.percentiles = [
          0, 0.01, 0.02, 0.03, 0.04, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7,
          0.8, 0.9, 0.95, 0.96, 0.97, 0.98, 0.99, 1,
        ];

        this.showDefaultControls =
          options.showDefaultControls !== undefined
            ? options.showDefaultControls
            : true;

        // Await the initialization
        await this.initializeTable(data, columnNames, options);

        return this;
      })();
    }
    throw new Error("SorterTable must be instantiated with new");
  }

  async initializeTable(data, columnNames, options) {
    try {
      // Initialize DuckDB processor
      this.duckDBProcessor = new DuckDBDataProcessor(
        null,
        this.duckDBTableName
      );
      await this.duckDBProcessor.connect();

      // Load data and get column types
      await this.duckDBProcessor.loadData(data, this.detectDataFormat(data));

      // Get column types from DuckDB
      const columnTypes = await Promise.all(
        columnNames.map(async (col) => {
          const type = await this.duckDBProcessor.getTypeFromDuckDB(col);
          return { column: col, type };
        })
      );

      // Get initial data with proper limit
      this.data = await this.duckDBProcessor.query(
        `SELECT * FROM ${this.duckDBTableName} LIMIT ${this.options.rowsPerPage}`
      );

      if (!this.data || this.data.length === 0) {
        throw new Error("No data loaded from DuckDB");
      }

      this.dataInd = d3.range(this.data.length);

      // Initialize column manager with type information
      this.columnManager = new ColumnManager(
        columnTypes,
        this.data,
        this.binningService,
        options.maxOrdinalBins,
        options.continuousBinMethod
      );

      if (!this.columnManager || !this.columnManager.columns) {
        throw new Error("Column manager initialization failed");
      }

      // Store initial column state after columnManager is initialized
      this.initialColumns = JSON.parse(
        JSON.stringify(this.columnManager.columns)
      );

      // Initialize table renderer
      this.tableRenderer = new TableRenderer(
        this.columnManager.columns,
        this.preprocessData(this.data, columnNames),
        this.options.cellRenderers,
        this.selectRow.bind(this),
        this.unselectRow.bind(this),
        this.getRowIndex.bind(this)
      );
      this.tableRenderer.table = this.table; // Assign the table element to the renderer

      this.filterService = new FilterService(this.data);

      // Create initial table structure
      this.createHeader();
      this.createTable();
    } catch (error) {
      console.error("Failed to initialize table:", error);
      throw error;
    }
  }

  detectDataFormat(data) {
    if (Array.isArray(data)) return "json";
    if (typeof data === "string") {
      const extension = data.split(".").pop().toLowerCase();
      if (extension === "csv") return "csv";
      if (extension === "parquet") return "parquet";
    }
    throw new Error("Unsupported data format");
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

  preprocessData(data, columnNames) {
    return data.map((row) => {
      const processed = { ...row };
      columnNames.forEach((col) => {
        const colName = typeof col === "string" ? col : col.column;
        const colType = typeof col === "string" ? null : col.type;

        // Handle continuous columns
        if (colType === "continuous") {
          const value = row[colName];

          // Handle different value cases
          if (value === null || value === undefined || value === "") {
            // FALLBACK VALUE FOR NUMERICAL COLUMNS
            processed[colName] = 0;
          } else if (typeof value === "string") {
            // Clean string numbers
            const cleaned = value.replace(/[^0-9.-]/g, "");
            if (cleaned === "" || isNaN(Number(cleaned))) {
              processed[colName] = 0; // Fallback for invalid numbers
            } else {
              processed[colName] = Number(cleaned);
            }
          } else if (typeof value === "number") {
            if (isNaN(value)) {
              processed[colName] = 0; // Handle NaN
            } else {
              processed[colName] = value; // Keep valid numbers as-is
            }
          } else {
            // Handle any other unexpected types
            processed[colName] = 0;
          }

          // Log problematic values for debugging
          if (processed[colName] === 0 && value !== 0) {
            // DEBUG
            // console.warn(`Converted invalid value in ${colName}:`, {
            //   original: value,
            //   converted: processed[colName],
            //   rowData: row,
            // });
          }
        }
      });
      return processed;
    });
  }

  // Shift column position using visController
  shiftCol(columnName, dir) {
    // console.log("Shifting column:", columnName, "direction:", dir);

    let colIndex = this.columnManager.columns.findIndex(
      (c) => c.column === columnName
    );
    // console.log("Found column at index:", colIndex);

    const targetIndex = dir === "left" ? colIndex - 1 : colIndex + 1;
    // console.log("Target index:", targetIndex);

    if (targetIndex >= 0 && targetIndex < this.columnManager.columns.length) {
      if (!this._isUndoing) {
        this.history.push({
          type: "shiftcol",
          columnName: columnName,
          dir: dir,
          fromIndex: colIndex,
          toIndex: targetIndex,
        });
      }

      // Store the elements to be moved
      const columnsToMove = {
        column: this.columnManager.columns[colIndex],
        visController: this.visControllers[colIndex],
        sortController: this.sortControllers[colIndex],
      };

      // Remove elements from original positions
      this.columnManager.columns.splice(colIndex, 1);
      this.visControllers.splice(colIndex, 1);
      this.sortControllers.splice(colIndex, 1);

      // Insert elements at new positions
      this.columnManager.columns.splice(targetIndex, 0, columnsToMove.column);
      this.visControllers.splice(targetIndex, 0, columnsToMove.visController);
      this.sortControllers.splice(targetIndex, 0, columnsToMove.sortController);

      // Recreate the table and header
      // this.rebuildTable();
      this.createHeader();
      this.createTable();

      // Update data in visualization controllers
      this.visControllers.forEach((vc, idx) => {
        if (vc && vc.updateData && this.columnManager.columns[idx]) {
          const columnData = this.dataInd.map(
            (i) => this.data[i][this.columnManager.columns[idx].column]
          );
          vc.updateData(columnData);
        }
      });
    }
  }

  async filter() {
    try {
      const filterClause = this.buildFilterClause();

      // Get total filtered count
      const countQuery = `
        SELECT COUNT(*) as total 
        FROM ${this.duckDBTableName}
        WHERE ${filterClause}
      `;
      const countResult = await this.duckDBProcessor.query(countQuery);
      const totalFiltered = countResult[0].total;

      // Get filtered data with pagination
      const query = `
        SELECT *, ROWID
        FROM ${this.duckDBTableName}
        WHERE ${filterClause}
        LIMIT ${this.options.rowsPerPage}
      `;

      const filteredData = await this.duckDBProcessor.query(query);

      // Update data and indices
      this.data = filteredData;
      this.dataInd = filteredData.map((row) => row.ROWID);

      // Save filter state in history
      this.history.push({
        type: "filter",
        data: [...this.dataInd],
        totalFiltered,
      });

      // Update table
      this.createTable();

      // Update visualizations with filtered data
      this.visControllers.forEach((vc, vci) => {
        const columnName = this.columnManager.columns[vci].column;
        vc.setData(filteredData.map((row) => row[columnName]));
      });

      // Notify about filter change
      this.changed({
        type: "filter",
        indeces: this.dataInd,
        rule: this.getSelectionRule(),
        totalFiltered,
      });
    } catch (error) {
      console.error("Filtering failed:", error);
      throw error;
    }
  }

  async applyCustomFilter(filterFunction) {
    // Convert the filter function to a SQL WHERE clause
    const whereClause = this.convertFilterFunctionToWhereClause(filterFunction);

    // Construct SQL query with the WHERE clause
    const sqlQuery = `SELECT ROWID FROM ${this.duckDBTableName} WHERE ${whereClause}`;

    // Execute the query and get the filtered indices
    const filteredResult = await this.duckDBClient.query(sqlQuery);
    this.dataInd = filteredResult.map((row) => row.ROWID);

    // Re-render the table and update visualizations
    this.rebuildTable();
    this.visControllers.forEach((vc) => {
      if (vc && vc.updateData) {
        vc.updateData(this.dataInd.map((i) => this.data[i][vc.columnName]));
      }
    });

    // Notify about the filter change
    this.changed({ type: "customFilter", indices: this.dataInd });
  }

  // Helper function to convert a filter function to a SQL WHERE clause
  convertFilterFunctionToWhereClause(filterFunction) {
    // This is a placeholder implementation. You will need to implement
    // the actual conversion logic based on the structure of your data
    // and the filter function.
    // Example:
    // if (filterFunction === (row) => row.age > 18) {
    //   return "age > 18";
    // }
    // else if (filterFunction === (row) => row.city === "New York") {
    //   return "city = 'New York'";
    // }
    // ...
    // For more complex filter functions, you may need to use a parser
    // to analyze the function and generate the corresponding SQL WHERE clause.
    return "1=1"; // Default: return all rows
  }

  getAllRules() {
    return this.rules;
  }

  undo() {
    if (this.history.length > 0) {
      let u = this.history.pop();
      if (u.type === "filter" || u.type === "sort") {
        this.dataInd = u.data;
        this.createTable();
        this.visControllers.forEach((vc, vci) =>
          vc.updateData(
            this.dataInd.map(
              (i) => this.data[i][this.columnManager.columns[vci].column]
            )
          )
        );
        this.changed({
          type: "undo",
          indeces: this.dataInd,
          sort: this.compoundSorting,
        });
      } else if (u.type === "shiftcol") {
        this._isUndoing = true;
        const reverseDir = u.dir === "left" ? "right" : "left";
        this.shiftCol(u.columnName, reverseDir);
        this._isUndoing = false;
      }
    }
  }

  rebuildTable() {
    this.createHeader();
    this.createTable();
  }

  getSelection() {
    let ret = [];
    this.selectedRows.forEach((index) => {
      if (index >= 0 && index < this.dataInd.length) {
        ret.push({
          index: index,
          data: this.data[this.dataInd[index]],
        });
      }
    });
    // console.log("Selection result:", ret);
    this.selected = ret;
    return ret;
  }

  getSelectionRule() {
    let sel = this.getSelection();
    let sortKeys = Object.keys(this.compoundSorting);

    if (sortKeys.length === 0) {
      return null;
    } else {
      let col = sortKeys[0];
      let firstIndex = sel[sel.length - 1].index;
      let lastIndex = sel[sel.length - 1].index;

      if ((firstIndex = 0 && lastIndex == this.dataInd.length - 1)) return [];
      else {
        let rule = [];
        let r = "";
        if (
          firstIndex > 0 &&
          this.data[this.dataInd[firstIndex - 1]][col] !=
            this.data[this.dataInd[firstIndex]][col]
        ) {
          r =
            col +
            (this.compoundSorting[col].how === "up"
              ? " lower than "
              : " higher than ") +
            this.data[this.dataInd[firstIndex]][col];
        }
        if (
          lastIndex < this.dataInd.length - 1 &&
          this.data[this.dataInd[lastIndex + 1]][col] !=
            this.data[this.dataInd[lastIndex]][col]
        ) {
          if (r.length == 0)
            r =
              col +
              (this.compoundSorting[col].how === "up"
                ? " lower than "
                : " higher than ") +
              this.data[this.dataInd[lastIndex]][col];
          else
            r =
              r +
              (this.compoundSorting[col].how === "up"
                ? " and lower than"
                : "  and higher than ") +
              this.data[this.dataInd[lastIndex]][col];
        }
        if (r.length > 0) rule.push(r);

        if (this.compoundSorting[col].how === "up")
          r =
            col +
            " in bottom " +
            this.percentalize(lastIndex / this.data.length, "top") +
            " percentile";
        else
          r =
            col +
            " in top " +
            this.percentalize(1 - lastIndex / this.data.length, "bottom") +
            " percentile";
        rule.push(r);

        return rule;
      }
    }
  }

  selectionUpdated() {
    this.changed({
      type: "selection",
      indeces: this.dataInd,
      selection: this.getSelection(),
      rule: this.getSelectionRule(),
    });
  }

  clearSelection() {
    this.selectedRows.clear(); // Clear the Set of selected rows
    // Also, visually deselect all rows in the table
    if (this.tableRenderer.tBody) {
      this.tableRenderer.tBody.querySelectorAll("tr").forEach((tr) => {
        this.unselectRow(tr);
        tr.selected = false;
        tr.style.fontWeight = "normal";
        tr.style.color = "grey";
      });
    }
    // if (this.tBody != null)
    //   this.tBody.querySelectorAll("tr").forEach((tr) => this.unselectRow(tr));
  }

  selectColumn(columnName) {
    console.log("Selected column:", columnName);
    this.selectedColumn = columnName;

    this.tableRenderer.tHead.querySelectorAll("th").forEach((th) => {
      th.classList.remove("selected-column"); // Remove previous selection
    });

    const columnIndex = this.columnManager.columns.findIndex(
      (c) => c.column === columnName
    );
    if (columnIndex !== -1) {
      this.tableRenderer.tHead
        .querySelectorAll("th")
        [columnIndex].classList.add("selected-column");
    }
    this.changed({
      type: "columnSelection",
      selectedColumn: this.selectedColumn,
    });
  }

  selectRow(tr) {
    tr.selected = true;
    tr.style.fontWeight = "bold";
    tr.style.color = "black";
    this.selectedRows.add(this.getRowIndex(tr));
  }

  unselectRow(tr) {
    tr.selected = false;
    tr.style.fontWeight = "normal";
    tr.style.color = "grey";
    this.selectedRows.delete(this.getRowIndex(tr));
  }

  getRowIndex(tr) {
    let index = -1;
    this.tableRenderer.tBody.querySelectorAll("tr").forEach((t, i) => {
      if (t == tr) index = i;
    });
    return index;
  }

  createHeader() {
    // Ensure TableRenderer has the table reference
    if (!this.tableRenderer.table) {
      this.tableRenderer.setTable(this.table);
    }

    if (this.tableRenderer.tHead != null) {
      this.table.removeChild(this.tableRenderer.tHead);
    }

    this.sortControllers = [];
    this.visControllers = [];

    this.tableRenderer.tHead = document.createElement("thead");
    this.table.appendChild(this.tableRenderer.tHead);

    // --- Column Header Row ---
    let headerRow = document.createElement("tr");
    this.tableRenderer.tHead.append(headerRow);

    this.columnManager.columns.forEach((c) => {
      let th = document.createElement("th");
      headerRow.appendChild(th);
      Object.assign(th.style, {
        textAlign: "center",
        padding: "8px",
        borderBottom: "1px solid #e0e0e0",
        background: "#f8f9fa",
      });

      // --- Column Name ---
      let nameSpan = document.createElement("span");
      nameSpan.innerText = c.alias || c.column;
      Object.assign(nameSpan.style, {
        fontWeight: "600",
        fontFamily: "Arial, sans-serif",
        fontSize: "0.9em",
        cursor: "pointer",
        userSelect: "none",
        padding: "4px 8px",
        display: "block",
        color: "#2c3e50",
        transition: "background-color 0.2s",
      });

      // Add long press event listener
      let longPressTimer;
      let isLongPress = false;
      nameSpan.addEventListener("mousedown", (event) => {
        // Check if the left mouse button was pressed
        if (event.button === 0) {
          isLongPress = false; // Reset long press flag
          longPressTimer = setTimeout(() => {
            isLongPress = true;
            this.selectColumn(c.column); // Select the column
          }, 500); // Adjust the timeout (in milliseconds) as needed
        }
      });

      nameSpan.addEventListener("mouseup", () => {
        clearTimeout(longPressTimer);
      });

      // Prevent context menu on long press
      nameSpan.addEventListener("contextmenu", (event) => {
        event.preventDefault();
      });

      nameSpan.addEventListener("click", () => {
        if (!isLongPress) {
          const sortCtrl = this.sortControllers.find(
            (ctrl) => ctrl.getColumn() === c.column
          );
          if (sortCtrl) {
            sortCtrl.toggleDirection();
            this.sortChanged(sortCtrl);
          }
        }
      });

      nameSpan.addEventListener("mouseover", () => {
        nameSpan.style.backgroundColor = "#edf2f7";
      });
      nameSpan.addEventListener("mouseout", () => {
        nameSpan.style.backgroundColor = "transparent";
      });

      th.appendChild(nameSpan);

      // --- Controls Row ---
      let controlsRow = document.createElement("tr");
      th.appendChild(controlsRow); // Append controls row to the header cell (th)

      let controlsTd = document.createElement("td");
      controlsRow.appendChild(controlsTd);

      // Create a container for controls
      let controlsContainer = document.createElement("div");
      Object.assign(controlsContainer.style, {
        display: "flex",
        alignItems: "center",
        justifyContent: "space-around",
        padding: "6px 0",
        borderBottom: "1px solid #eaeaea",
        background: "#ffffff",
      });
      controlsTd.appendChild(controlsContainer);

      // Shift controller cell
      const shiftCtrl = new ColShiftController(
        c.column,
        (columnName, direction) => this.shiftCol(columnName, direction)
      );
      controlsContainer.appendChild(shiftCtrl.getNode());

      // Sort controller cell
      let sortCtrl = new SortController(c.column, (controller) =>
        this.sortChanged(controller)
      );
      this.sortControllers.push(sortCtrl);
      controlsContainer.appendChild(sortCtrl.getNode());

      // --- Visualization Row ---
      let visRow = document.createElement("tr");
      th.appendChild(visRow); // Append visualization row to the header cell (th)

      let visTd = document.createElement("td");
      visRow.appendChild(visTd);

      if (c.unique) {
        // For unique columns, create a histogram with a single bin
        let uniqueData = this.dataInd.map((i) => this.data[i][c.column]);
        const uniqueBinning = [
          { x0: "Unique", x1: "Unique", values: uniqueData },
        ];
        // let visCtrl = new HistogramController(uniqueData, uniqueBinning); // { unique: true });
        let visCtrl = new HistogramController(uniqueData, { unique: true });
        visCtrl.table = this;
        visCtrl.columnName = c.column;
        this.visControllers.push(visCtrl);
        visTd.appendChild(visCtrl.getNode());
      } else {
        // Create and add visualization controller (histogram) for non-unique columns
        console.log(" >>>> Creating histogram for column:", c);
        let visCtrl = new HistogramController(
          this.dataInd.map((i) => this.data[i][c.column]),
          c.type === "continuous"
            ? { thresholds: c.thresholds, binInfo: c.bins }
            : { nominals: c.nominals }
          // this.getColumnType(c.column) === "continuous"
          //   ? { thresholds: c.thresholds }
          //   : { nominals: c.nominals }
        );
        visCtrl.table = this;
        visCtrl.columnName = c.column;
        this.visControllers.push(visCtrl);
        visTd.appendChild(visCtrl.getNode());
      }
    });

    // Add sticky positioning to thead
    this.tableRenderer.tHead.style.position = "sticky";
    this.tableRenderer.tHead.style.top = "0";
    this.tableRenderer.tHead.style.backgroundColor = "#ffffff"; // Ensure header is opaque
    this.tableRenderer.tHead.style.zIndex = "1"; // Keep header above table content
    this.tableRenderer.tHead.style.boxShadow = "0 2px 2px rgba(0,0,0,0.1)";
  }

  createTable() {
    // Ensure TableRenderer has the table reference
    if (!this.tableRenderer.table) {
      this.tableRenderer.setTable(this.table);
    }

    if (this.tableRenderer.tBody != null)
      this.table.removeChild(this.tableRenderer.tBody);

    this.tableRenderer.tBody = document.createElement("tbody");
    this.table.appendChild(this.tableRenderer.tBody);

    this.lastLineAdded = -1;
    this.addTableRows(this.defaultLines);
  }

  async addTableRows(howMany) {
    if (this.addingRows) {
      return;
    }
    this.addingRows = true;

    try {
      let min = this.lastLineAdded + 1;
      let max = Math.min(min + howMany - 1, this.dataInd.length - 1);

      // Fetch the next batch of data from DuckDB
      const query = `
        SELECT * 
        FROM ${this.duckDBTableName}
        LIMIT ${howMany} 
        OFFSET ${min}
      `;

      const newData = await this.duckDBProcessor.query(query);

      // Extend the data array with new records
      this.data.push(...newData);

      for (let row = min; row <= max; row++) {
        let dataIndex = this.dataInd[row];
        if (dataIndex === undefined) continue;

        let tr = document.createElement("tr");
        tr.selected = false;
        Object.assign(tr.style, {
          color: "grey",
          borderBottom: "1px solid #ddd",
        });
        this.tableRenderer.tBody.appendChild(tr);

        this.columnManager.columns.forEach((c) => {
          let td = document.createElement("td");
          if (
            typeof this.tableRenderer.cellRenderers[c.column] === "function"
          ) {
            td.innerHTML = "";
            td.appendChild(
              this.tableRenderer.cellRenderers[c.column](
                this.data[dataIndex][c.column],
                this.data[dataIndex]
              )
            );
          } else {
            td.innerText = this.data[dataIndex][c.column];
          }

          tr.appendChild(td);
          td.style.color = "inherit";
          td.style.fontWidth = "inherit";
        });

        // ...existing code for event listeners...
      }

      this.lastLineAdded = max;
    } catch (error) {
      console.error("Error loading more data:", error);
    } finally {
      this.addingRows = false;
    }
  }

  resetTable() {
    // Reset data and indices to initial state
    this.dataInd = d3.range(this.data.length);
    this.selectedRows.clear();
    this.compoundSorting = {};
    this.rules = [];
    this.history = [];

    // Reset sort and shift controllers
    // this.sortControllers.forEach((ctrl) => ctrl.setDirection("none"));
    this.sortControllers.forEach((ctrl) => ctrl.toggleDirection()); // Toggle direction to reset

    // Update column order to the initial state
    this.columnManager.columns = this.initialColumns.map((col) => ({
      ...col,
    }));

    // Update vis controllers
    this.visControllers.forEach((vc, index) => {
      const columnData = this.dataInd.map(
        (i) => this.data[i][this.columnManager.columns[index].column]
      );
      vc.updateData(columnData);
    });

    // Re-render the table
    this.createHeader();
    this.createTable();

    // Notify about the reset
    this.changed({ type: "reset" });
  }

  resetHistogramSelections() {
    this.visControllers.forEach((vc) => {
      if (vc instanceof HistogramController) {
        vc.resetSelection();
      }
    });
  }

  async sortChanged(controller) {
    this.history.push({ type: "sort", data: [...this.dataInd] });
    this.compoundSorting = {};

    const col = controller.getColumn();
    const how = controller.getDirection();
    const sortDirection = how === "up" ? "ASC" : "DESC";

    try {
      // Get total count first
      const countQuery = `SELECT COUNT(*) as total FROM ${this.duckDBTableName}`;
      const countResult = await this.duckDBProcessor.query(countQuery);
      const totalRows = countResult[0].total;

      // Query with ROWID to maintain consistency
      const query = `
        SELECT *, ROWID 
        FROM ${this.duckDBTableName}
        ORDER BY ${col} ${sortDirection}
        LIMIT ${this.options.rowsPerPage}
      `;

      const sortedResult = await this.duckDBProcessor.query(query);

      // Update data with sorted results
      this.data = sortedResult;

      // Create new index mapping based on ROWIDs
      this.dataInd = sortedResult.map((row) => row.ROWID);

      // Update visualizations with the new order
      this.visControllers.forEach((vc, index) => {
        const columnName = this.columnManager.columns[index].column;
        const columnData = sortedResult.map((row) => row[columnName]);
        vc.setData(columnData);
      });

      // Store sort state for compound sorting
      this.compoundSorting[col] = {
        how,
        order: Object.keys(this.compoundSorting).length,
      };

      // Recreate table with new data
      this.lastLineAdded = -1;
      this.createTable();

      // Notify about the sort change
      this.changed({
        type: "sort",
        sort: this.compoundSorting,
        indeces: this.dataInd,
        totalRows,
      });
    } catch (error) {
      console.error("Sorting failed:", error);
    }
  }

  percentalize(v, dir = "top") {
    if (dir === "bottom") {
      for (let i = 1; i < this.percentiles.length; i++) {
        if (v >= this.percentiles[i - 1] && v <= this.percentiles[i]) {
          return 100 * this.percentiles[i - 1];
        }
      }
    } else if (dir === "top") {
      for (let i = 1; i < this.percentiles.length; i++) {
        if (v >= this.percentiles[i - 1] && v <= this.percentiles[i])
          return 100 * this.percentiles[i];
      }
    } else return -1;
  }

  getNode() {
    // Ensure TableRenderer has the table reference before creating container
    if (!this.tableRenderer.table) {
      this.tableRenderer.setTable(this.table);
    }

    let container = document.createElement("div");
    Object.assign(container.style, {
      height: this.options.containerHeight,
      width: this.options.containerWidth,
      overflow: "auto",
      position: "relative",
      display: "flex",
      flexDirection: "row",
    });

    // --- Sidebar ---
    let sidebar = document.createElement("div");
    Object.assign(sidebar.style, {
      display: "flex",
      flexDirection: "column",
      alignItems: "center",
      width: "35px",
      padding: "5px",
      borderRight: "1px solid #ccc",
      marginRight: "2px",
    });

    // --- Filter Icon ---
    let filterIcon = document.createElement("i");
    filterIcon.classList.add("fas", "fa-filter");
    Object.assign(filterIcon.style, {
      cursor: "pointer",
      marginBottom: "15px",
      color: "gray",
    });
    filterIcon.setAttribute("title", "Apply Filter");
    filterIcon.addEventListener("click", (event) => {
      event.stopPropagation();
      this.filter();
    });
    sidebar.appendChild(filterIcon);

    // --- Undo Icon ---
    let undoIcon = document.createElement("i");
    undoIcon.classList.add("fas", "fa-undo");
    Object.assign(undoIcon.style, {
      cursor: "pointer",
      marginBottom: "15px",
      color: "gray",
    });
    undoIcon.setAttribute("title", "Undo");
    undoIcon.addEventListener("click", (event) => {
      event.stopPropagation();
      this.undo();
    });
    sidebar.appendChild(undoIcon);

    // --- Reset Icon ---
    let resetIcon = document.createElement("i");
    resetIcon.classList.add("fas", "fa-sync-alt");
    Object.assign(resetIcon.style, {
      cursor: "pointer",
      marginBottom: "15px",
      color: "gray",
    });
    resetIcon.setAttribute("title", "Reset Table");
    resetIcon.addEventListener("click", (event) => {
      event.stopPropagation();
      this.resetTable();
    });
    sidebar.appendChild(resetIcon);

    // --- Table Container ---
    let tableContainer = document.createElement("div");
    Object.assign(tableContainer.style, {
      flex: "1",
      overflowX: "auto",
    });

    // Set table width
    if (this.tableWidth) {
      this.table.style.width = this.tableWidth;
    } else {
      this.table.style.width = "100%";
    }

    // Ensure table is properly initialized in renderer before adding to container
    const table = this.tableRenderer.getTable() || this.table;
    tableContainer.appendChild(table);

    // Add components to container
    container.appendChild(sidebar);
    container.appendChild(tableContainer);

    // Event listeners for shift and ctrl keys
    container.addEventListener("keydown", (event) => {
      if (event.shiftKey) {
        this.shiftDown = true;
      }
      if (event.ctrlKey) {
        this.ctrlDown = true;
      }
      event.preventDefault();
    });

    container.addEventListener("keyup", (event) => {
      this.shiftDown = false;
      this.ctrlDown = false;
      event.preventDefault();
    });

    container.setAttribute("tabindex", "0"); // Make the container focusable

    // Lazy loading listener
    container.addEventListener("scroll", () => {
      const threshold = 100;
      const scrollTop = container.scrollTop;
      const scrollHeight = container.scrollHeight;
      const clientHeight = container.clientHeight;

      if (scrollTop + clientHeight >= scrollHeight - threshold) {
        if (!this.addingRows) {
          this.addTableRows(this.additionalLines);
        }
      }
    });

    //deselection
    // Add click listener to the container
    container.addEventListener("click", (event) => {
      // Check if the click target is outside any table row
      let isOutsideRow = true;
      let element = event.target;
      while (element != null) {
        if (
          element == this.tableRenderer.tBody ||
          element == this.tableRenderer.tHead
        ) {
          isOutsideRow = false;
          break;
        }
        element = element.parentNode;
      }

      if (isOutsideRow) {
        this.clearSelection();
        this.resetHistogramSelections();
        this.selectionUpdated();
      }
    });

    return container;
  }

  getNode() {
    // Ensure TableRenderer has the table reference before creating container
    if (!this.tableRenderer.table) {
      this.tableRenderer.setTable(this.table);
    }

    let container = document.createElement("div");
    Object.assign(container.style, {
      height: this.options.containerHeight,
      width: this.options.containerWidth,
      overflow: "auto",
      position: "relative",
      display: "flex",
      flexDirection: "row",
    });

    // --- Sidebar ---
    let sidebar = document.createElement("div");
    Object.assign(sidebar.style, {
      display: "flex",
      flexDirection: "column",
      alignItems: "center",
      width: "35px",
      padding: "5px",
      borderRight: "1px solid #ccc",
      marginRight: "2px",
    });

    // --- Filter Icon ---
    let filterIcon = document.createElement("i");
    filterIcon.classList.add("fas", "fa-filter");
    Object.assign(filterIcon.style, {
      cursor: "pointer",
      marginBottom: "15px",
      color: "gray",
    });
    filterIcon.setAttribute("title", "Apply Filter");
    filterIcon.addEventListener("click", (event) => {
      event.stopPropagation();
      this.filter();
    });
    sidebar.appendChild(filterIcon);

    // --- Undo Icon ---
    let undoIcon = document.createElement("i");
    undoIcon.classList.add("fas", "fa-undo");
    Object.assign(undoIcon.style, {
      cursor: "pointer",
      marginBottom: "15px",
      color: "gray",
    });
    undoIcon.setAttribute("title", "Undo");
    undoIcon.addEventListener("click", (event) => {
      event.stopPropagation();
      this.undo();
    });
    sidebar.appendChild(undoIcon);

    // --- Reset Icon ---
    let resetIcon = document.createElement("i");
    resetIcon.classList.add("fas", "fa-sync-alt");
    Object.assign(resetIcon.style, {
      cursor: "pointer",
      marginBottom: "15px",
      color: "gray",
    });
    resetIcon.setAttribute("title", "Reset Table");
    resetIcon.addEventListener("click", (event) => {
      event.stopPropagation();
      this.resetTable();
    });
    sidebar.appendChild(resetIcon);

    // --- Table Container ---
    let tableContainer = document.createElement("div");
    Object.assign(tableContainer.style, {
      flex: "1",
      overflowX: "auto",
    });

    // Set table width
    if (this.tableWidth) {
      this.table.style.width = this.tableWidth;
    } else {
      this.table.style.width = "100%";
    }

    // Ensure table is properly initialized in renderer before adding to container
    const table = this.tableRenderer.getTable() || this.table;
    tableContainer.appendChild(table);

    // Add components to container
    container.appendChild(sidebar);
    container.appendChild(tableContainer);

    // Event listeners for shift and ctrl keys
    container.addEventListener("keydown", (event) => {
      if (event.shiftKey) {
        this.shiftDown = true;
      }
      if (event.ctrlKey) {
        this.ctrlDown = true;
      }
      event.preventDefault();
    });

    container.addEventListener("keyup", (event) => {
      this.shiftDown = false;
      this.ctrlDown = false;
      event.preventDefault();
    });

    container.setAttribute("tabindex", "0"); // Make the container focusable

    // Lazy loading listener
    container.addEventListener("scroll", () => {
      const threshold = 100;
      const scrollTop = container.scrollTop;
      const scrollHeight = container.scrollHeight;
      const clientHeight = container.clientHeight;

      if (scrollTop + clientHeight >= scrollHeight - threshold) {
        if (!this.addingRows) {
          this.addTableRows(this.additionalLines);
        }
      }
    });

    //deselection
    // Add click listener to the container
    container.addEventListener("click", (event) => {
      // Check if the click target is outside any table row
      let isOutsideRow = true;
      let element = event.target;
      while (element != null) {
        if (
          element == this.tableRenderer.tBody ||
          element == this.tableRenderer.tHead
        ) {
          isOutsideRow = false;
          break;
        }
        element = element.parentNode;
      }

      if (isOutsideRow) {
        this.clearSelection();
        this.resetHistogramSelections();
        this.selectionUpdated();
      }
    });

    return container;
  }

  // Add this helper method for building filter clause
  buildFilterClause() {
    if (this.selectedRows.size === 0) return "1=1";

    // Get unique column values for selected rows
    const selectedIndices = Array.from(this.selectedRows);
    const selectedData = selectedIndices.map((i) => this.data[i]);

    // Build filter conditions based on selection
    const conditions = [];

    // Check if we have a compound sort active
    if (Object.keys(this.compoundSorting).length > 0) {
      const sortCol = Object.keys(this.compoundSorting)[0];
      const uniqueValues = [
        ...new Set(selectedData.map((row) => row[sortCol])),
      ];

      if (uniqueValues.length > 0) {
        const valueList = uniqueValues
          .map((val) => (typeof val === "string" ? `'${val}'` : val))
          .join(", ");
        conditions.push(`${sortCol} IN (${valueList})`);
      }
    } else {
      // Default to ROWID based filter
      conditions.push(`ROWID IN (${selectedIndices.join(", ")})`);
    }

    return conditions.length > 0 ? conditions.join(" AND ") : "1=1";
  }

  // Update cleanup method
  async destroy() {
    if (this.duckDBProcessor) {
      await this.duckDBProcessor.dropTable();
      await this.duckDBProcessor.close();
      await this.duckDBProcessor.terminate();
    }
  }
}
