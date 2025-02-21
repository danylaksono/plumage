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

        // Normalize column definitions upfront
        this.normalizedColumns = this.normalizeColumnDefinitions(columnNames);

        // Create table element first
        this.table = this.createTableElement();

        this.duckDBTableName = "main_table";
        this.duckDBProcessor = new DuckDBDataProcessor(
          null,
          this.duckDBTableName
        );
        this.data = null;
        this.binningService = new BinningService();
        this.filterService = null; // Will be initialized after DuckDB setup
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
        await this.initializeTable(data, this.normalizedColumns, options);

        return this;
      })();
    }
    throw new Error("SorterTable must be instantiated with new");
  }

  normalizeColumnDefinitions(columns) {
    return columns.map((col) => {
      if (typeof col === "string") {
        return { column: col };
      }
      if (typeof col === "object" && col !== null && col.column) {
        return { ...col };
      }
      console.error("Invalid column definition:", col);
      throw new Error("Invalid column definition");
    });
  }

  async initializeTable(data, columnDefinitions, options) {
    try {
      // Initialize DuckDB first
      await this.duckDBProcessor.connect();

      // Now initialize FilterService with DuckDB processor
      this.filterService = new FilterService(this.duckDBProcessor);

      // Load data first
      await this.duckDBProcessor.loadData(data, this.detectDataFormat(data));

      // Process column definitions and get types from DuckDB
      const columnTypes = await Promise.all(
        columnDefinitions.map(async (colDef) => {
          const colName = typeof colDef === "string" ? colDef : colDef.column;
          const type = await this.duckDBProcessor.getTypeFromDuckDB(colName);

          // Initialize bins using DuckDB for non-unique columns
          let binData = [];
          if (!colDef.unique) {
            binData = await this.duckDBProcessor.binDataWithDuckDB(
              colName,
              type,
              options.maxOrdinalBins || 20
            );
          }

          return {
            ...(typeof colDef === "string" ? { column: colDef } : colDef),
            type: colDef.type || type,
            bins: binData,
          };
        })
      );

      // Get initial data
      this.data = await this.duckDBProcessor.query(
        `SELECT * FROM ${this.duckDBTableName} LIMIT ${this.options.rowsPerPage}`
      );

      if (!this.data || this.data.length === 0) {
        throw new Error("No data loaded from DuckDB");
      }

      this.dataInd = d3.range(this.data.length);

      // Initialize column manager with processed definitions
      this.columnManager = new ColumnManager(
        columnTypes,
        this.data,
        this.binningService,
        options.maxOrdinalBins,
        options.continuousBinMethod
      );

      // Store initial column state
      this.initialColumns = JSON.parse(
        JSON.stringify(this.columnManager.columns)
      );

      // Initialize table renderer
      this.tableRenderer = new TableRenderer(
        this.columnManager.columns,
        this.data,
        options.cellRenderers,
        this.selectRow.bind(this),
        this.unselectRow.bind(this),
        this.getRowIndex.bind(this)
      );
      this.tableRenderer.setTable(this.table);

      // Initialize visualizations
      await this.initializeVisualizations();

      // Create initial table structure
      this.createHeader();
      this.createTable();
    } catch (error) {
      console.error("Failed to initialize table:", error);
      throw error;
    }
  }

  async initializeVisualizations() {
    // Initialize visualization controllers for each column
    this.visControllers = await Promise.all(
      this.columnManager.columns.map(async (col) => {
        if (col.unique) {
          return null;
        }

        // Create visualization controller with proper options
        const controller = new HistogramController([], {
          type: col.type,
          thresholds: col.type === "continuous" ? 20 : undefined,
          maxOrdinalBins: this.options.maxOrdinalBins,
        });

        // Link controller to table and set column name
        controller.table = this;
        controller.columnName = col.column;

        // Get binned data from DuckDB
        const binData = await this.duckDBProcessor.binDataWithDuckDB(
          col.column,
          col.type,
          this.options.maxOrdinalBins || 20
        );

        // Set initial data with binning info
        const visData = await this.getVisualizationData(col.column, binData);
        await controller.initialize(visData, {
          type: col.type,
          bins: binData,
        });

        return controller;
      })
    );
  }

  async getVisualizationData(columnName, binData) {
    // Get raw column data
    const query = `SELECT "${columnName}" FROM ${this.duckDBTableName}`;
    const result = await this.duckDBProcessor.query(query);
    return result.map((row) => row[columnName]);
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
      const filterClause = await this.filterService.applyFilter(
        Array.from(this.selectedRows).map((i) => this.data[i]),
        Object.keys(this.compoundSorting)[0]
      );

      if (!filterClause) {
        return;
      }

      // Get filtered data
      const filteredData = await this.duckDBProcessor.query(`
        SELECT * FROM ${this.duckDBTableName}
        WHERE ${filterClause}
        LIMIT ${this.options.rowsPerPage}
      `);

      // Update data and indices
      this.data = filteredData;
      this.dataInd = filteredData.map((_, idx) => idx);

      // Update all histograms with filtered data while keeping original data visible
      await Promise.all(
        this.visControllers.map(async (vc, idx) => {
          if (!vc) return;

          const columnName = this.columnManager.columns[idx].column;
          const columnData = filteredData.map((row) => row[columnName]);

          // Update histogram with new data while keeping original data visible in background
          await vc.updateData(columnData);
        })
      );

      this.createTable();
      this.changed({
        type: "filter",
        indeces: this.dataInd,
        rule: this.getSelectionRule(),
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
    const selection = Array.from(this.selectedRows)
      .filter((index) => index >= 0 && index < this.data.length)
      .map((index) => ({
        index,
        data: this.data[index],
      }));

    // Log selection state for debugging
    console.log("Getting selection:", {
      selectedRows: Array.from(this.selectedRows),
      validSelection: selection.length,
      totalRows: this.data.length,
    });

    this.selected = selection;
    return selection;
  }

  getSelectionRule() {
    if (this.selectedRows.size === 0) return null;

    const rules = [];
    const selectedValues = new Map();

    // Group selected values by column
    this.columnManager.columns.forEach((col) => {
      const values = new Set();
      this.selectedRows.forEach((idx) => {
        const value = this.data[idx][col.column];
        if (value != null) values.add(value);
      });
      if (values.size > 0) {
        selectedValues.set(col.column, Array.from(values));
      }
    });

    // Create rules for each column with selections
    selectedValues.forEach((values, column) => {
      const columnDef = this.columnManager.columns.find(
        (c) => c.column === column
      );
      if (columnDef) {
        if (columnDef.type === "continuous") {
          const range = d3.extent(values);
          rules.push(`${column} between ${range[0]} and ${range[1]}`);
        } else {
          rules.push(`${column} in (${values.join(", ")})`);
        }
      }
    });

    return rules.length > 0 ? rules : null;
  }

  selectionUpdated() {
    const selection = this.getSelection();
    this.changed({
      type: "selection",
      indeces: Array.from(this.selectedRows),
      selection: selection,
      rule: this.getSelectionRule(),
    });

    // Also update histogram visuals
    this.visControllers.forEach((controller) => {
      if (controller) {
        // Get the selected data for this controller's column
        const columnName = controller.columnName;
        if (columnName) {
          const selectedData = Array.from(this.selectedRows)
            .map((idx) => this.data[idx][columnName])
            .filter((val) => val != null);

          if (selectedData.length > 0) {
            controller.highlightedData = selectedData;
          } else {
            controller.highlightedData = null;
          }
          controller.render();
        }
      }
    });
  }

  clearSelection() {
    const previousSize = this.selectedRows.size;
    this.selectedRows.clear();

    if (this.tableRenderer.tBody) {
      Array.from(this.tableRenderer.tBody.children).forEach((tr) => {
        tr.selected = false;
        tr.style.fontWeight = "normal";
        tr.style.color = "grey";
      });
    }

    console.log("Selection cleared:", {
      previousSize,
      currentSize: this.selectedRows.size,
    });
    // Clear histogram selections
    this.resetHistogramSelections();
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
    if (!tr || tr.selected) return;

    const rowIndex = Array.prototype.indexOf.call(
      this.tableRenderer.tBody.children,
      tr
    );
    if (rowIndex >= 0) {
      tr.selected = true;
      tr.style.fontWeight = "bold";
      tr.style.color = "black";
      this.selectedRows.add(rowIndex);

      console.log("Row selected:", {
        index: rowIndex,
        totalSelected: this.selectedRows.size,
      });
    }
  }

  unselectRow(tr) {
    tr.selected = false;
    tr.style.fontWeight = "normal";
    tr.style.color = "grey";
    this.selectedRows.delete(this.getRowIndex(tr));
  }

  getRowIndex(tr) {
    if (!tr || !this.tableRenderer.tBody) return -1;
    return Array.from(this.tableRenderer.tBody.children).indexOf(tr);
  }

  createHeader() {
    // Ensure TableRenderer has the table reference
    if (!this.tableRenderer.table) {
      this.tableRenderer.setTable(this.table);
    }

    // Safely remove existing thead if it exists
    const existingTHead = this.table.querySelector("thead");
    if (existingTHead) {
      existingTHead.remove();
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

    // Safely remove existing tbody if it exists
    const existingTBody = this.table.querySelector("tbody");
    if (existingTBody) {
      existingTBody.remove();
    }

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
      const offset = this.lastLineAdded + 1;

      // Get the current sort column and direction if any
      let orderClause = "";
      if (Object.keys(this.compoundSorting).length > 0) {
        const sortCol = Object.keys(this.compoundSorting)[0];
        const sortDir =
          this.compoundSorting[sortCol].how === "up" ? "ASC" : "DESC";
        orderClause = `ORDER BY ${this.duckDBProcessor.safeColumnName(
          sortCol
        )} ${sortDir}`;
      }

      // Fetch the next batch of data from DuckDB
      const query = `
        WITH ordered_data AS (
          SELECT *, ROW_NUMBER() OVER (${orderClause}) as row_num
          FROM ${this.duckDBTableName}
        )
        SELECT *
        FROM ordered_data
        WHERE row_num > ${offset} AND row_num <= ${offset + howMany}
      `;

      const newData = await this.duckDBProcessor.query(query);

      if (newData && newData.length > 0) {
        // Add new rows to table
        for (let i = 0; i < newData.length; i++) {
          const rowData = newData[i];
          const rowIndex = this.lastLineAdded + 1 + i;

          let tr = document.createElement("tr");
          tr.selected = false;
          Object.assign(tr.style, {
            color: "grey",
            borderBottom: "1px solid #ddd",
          });
          this.tableRenderer.tBody.appendChild(tr);

          // Create cells for each column
          this.columnManager.columns.forEach((c) => {
            let td = document.createElement("td");
            if (
              typeof this.tableRenderer.cellRenderers[c.column] === "function"
            ) {
              td.innerHTML = "";
              td.appendChild(
                this.tableRenderer.cellRenderers[c.column](
                  rowData[c.column],
                  rowData
                )
              );
            } else {
              td.innerText = rowData[c.column];
            }

            tr.appendChild(td);
            td.style.color = "inherit";
            td.style.fontWidth = "inherit";
          });

          // Add click event listener
          tr.addEventListener("click", (event) => {
            if (this.shiftDown) {
              // Select range
              let start = Math.min(this.lastRowSelected, rowIndex);
              let end = Math.max(this.lastRowSelected, rowIndex);
              this.tableRenderer.tBody
                .querySelectorAll("tr")
                .forEach((tr, i) => {
                  if (i >= start && i <= end) {
                    if (!tr.selected) {
                      this.selectRow(tr);
                    }
                  }
                });
            } else if (this.ctrlDown) {
              // Toggle selection
              if (tr.selected) {
                this.unselectRow(tr);
              } else {
                this.selectRow(tr);
              }
            } else {
              // Single select
              this.clearSelection();
              this.selectRow(tr);
            }
            this.lastRowSelected = rowIndex;
            this.selectionUpdated();
          });
        }

        // Update data arrays
        this.data.push(...newData);
        this.dataInd.push(...newData.map((_, i) => this.lastLineAdded + 1 + i));
        this.lastLineAdded += newData.length;
      }
    } catch (error) {
      console.error("Error loading more data:", error);
      throw error;
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
    try {
      this.history.push({ type: "sort", data: [...this.dataInd] });
      this.compoundSorting = {};

      const colName = controller.getColumn();
      const how = controller.getDirection();
      const sortDirection = how === "up" ? "ASC" : "DESC";

      // Get safe column name from DuckDB processor
      const escapedColumn = this.duckDBProcessor.safeColumnName(colName);

      // Handle BigInt columns by casting to DOUBLE in the ORDER BY clause
      const orderByClause = `CAST(${escapedColumn} AS DOUBLE) ${sortDirection}`;

      // First get total count for pagination info
      const countQuery = `SELECT COUNT(*) as total FROM ${this.duckDBTableName}`;
      const countResult = await this.duckDBProcessor.query(countQuery);
      const totalRows = countResult[0].total;

      // Get sorted data with all necessary columns for visualization
      const query = `
        WITH sorted_data AS (
          SELECT *, ROW_NUMBER() OVER (ORDER BY ${orderByClause}) as sort_order
          FROM ${this.duckDBTableName}
        )
        SELECT *
        FROM sorted_data
        WHERE sort_order <= ${this.options.rowsPerPage}
      `;

      const sortedResult = await this.duckDBProcessor.query(query);

      // Update data with sorted results
      this.data = sortedResult;
      this.dataInd = sortedResult.map((row, idx) => idx);

      // Store sort state
      this.compoundSorting[colName] = {
        how,
        order: Object.keys(this.compoundSorting).length,
      };

      // Update each visualization controller with properly processed data
      for (let i = 0; i < this.visControllers.length; i++) {
        const vc = this.visControllers[i];
        if (!vc) continue;

        const columnName = this.columnManager.columns[i].column;
        const columnType = this.columnManager.columns[i].type;

        try {
          // Get binned data for the column using DuckDB
          const binData = await this.duckDBProcessor.binDataWithDuckDB(
            columnName,
            columnType,
            this.options.maxOrdinalBins || 20
          );

          // Prepare visualization data
          const visData = {
            type: columnType,
            bins: binData || [], // Ensure bins is at least an empty array
            ...(columnType === "ordinal" && {
              nominals:
                binData?.map((bin) => bin.key).filter((k) => k !== undefined) ||
                [],
            }),
          };

          // Check if visualization controller has setData method before calling
          if (vc && typeof vc.setData === "function") {
            vc.setData(visData);
          }
        } catch (error) {
          console.warn(
            `Failed to update visualization for column ${columnName}:`,
            error
          );
          // Continue with other visualizations even if one fails
          continue;
        }
      }

      // Recreate table with new data
      this.lastLineAdded = this.options.rowsPerPage - 1;
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
      throw error;
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
        const escapedColumn = this.duckDBProcessor.safeColumnName(sortCol);
        const valueList = uniqueValues
          .map((val) => {
            if (val === null || val === undefined) return "NULL";
            return `'${String(val).replace(/'/g, "''")}'`;
          })
          .join(", ");

        conditions.push(`${escapedColumn} IN (${valueList})`);
      }
    } else {
      // For ROWID filtering
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

  // Update the updateSelection method to handle histogram interactions
  updateSelection(selectedValues, sourceColumn) {
    // Use histogram handler for selection
    if (this.handleHistogramSelection) {
      this.handleHistogramSelection(selectedValues, sourceColumn);
      return;
    }

    if (!this.ctrlDown) {
      this.clearSelection();
    }

    const columnDef = this.columnManager.columns.find(
      (c) => c.column === sourceColumn
    );
    if (!columnDef) return;

    // Convert selected values to strings for consistent comparison
    const selectedLookup = new Set(
      Array.from(selectedValues).map((v) => String(v))
    );

    // Update table row selection
    let selectedCount = 0;
    Array.from(this.tableRenderer.tBody.children).forEach((tr, idx) => {
      const rowValue = this.data[idx][sourceColumn];
      if (rowValue != null) {
        const strValue = String(rowValue);
        if (selectedLookup.has(strValue)) {
          this.selectRow(tr);
          selectedCount++;
        }
      }
    });

    // Update other histograms
    this.updateOtherHistograms(sourceColumn);

    this.selectionUpdated();
  }

  handleHistogramSelection(selectedValues, sourceColumn) {
    if (!this.ctrlDown) {
      this.clearSelection();
    }

    const columnDef = this.columnManager.columns.find(
      (c) => c.column === sourceColumn
    );
    if (!columnDef) return;

    // Create a lookup of selected values for efficient comparison
    const selectedLookup = new Set();
    selectedValues.forEach((value) => {
      // Store both string and original value to handle different types
      selectedLookup.add(String(value));
      selectedLookup.add(value);
    });

    console.log("Processing selection:", {
      selectedValues: Array.from(selectedValues),
      columnName: sourceColumn,
      columnType: columnDef.type,
    });

    // Select matching rows
    let selectedCount = 0;
    Array.from(this.tableRenderer.tBody.children).forEach((tr, idx) => {
      const rowValue = this.data[idx][sourceColumn];
      if (rowValue != null) {
        // Try both string and original value comparison
        if (
          selectedLookup.has(rowValue) ||
          selectedLookup.has(String(rowValue))
        ) {
          this.selectRow(tr);
          selectedCount++;
          console.log("Selected row:", {
            index: idx,
            value: rowValue,
            stringValue: String(rowValue),
          });
        }
      }
    });

    console.log("Selection complete:", {
      totalSelected: selectedCount,
      selectedRows: Array.from(this.selectedRows),
    });

    // Update other histograms
    this.updateOtherHistograms(sourceColumn);

    this.selectionUpdated();
  }

  selectRow(tr) {
    const rowIndex = Array.from(this.tableRenderer.tBody.children).indexOf(tr);
    if (rowIndex >= 0 && !tr.selected) {
      tr.selected = true;
      tr.style.fontWeight = "bold";
      tr.style.color = "black";
      this.selectedRows.add(rowIndex);
    }
  }

  updateOtherHistograms(sourceColumn) {
    this.visControllers.forEach((controller, idx) => {
      if (
        controller &&
        this.columnManager.columns[idx].column !== sourceColumn
      ) {
        const columnName = this.columnManager.columns[idx].column;

        // Get data for the selected rows for this column
        const selectedData = Array.from(this.selectedRows)
          .map((rowIdx) => this.data[rowIdx][columnName])
          .filter((val) => val != null);

        console.log("Updating histogram:", {
          column: columnName,
          selectedDataCount: selectedData.length,
          sampleValues: selectedData.slice(0, 3),
        });

        if (selectedData.length > 0) {
          controller.highlightedData = selectedData;
          controller.render();
        }
      }
    });
  }

  clearSelection() {
    this.selectedRows.clear();
    this.tableRenderer.updateSelection([]);
  }
}
