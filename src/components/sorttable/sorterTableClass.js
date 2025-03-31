import * as d3 from "npm:d3";
import { DuckDBDataProcessor } from "../_base/duckdb-processor.js";
import { BinningService } from "./BinningService.js";

export class sorterTable {
  constructor(data, columnNames, changed, options = {}) {
    // Setup DuckDB processor
    this.dbProcessor = null;
    this.tableName = `table_${Math.random().toString(36).substring(7)}`;

    // Add query cache
    this.queryCache = new Map();
    this.enableCaching =
      options.enableCaching !== undefined ? options.enableCaching : true;
    this.cacheTTL = options.cacheTTL || 60000; // Default 60 seconds cache lifetime

    // Initialize core properties first
    this.data = this.preprocessData(data, columnNames); // Add preprocessing

    // this.db = DuckDBClient.of({ dataset: data }); // Initialize DuckDBClient
    // console.log("DuckDBClient initialized:", this.db);

    // console.log("Duckdb query", this.duckFilter());

    this.columnTypes = {};
    this.columns = columnNames.map((col) => {
      if (typeof col === "string") {
        return { column: col, unique: false };
      } else {
        return {
          column: col.column,
          alias: col.alias,
          unique: col.unique || false,
          type: col.type || null, // Add support for manual type definition
        };
      }
    });

    // Pre-populate column types if manually specified
    this.columns.forEach((col) => {
      if (col.type) {
        this.columnTypes[col.column] = col.type;
      }
    });

    // Initialize the BinningService
    this.binningService = new BinningService({
      maxOrdinalBins: options.maxOrdinalBins || 12,
      continuousBinMethod: options.continuousBinMethod || "scott",
      dateInterval: options.dateInterval || "day",
      minBinSize: options.minBinSize || 5,
      customThresholds: options.customThresholds || null,
    });

    this.inferColumnTypesAndThresholds(data);

    // create table element
    this.table = document.createElement("table");
    this.table.classList.add("sorter-table");

    this.initialColumns = JSON.parse(JSON.stringify(this.columns));

    console.log("Initial columns:", this.columns);

    this.changed = changed;
    this._isUndoing = false;
    this.dataInd = d3.range(data.length);
    this.sortControllers = [];
    this.visControllers = [];
    this.table = document.createElement("table");
    this.table.style.userSelect = "none";
    this.compoundSorting = {};
    this.selected = [];
    this.selectedColumn = null;
    this.history = [];
    this.tBody = null;
    this.tHead = null;
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
      0, 0.01, 0.02, 0.03, 0.04, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8,
      0.9, 0.95, 0.96, 0.97, 0.98, 0.99, 1,
    ];
    this.cellRenderers = {}; // Custom cell renderers
    if (options.cellRenderers) {
      for (const [columnName, renderer] of Object.entries(
        options.cellRenderers
      )) {
        // Find the column object that matches the renderer's column name
        const column = this.columns.find((col) => col.column === columnName);
        if (column) {
          this.cellRenderers[columnName] = renderer;
        } else {
          console.warn(`No column found for cell renderer: ${columnName}`);
        }
      }
    }
    this.showDefaultControls =
      options.showDefaultControls !== undefined
        ? options.showDefaultControls
        : true;

    this.options = {
      containerHeight: options.height || "400px",
      containerWidth: options.width || "100%",
      rowsPerPage: options.rowsPerPage || 50,
      loadMoreThreshold: options.loadMoreThreshold || 100,
    };

    Object.assign(this.table.style, {
      width: "100%",
      borderCollapse: "collapse",
      // border: "1px solid #ddd",
      fontFamily: "Arial, sans-serif",
      fontSize: "14px",
    });

    // Parallel query settings
    this.maxWorkers = options.maxWorkers || navigator.hardwareConcurrency || 4;
    this.workerPool = [];
    this.parallelQueries =
      options.parallelQueries !== undefined ? options.parallelQueries : true;

    this.createHeader();
    this.createTable();

    this.table.addEventListener("mousedown", (event) => {
      if (event.shiftKey) {
        this.shiftDown = true;
      } else {
        this.shiftDown = false;
      }
    });
  }

  async initializeDuckDB() {
    if (!this.dbProcessor) {
      this.dbProcessor = new DuckDBDataProcessor(null, this.tableName);
      await this.dbProcessor.connect();
      await this.dbProcessor.loadData(this.data);
    }
    return this.dbProcessor;
  }

  async getProcessedData() {
    await this.initializeDuckDB();
    return this.dbProcessor;
  }

  // Helper method to ensure DuckDB is ready
  async ensureDuckDB() {
    if (!this.dbProcessor) {
      await this.initializeDuckDB();
    }
    return this.dbProcessor;
  }

  // Initialize worker pool for parallel queries
  async initializeWorkerPool() {
    if (this.workerPool.length > 0) return; // Already initialized

    // Ensure the main connection exists first
    await this.ensureDuckDB();

    // Only create multiple workers if parallel queries are enabled
    if (this.parallelQueries) {
      const workerCount = Math.min(this.maxWorkers, 8); // Limit to reasonable number

      for (let i = 0; i < workerCount; i++) {
        const workerId = `worker_${i}`;
        const worker = new DuckDBDataProcessor(
          null,
          `${this.tableName}_${workerId}` // Use worker-specific table name
        );
        await worker.connect();
        await worker.loadData(this.data);
        this.workerPool.push({
          id: workerId,
          worker: worker,
          busy: false,
        });
      }

      console.log(`Initialized ${this.workerPool.length} DuckDB workers`);

      // Ensure tables are correctly set up
      await this.synchronizeWorkerTables();
    }
  }

  // Get an available worker from the pool
  async getAvailableWorker() {
    if (!this.parallelQueries || this.workerPool.length === 0) {
      return await this.ensureDuckDB(); // Fall back to main worker
    }

    // Find first non-busy worker
    const availableWorker = this.workerPool.find((w) => !w.busy);
    if (availableWorker) {
      availableWorker.busy = true;
      return availableWorker.worker;
    }

    // If all workers busy, return the main worker
    return await this.ensureDuckDB();
  }

  // Release worker back to pool
  releaseWorker(worker) {
    const poolWorker = this.workerPool.find((w) => w.worker === worker);
    if (poolWorker) {
      poolWorker.busy = false;
    }
  }

  // Execute queries in parallel
  async executeQueriesInParallel(queries) {
    if (!this.parallelQueries || queries.length <= 1) {
      const db = await this.ensureDuckDB();
      return Promise.all(queries.map((q) => db.query(q)));
    }

    await this.initializeWorkerPool();

    // If we have multiple workers, distribute queries among them
    const results = await Promise.all(
      queries.map(async (query) => {
        const worker = await this.getAvailableWorker();
        try {
          // Create cache key before modifying the query
          const cacheKey = query;

          // Check cache first
          const cachedResult = this.getCachedResult(cacheKey);
          if (cachedResult !== null) {
            return cachedResult;
          }

          // Replace table name with worker-specific table name if needed
          let workerQuery = query;
          if (worker !== this.dbProcessor) {
            // Find the worker in the pool to get its ID
            const poolWorker = this.workerPool.find((w) => w.worker === worker);
            if (poolWorker) {
              // Replace main table name with worker-specific table name
              workerQuery = query.replace(
                new RegExp(this.tableName, "g"),
                `${this.tableName}_${poolWorker.id}`
              );
            }
          }

          // Execute query with potentially modified table name
          const result = await worker.query(workerQuery);

          // Cache the result using original query as key
          this.setCachedResult(cacheKey, result);
          return result;
        } finally {
          this.releaseWorker(worker);
        }
      })
    );

    return results;
  }

  // Ensure worker tables have the same structure
  async synchronizeWorkerTables() {
    if (!this.parallelQueries || this.workerPool.length === 0) {
      return; // Nothing to synchronize
    }

    // Get schema from main table
    const db = await this.ensureDuckDB();
    const schemaQuery = `DESCRIBE ${this.tableName}`;
    let tableSchema;

    try {
      tableSchema = await db.query(schemaQuery);
    } catch (error) {
      console.error(`Failed to get schema for main table: ${error.message}`);
      return;
    }

    // Apply schema to each worker
    for (const worker of this.workerPool) {
      try {
        // First ensure the worker table exists
        const createTableQuery = `CREATE TABLE IF NOT EXISTS ${this.tableName}_${worker.id} AS SELECT * FROM ${this.tableName}`;
        await worker.worker.query(createTableQuery);
      } catch (error) {
        console.error(
          `Failed to synchronize worker table ${worker.id}: ${error.message}`
        );
      }
    }

    console.log(`Synchronized schema across ${this.workerPool.length} workers`);
  }

  // Execute a single query with worker fallback
  async executeQueryWithFallback(query) {
    const worker = await this.getAvailableWorker();
    try {
      // Check cache first using original query as key
      const cachedResult = this.getCachedResult(query);
      if (cachedResult !== null) {
        return cachedResult;
      }

      // Replace table name if using a worker
      let workerQuery = query;
      if (worker !== this.dbProcessor) {
        const poolWorker = this.workerPool.find((w) => w.worker === worker);
        if (poolWorker) {
          workerQuery = query.replace(
            new RegExp(this.tableName, "g"),
            `${this.tableName}_${poolWorker.id}`
          );
        }
      }

      // Execute query
      const result = await worker.query(workerQuery);
      this.setCachedResult(query, result);
      return result;
    } catch (error) {
      console.warn(
        `Worker query failed, falling back to main connection: ${error.message}`
      );

      // If worker failed and it's not the main connection, try with main
      if (worker !== this.dbProcessor) {
        try {
          const mainResult = await this.dbProcessor.query(query);
          this.setCachedResult(query, mainResult);
          return mainResult;
        } catch (mainError) {
          console.error(
            `Main connection query also failed: ${mainError.message}`
          );
          console.error(`Failed query: ${query}`);
          return [];
        }
      } else {
        console.error(`Main connection query failed: ${error.message}`);
        console.error(`Failed query: ${query}`);
        return [];
      }
    } finally {
      this.releaseWorker(worker);
    }
  }

  // Method to shift column position in the table
  shiftCol(columnName, direction) {
    // Add to history if not undoing
    if (!this._isUndoing) {
      this.history.push({
        type: "shiftcol",
        columnName: columnName,
        dir: direction,
      });
    }

    // Find current column index
    const currentIndex = this.columns.findIndex((c) => c.column === columnName);
    if (currentIndex === -1) {
      console.warn(`Column ${columnName} not found in columns array.`);
      return;
    }

    // Calculate new index based on direction
    let newIndex;
    if (direction === "left") {
      newIndex = Math.max(0, currentIndex - 1);
    } else if (direction === "right") {
      newIndex = Math.min(this.columns.length - 1, currentIndex + 1);
    } else {
      console.warn(`Invalid direction: ${direction}`);
      return;
    }

    // If no actual movement needed, return
    if (newIndex === currentIndex) return;

    // Move the column in the array
    const column = this.columns.splice(currentIndex, 1)[0];
    this.columns.splice(newIndex, 0, column);

    // Reorder visualisation controllers to match
    if (this.visControllers.length === this.columns.length) {
      const visController = this.visControllers.splice(currentIndex, 1)[0];
      this.visControllers.splice(newIndex, 0, visController);
    }

    // Reorder sort controllers to match
    if (this.sortControllers.length === this.columns.length) {
      const sortController = this.sortControllers.splice(currentIndex, 1)[0];
      this.sortControllers.splice(newIndex, 0, sortController);
    }

    // Rebuild the table UI
    this.rebuildTable();

    // Notify about the column shift
    this.changed({
      type: "columnShift",
      column: columnName,
      direction: direction,
    });
  }

  // async duckFilter() {
  //   // const whereClause = this.getSelectionRuleAsSQL(); // Convert rules to SQL
  //   const result = await this.db.sql`
  //     SELECT * FROM dataset`;
  //   this.dataInd = await result.array(); // Assuming original indices are preserved
  // }

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

  setColumnType(columnName, type) {
    if (!columnName) {
      console.error("Invalid columnName:", columnName);
      return;
    }
    this.columnTypes[columnName] = type;
    // console.log("Setting column type:", this.columnTypes);  // DEBUG
  }

  getColumnType(data, column) {
    // If already cached, return the cached type
    if (this.columnTypes[column]) {
      return this.columnTypes[column];
    }

    // Infer type from data
    for (const d of data) {
      const value = d[column];
      if (value === undefined || value === null) continue;

      // Check for date objects
      if (value instanceof Date) return "date";

      // Check for numbers
      if (typeof value === "number" || !isNaN(Number(value))) {
        // Check if it's really continuous or just a few discrete values
        const uniqueValues = new Set(data.map((d) => d[column])).size;
        if (uniqueValues > 10) {
          // Threshold for considering it continuous
          return "continuous";
        }
      }

      // Default to ordinal for strings and small number sets
      return "ordinal";
    }

    // Default to ordinal if no clear type is found
    return "ordinal";
  }

  async inferColumnTypesAndThresholds(data) {
    if (!this.binningService) {
      console.error("BinningService not initialized");
      return;
    }

    const db = await this.ensureDuckDB();

    // Prepare batches of queries for parallel execution
    const histogramQueries = [];
    const typeQueries = [];
    const columnsToProcess = [];

    for (const colDef of this.columns) {
      const colName = colDef.column;

      // Skip unique columns for binning
      if (colDef.unique) continue;

      // Collect column metadata queries
      if (!colDef.type) {
        typeQueries.push({
          query: `SELECT typeof(${colName}) as type FROM ${this.tableName} LIMIT 1`,
          colDef: colDef,
        });
      }

      // Collect histogram queries for non-date columns
      if (colDef.type !== "date") {
        histogramQueries.push({
          query: `SELECT histogram(${colName}) AS hist FROM ${this.tableName}`,
          colDef: colDef,
          colName: colName,
        });
        columnsToProcess.push(colDef);
      }
    }

    // Execute type queries in parallel if needed
    if (typeQueries.length > 0) {
      const typeQueryStrings = typeQueries.map((q) => q.query);
      const typeResults = await this.executeQueriesInParallel(typeQueryStrings);

      // Process type results
      typeQueries.forEach((q, i) => {
        const result = typeResults[i];
        if (result && result[0]) {
          q.colDef.type = result[0].type;
          this.setColumnType(q.colDef.column, q.colDef.type);
        }
      });
    }

    // Execute histogram queries in parallel
    if (histogramQueries.length > 0) {
      const histQueryStrings = histogramQueries.map((q) => q.query);
      const histResults = await this.executeQueriesInParallel(histQueryStrings);

      // Process histogram results
      histogramQueries.forEach((q, i) => {
        const result = histResults[i];
        const histData = result && result[0]?.hist;

        if (!histData) {
          console.warn(`No histogram data for column: ${q.colName}`);
          return;
        }

        const bins = Object.entries(histData).map(([key, count]) => ({
          key:
            key === "null"
              ? null
              : q.colDef.type === "continuous"
              ? parseFloat(key)
              : key, // Handle null and type conversion
          count: count,
        }));

        if (!bins || bins.length === 0) {
          console.warn(`No bins generated for column: ${q.colName}`);
          return;
        }

        if (q.colDef.type === "continuous") {
          bins.sort((a, b) => a.key - b.key);
          q.colDef.thresholds = bins.map((bin) => bin.key);
          q.colDef.bins = bins;
        } else if (q.colDef.type === "ordinal") {
          q.colDef.bins = bins;
          q.colDef.nominals = bins
            .map((bin) => bin.key)
            .filter((key) => key !== undefined && key !== null);
        }
      });
    }

    // Process any remaining columns (like date columns) sequentially
    for (const colDef of this.columns) {
      if (colDef.unique || columnsToProcess.includes(colDef)) continue;

      try {
        const colName = colDef.column;
        const cacheKey = `binDataWithDuckDB_${colName}_${colDef.type}`;
        let bins = this.getCachedResult(cacheKey);

        if (!bins) {
          bins = await db.binDataWithDuckDB(colName, colDef.type);
          this.setCachedResult(cacheKey, bins);
        }

        if (!bins || bins.length === 0) {
          console.warn(`No bins generated for column: ${colName}`);
          continue;
        }

        if (colDef.type === "date") {
          colDef.bins = bins;
          colDef.dateRange = d3.extent(bins, (bin) => bin.x0);
        }
      } catch (error) {
        console.error(`Error processing column ${colDef.column}:`, error);
      }
    }
  }

  async filter() {
    const db = await this.ensureDuckDB();

    // Get current selection rule
    const rules = this.getSelectionRule();
    if (rules) {
      // Handle both array and single rule cases
      const rulesToAdd = Array.isArray(rules) ? rules : [rules];

      // Add new rules to this.rules
      this.rules.push(...rulesToAdd);

      // Convert selection to SQL filter conditions
      const filterConditions = this.rules
        .filter((rule) => typeof rule === "string") // Ensure rule is a string
        .map((rule) => {
          try {
            // Parse rule string to extract column, operator and value
            const parts = rule.split(" ");
            return {
              column: parts[0],
              operator: parts.includes("lower") ? "<" : ">",
              value: parseFloat(parts[parts.length - 1]),
            };
          } catch (error) {
            console.error("Error parsing rule:", rule, error);
            return null;
          }
        })
        .filter((condition) => condition !== null); // Remove any failed parses

      if (filterConditions.length > 0) {
        // Apply filter using DuckDB
        const result = await db.applyFilter(filterConditions);
        this.dataInd = result.map((row) => row.ROWID);
      }
    }

    // Update visualizations
    this.visControllers.forEach((vc, vci) => {
      if (vc instanceof HistogramController) {
        const columnName = this.columns[vci].column;
        const columnData = this.dataInd.map((i) => this.data[i][columnName]);
        vc.setData(columnData);
      }
    });

    // Recreate table with filtered data
    this.createTable();

    // Notify about the filter change
    this.changed({
      type: "filter",
      indeces: this.dataInd,
      rule: this.getSelectionRule(),
    });
  }

  applyCustomFilter(filterFunction) {
    // Apply the custom filter function to the data
    this.dataInd = this.dataInd.filter((index) => {
      return filterFunction(this.data[index]); // Pass the data object to the filter function
    });

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
            this.dataInd.map((i) => this.data[i][this.columns[vci].column])
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

    // Check if there are any selected rows or sorting keys
    if (sortKeys.length === 0 || sel.length === 0) {
      return null;
    }

    let col = sortKeys[0];
    // Safe access to first and last indices
    let firstIndex = sel[0].index;
    let lastIndex = sel[sel.length - 1].index;

    // Fix comparison operator (was using assignment = instead of comparison ==)
    if (firstIndex == 0 && lastIndex == this.dataInd.length - 1) return [];
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
    if (this.tBody) {
      this.tBody.querySelectorAll("tr").forEach((tr) => {
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

    this.tHead.querySelectorAll("th").forEach((th) => {
      th.classList.remove("selected-column"); // Remove previous selection
    });

    const columnIndex = this.columns.findIndex((c) => c.column === columnName);
    if (columnIndex !== -1) {
      this.tHead
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
    this.tBody.querySelectorAll("tr").forEach((t, i) => {
      if (t == tr) index = i;
    });
    return index;
  }

  createHeader() {
    if (this.tHead != null) {
      this.table.removeChild(this.tHead);
    }

    this.sortControllers = [];
    this.visControllers = [];

    this.tHead = document.createElement("thead");
    this.table.appendChild(this.tHead);

    // --- Column Header Row ---
    let headerRow = document.createElement("tr");
    this.tHead.append(headerRow);

    this.columns.forEach((c) => {
      let th = document.createElement("th");
      headerRow.appendChild(th);
      th.style.textAlign = "center";

      // --- Column Name ---
      let nameSpan = document.createElement("span");
      nameSpan.innerText = c.alias || c.column;
      // nameSpan.style.fontWeight = "bold";
      // nameSpan.style.fontFamily = "Arial, sans-serif"; // Set font (optional)
      // nameSpan.style.fontSize = "1em";
      // nameSpan.style.cursor = "pointer";
      Object.assign(nameSpan.style, {
        fontWeight: "bold",
        fontFamily: "Arial, sans-serif",
        fontSize: "1em",
        cursor: "pointer",
        userSelect: "none",
        padding: "8px",
      });
      th.appendChild(nameSpan);

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

      // nameSpan.addEventListener("mouseover", () => {
      //   th.style.backgroundColor = "#e8e8e8"; // Light hover effect
      // });

      // nameSpan.addEventListener("mouseout", () => {
      //   th.style.backgroundColor = ""; // Reset background color
      // });

      // --- Controls Row ---
      let controlsRow = document.createElement("tr");
      th.appendChild(controlsRow); // Append controls row to the header cell (th)

      let controlsTd = document.createElement("td");
      controlsRow.appendChild(controlsTd);

      // Create a container for controls
      let controlsContainer = document.createElement("div");
      controlsContainer.style.display = "flex";
      controlsContainer.style.alignItems = "center"; // Vertically center
      controlsContainer.style.justifyContent = "space-around"; // Space out the controls
      controlsContainer.style.width = "100%";
      controlsContainer.style.padding = "2px 0"; // Add some padding
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
    this.tHead.style.position = "sticky";
    this.tHead.style.top = "0";
    this.tHead.style.backgroundColor = "#ffffff"; // Ensure header is opaque
    this.tHead.style.zIndex = "1"; // Keep header above table content
    this.tHead.style.boxShadow = "0 2px 2px rgba(0,0,0,0.1)";
  }

  createTable() {
    if (this.tBody != null) this.table.removeChild(this.tBody);

    this.tBody = document.createElement("tbody");
    this.table.appendChild(this.tBody);

    this.lastLineAdded = -1;
    this.addTableRows(this.defaultLines);
  }

  addTableRows(howMany) {
    if (this.addingRows) {
      return; // Prevent overlapping calls
    }
    this.addingRows = true;

    let min = this.lastLineAdded + 1; // Corrected: Start from the next line
    let max = Math.min(min + howMany - 1, this.dataInd.length - 1); // Corrected: Use Math.min to avoid exceeding dataInd.length

    for (let row = min; row <= max; row++) {
      let dataIndex = this.dataInd[row]; // Adjust index for dataInd
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
      this.lastLineAdded = row; // Update the last line added
    }

    this.addingRows = false;
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
    this.columns = this.initialColumns.map((col) => ({ ...col }));

    // Update vis controllers
    this.visControllers.forEach((vc, index) => {
      const columnData = this.dataInd.map(
        (i) => this.data[i][this.columns[index].column]
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

  async sortChanged(sortController) {
    // Store the current data for undo history
    if (!this._isUndoing) {
      this.history.push({
        type: "sort",
        data: [...this.dataInd],
        sort: JSON.parse(JSON.stringify(this.compoundSorting)),
      });
    }

    // Get column and sort direction from the controller
    const column = sortController.getColumn();
    const direction = sortController.getDirection();

    if (direction === "none") {
      delete this.compoundSorting[column];
    } else {
      this.compoundSorting[column] = { direction, how: direction };
    }

    // Generate SQL ORDER BY clause
    const orderBy = Object.entries(this.compoundSorting)
      .map(([col, details]) => {
        return `${col} ${details.direction === "up" ? "ASC" : "DESC"}`;
      })
      .join(", ");

    // If no sorting criteria remain, do nothing
    if (orderBy === "") return;

    try {
      const db = await this.ensureDuckDB();
      const query = `SELECT ROWID FROM ${this.tableName} ORDER BY ${orderBy}`;
      const result = await this.executeQueryWithFallback(query);

      // FIX: Ensure result is properly processed before mapping
      if (!result) {
        console.error("Sort query returned null or undefined result");
        return;
      }

      // Handle different result formats that might come from DuckDB
      let rowIds;
      if (Array.isArray(result)) {
        // If result is already an array
        rowIds = result.map((row) => row.ROWID);
      } else if (result.rows && Array.isArray(result.rows)) {
        // If result has a rows property that is an array
        rowIds = result.rows.map((row) => row.ROWID);
      } else if (typeof result === "object" && result !== null) {
        // If result is an object with direct properties
        rowIds = Object.values(result).map((row) => row.ROWID || row);
      } else {
        console.error("Unexpected result format:", result);
        return;
      }

      // Update dataInd with sorted indices
      this.dataInd = rowIds;

      // Rebuild the table with sorted data
      this.rebuildTable();

      // Notify about the sort change
      this.changed({
        type: "sort",
        indices: this.dataInd,
        sort: this.compoundSorting,
      });
    } catch (error) {
      console.error("Error during sorting:", error);
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
    let container = document.createElement("div");
    container.style.width = "100%";
    container.style.display = "flex";
    container.style.flexDirection = "row";
    Object.assign(container.style, {
      height: this.options.containerHeight,
      width: this.options.containerWidth,
      overflow: "auto",
      position: "relative",
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
    if (this.tableWidth) {
      this.table.style.width = this.tableWidth;
    } else {
      this.table.style.width = "100%"; // Default to 100%
    }
    tableContainer.appendChild(this.table);

    // --- Add sidebar and table container to main container ---
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
        if (element == this.tBody || element == this.tHead) {
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

  async cleanup() {
    // Clear the query cache
    this.clearCache();

    // Clean up the worker pool
    if (this.workerPool && this.workerPool.length > 0) {
      await Promise.all(
        this.workerPool.map(async (worker) => {
          if (worker.worker) {
            await worker.worker.dropTable();
            await worker.worker.close();
            await worker.worker.terminate();
          }
        })
      );
      this.workerPool = [];
    }

    // Clean up the main database processor
    if (this.dbProcessor) {
      await this.dbProcessor.dropTable();
      await this.dbProcessor.close();
      await this.dbProcessor.terminate();
    }
  }

  // Cache management methods
  clearCache() {
    this.queryCache.clear();
  }

  getCachedResult(queryKey) {
    if (!this.enableCaching) return null;

    const cached = this.queryCache.get(queryKey);
    if (!cached) return null;

    // Check if cache entry has expired
    if (Date.now() - cached.timestamp > this.cacheTTL) {
      this.queryCache.delete(queryKey);
      return null;
    }

    return cached.result;
  }

  setCachedResult(queryKey, result) {
    if (!this.enableCaching) return;

    this.queryCache.set(queryKey, {
      result: result,
      timestamp: Date.now(),
    });
  }

  // Enhanced query method with caching and error handling
  async cachedQuery(query, params = []) {
    // Create a cache key from the query and parameters
    const queryKey = JSON.stringify({ query, params });

    // Check cache first
    const cachedResult = this.getCachedResult(queryKey);
    if (cachedResult !== null) {
      return cachedResult;
    }

    // Execute query if not in cache
    const db = await this.ensureDuckDB();
    try {
      const result = await db.query(query, params);

      // Cache the result
      this.setCachedResult(queryKey, result);
      return result;
    } catch (error) {
      console.error(`Query failed: ${error.message}`);
      console.error(`Failed query: ${query}`);
      // Return empty result instead of throwing error
      return [];
    }
  }
}

function SortController(colName, update) {
  let active = false;

  let controller = this;
  let div = document.createElement("div");
  div.style.width = "24px";
  div.style.height = "24px";
  div.style.margin = "0 auto";
  div.style.cursor = "pointer";

  // Use Font Awesome icon
  const icon = document.createElement("i");
  icon.classList.add("fas", "fa-sort"); // Initial sort icon
  icon.style.color = "gray";
  icon.style.fontSize = "12px"; // Adjust icon size if needed
  div.appendChild(icon);

  let sorting = "none";

  // Toggle function
  this.toggleDirection = () => {
    if (sorting === "none" || sorting === "down") {
      sorting = "up";
      icon.classList.remove("fa-sort-down", "fa-sort");
      icon.classList.add("fa-sort-up");
    } else {
      sorting = "down";
      icon.classList.remove("fa-sort-up", "fa-sort");
      icon.classList.add("fa-sort-down");
    }
  };

  this.getDirection = () => sorting;

  this.getColumn = () => colName;

  this.getNode = () => div;

  // Prevent click propagation from the icon
  div.addEventListener("click", (event) => {
    event.stopPropagation();
    active = !active;
    controller.toggleDirection();
    update(controller);

    // Visual feedback
    icon.style.color = active ? "#2196F3" : "gray";
  });

  return this;
}

function ColShiftController(columnName, update) {
  let controller = this;
  let div = document.createElement("div");
  div.style.display = "flex";
  div.style.justifyContent = "space-around";
  div.style.width = "100%";

  const createIcon = (iconClass, direction) => {
    const icon = document.createElement("i");
    icon.classList.add("fas", iconClass);
    icon.style.color = "gray";
    icon.style.cursor = "pointer";
    icon.style.fontSize = "12px";
    icon.style.margin = "0 5px";

    icon.addEventListener("click", (event) => {
      event.stopPropagation();
      update(columnName, direction);
    });

    return icon;
  };

  const leftIcon = createIcon("fa-arrow-left", "left");
  const rightIcon = createIcon("fa-arrow-right", "right");

  div.appendChild(leftIcon);
  div.appendChild(rightIcon);

  this.getNode = () => div;
  return this;
}

function HistogramController(data, binrules) {
  let controller = this;
  let div = document.createElement("div");

  this.bins = [];
  this.brush = null;
  this.isBrushing = false;

  this.updateData = (d) => this.setData(d);

  // Reset the selection state of the histogram
  this.resetSelection = () => {
    this.bins.forEach((bin) => {
      bin.selected = false;
    });

    this.svg.selectAll(".bar rect:nth-child(1)").attr("fill", "steelblue");
  };

  // console.log("------------------binrules outside setData: ", binrules);

  this.setData = function (dd) {
    div.innerHTML = "";

    let data = dd.map((d, i) => ({ value: d, index: i }));

    const svgWidth = 100;
    const svgHeight = 50;
    const margin = { top: 5, right: 5, bottom: 8, left: 5 };
    const width = svgWidth - margin.left - margin.right;
    const height = svgHeight - margin.top - margin.bottom;

    this.svg = d3
      .select(div)
      .append("svg")
      .attr("width", svgWidth)
      .attr("height", svgHeight);
    // .append("g")
    // .attr("transform", `translate(${margin.left},${margin.top})`);

    console.log("------------------binrules in setData: ", binrules);

    if (binrules.unique) {
      // Handle unique columns: create a single bin
      this.bins = [
        {
          category: "Unique Values",
          count: data.length,
          indeces: data.map((d) => d.index),
        },
      ];
    } else if ("thresholds" in binrules) {
      console.log("------------------Continuous data----------------------");
      // Continuous data
      // console.log("Domain: ", [
      //   d3.min(data, (d) => d.value),
      //   d3.max(data, (d) => d.value),
      // ]);

      // console.log("Thresholds: ", binrules.thresholds);
      let contBins = d3
        .bin()
        .domain([d3.min(data, (d) => d.value), d3.max(data, (d) => d.value)])
        .thresholds(binrules.thresholds)
        .value((d) => d.value)(data);

      this.bins = contBins.map((b) => ({
        category: b.x0 + "-" + b.x1,
        count: b.length,
        indeces: b.map((v) => v.index),
      }));

      // console.log("Brush Bins: ", this.bins);

      this.xScale = d3
        .scaleLinear()
        .domain([d3.min(data, (d) => d.value), d3.max(data, (d) => d.value)])
        .range([0, width]);

      // Initialize brush for continuous data
      this.brush = d3
        .brushX()
        .extent([
          [0, 0],
          [svgWidth, svgHeight],
        ])
        .on("end", this.handleBrush);

      // Add brush to svg
      this.svg
        .append("g")
        .attr("class", "brush")
        .style("position", "absolute")
        .style("z-index", 90999) // Attempt to force the brush on top
        .call(this.brush);
    } else if ("ordinals" in binrules || "nominals" in binrules) {
      // Handle ordinal or nominal data
      const frequency = d3.rollup(
        data,
        (values) => ({
          count: values.length,
          indeces: values.map((v) => v.index),
        }),
        (d) => d.value
      );

      const binType = "ordinals" in binrules ? "ordinals" : "nominals";

      // use predefined bin order if available
      if (binType in binrules && Array.isArray(binrules[binType])) {
        this.bins = binrules[binType].map((v) => ({
          category: v,
          count: frequency.get(v) != null ? frequency.get(v).count : 0,
          indeces: frequency.get(v) != null ? frequency.get(v).indeces : [],
        }));
      } else {
        this.bins = Array.from(frequency, ([key, value]) => ({
          category: key,
          count: value.count,
          indeces: value.indeces,
        }));
      }
    }

    this.bins.map((bin, i) => (bin.index = i));

    const y = d3
      .scaleLinear()
      .domain([0, d3.max(this.bins, (d) => d.count)])
      .range([height, 0]);

    const barGroups = this.svg
      .selectAll(".bar")
      .data(this.bins)
      .join("g")
      .attr("class", "bar")
      .attr(
        "transform",
        (d, i) => `translate(${(i * width) / this.bins.length}, 0)`
      );

    // Visible bars
    barGroups
      .append("rect")
      .attr("x", 0)
      .attr("width", (d) => width / this.bins.length)
      .attr("y", (d) => y(d.count))
      .attr("height", (d) => height - y(d.count))
      .attr("fill", "steelblue");

    // For continuous data, we don't need the invisible interaction bars
    // Only add them for ordinal/nominal data
    if (!("thresholds" in binrules)) {
      barGroups
        .append("rect")
        .attr("width", (d) => width / this.bins.length)
        .attr("height", height)
        .attr("fill", "transparent")
        .on("mouseover", (event, d) => {
          if (!d.selected) {
            d3.select(event.currentTarget.previousSibling).attr(
              "fill",
              "purple"
            );
          }

          this.svg
            .selectAll(".histogram-label")
            .data([d])
            .join("text")
            .attr("class", "histogram-label")
            .attr("x", width / 2)
            .attr("y", height + 10)
            .attr("font-size", "10px")
            .attr("fill", "#444444")
            .attr("text-anchor", "middle")
            .text(d.category + ": " + d.count);
        })
        .on("mouseout", (event, d) => {
          if (!d.selected) {
            d3.select(event.currentTarget.previousSibling).attr(
              "fill",
              "steelblue"
            );
          }

          this.svg.selectAll(".histogram-label").remove();
        })
        .on("click", (event, d) => {
          d.selected = !d.selected;

          if (d.selected) {
            d3.select(event.currentTarget.previousSibling).attr(
              "fill",
              "orange"
            );
          } else {
            d3.select(event.currentTarget.previousSibling).attr(
              "fill",
              "steelblue"
            );
          }

          if (controller.table) {
            if (!d.selected) {
              controller.table.clearSelection();
            }

            this.bins[d.index].indeces.forEach((rowIndex) => {
              const tr = controller.table.tBody.querySelector(
                `tr:nth-child(${rowIndex + 1})`
              );
              if (tr) {
                if (d.selected) {
                  controller.table.selectRow(tr);
                } else {
                  controller.table.unselectRow(tr);
                }
              }
            });
            controller.table.selectionUpdated();
          }
        });
    }

    // Add brushing for continuous data
    // Handle brush end event
    this.handleBrush = (event) => {
      // Remove any existing histogram label(s)
      this.svg.selectAll(".histogram-label").remove();

      if (!event.selection) {
        // If no selection from brushing, reset everything
        this.resetSelection();
        if (controller.table) {
          controller.table.clearSelection();
          controller.table.selectionUpdated();
        }
        return;
      }

      const [x0, x1] = event.selection;
      const [bound1, bound2] = event.selection.map(this.xScale.invert);
      const binWidth = width / this.bins.length;

      // console.log("Brushing event: ", event); // Debugging
      // console.log("Brushed Data Range:", x0, x1);
      // console.log("Bins:", this.bins);

      // // Compute selected data range
      // const selectedBins = this.bins.filter(
      //   (bin) => this.xScale(bin.x1) >= x0 && this.xScale(bin.x0) <= x1
      // );

      // console.log("Selected brushed bins: ", selectedBins);

      // // Extract categories or labels from bins
      // const selectedLabels = selectedBins.map((bin) => bin.category || bin.x0);

      // Compute which bins are selected and update their color
      this.bins.forEach((bin, i) => {
        const binStart = i * binWidth;
        const binEnd = (i + 1) * binWidth;
        bin.selected = binStart <= x1 && binEnd >= x0;
        this.svg
          .select(`.bar:nth-child(${i + 1}) rect:nth-child(1)`)
          .attr("fill", bin.selected ? "orange" : "steelblue");
      });

      // Add histogram label
      this.svg
        // .data([d])
        .append("text")
        .attr("class", "histogram-label")
        .join("text")
        .attr("class", "histogram-label")
        .attr("x", width / 2)
        .attr("y", height + 10)
        .attr("font-size", "10px")
        .attr("fill", "#444444")
        .attr("text-anchor", "middle")
        // .text(`Selected: ${selectedLabels.join(", ")}`);
        .text(`Range: ${Math.round(bound1)} - ${Math.round(bound2)}`);

      // Update table selection if table exists
      if (controller.table) {
        controller.table.clearSelection();
        this.bins.forEach((bin) => {
          if (bin.selected) {
            bin.indeces.forEach((rowIndex) => {
              const tr = controller.table.tBody.querySelector(
                `tr:nth-child(${rowIndex + 1})`
              );
              if (tr) {
                controller.table.selectRow(tr);
              }
            });
          }
        });
        controller.table.selectionUpdated();
      }
    };
  };

  this.table = null;
  this.setData(data);

  this.getNode = () => div;
  return this;
}

function createDynamicFilter(attribute, operator, threshold) {
  // Validate attribute
  if (typeof attribute !== "string" || attribute.trim() === "") {
    throw new Error("Invalid attribute: Attribute must be a non-empty string.");
  }

  // Validate operator
  const validOperators = [">", ">=", "<", "<=", "==", "!="];
  if (!validOperators.includes(operator)) {
    throw new Error(
      `Invalid operator: Supported operators are ${validOperators.join(", ")}.`
    );
  }

  // Validate threshold
  if (typeof threshold !== "number" && typeof threshold !== "string") {
    throw new Error(
      "Invalid threshold: Threshold must be a number or a string."
    );
  }

  // Return the filter function
  return (dataObj) => {
    // Use the passed data object directly
    const value = dataObj[attribute];

    if (value === undefined) {
      console.warn(`Attribute "${attribute}" not found in data object.`);
      return false; // Exclude data objects missing the attribute
    }

    // Perform comparison
    try {
      switch (operator) {
        case ">":
          return value > threshold;
        case ">=":
          return value >= threshold;
        case "<":
          return value < threshold;
        case "<=":
          return value <= threshold;
        case "==":
          return value == threshold; // Consider using === for strict equality
        case "!=":
          return value != threshold; // Consider using !== for strict inequality
        default:
          throw new Error(`Unexpected operator: ${operator}`);
      }
    } catch (error) {
      console.error(
        `Error evaluating filter: ${attribute} ${operator} ${threshold} - ${error.message}`
      );
      return false;
    }
  };
}
