import * as duckdb from "npm:@duckdb/duckdb-wasm";

export class DuckDBDataProcessor {
  constructor(config, tableName) {
    this.tableName = tableName;
    this.db = null;
    this.conn = null;
    this.duckdb = null;
    this.tableCreated = false;
    this.options = {
      rowsPerPage: 1000,
      ...config,
    };
    console.log(`[DuckDBProcessor] Created with table: ${this.tableName}`);
  }

  async connect() {
    console.log(`[DuckDBProcessor] Connecting with table: ${this.tableName}`);

    try {
      // Initialize DuckDB if not already initialized
      if (!this.duckdb) {
        // Get WebAssembly bundle URL
        const JSDELIVR_BUNDLES = {
          mvp: {
            mainModule: import.meta.resolve(
              "npm:@duckdb/duckdb-wasm@1.28.1-dev287.0/dist/duckdb-mvp.wasm"
            ),
            mainWorker: import.meta.resolve(
              "npm:@duckdb/duckdb-wasm@1.28.1-dev287.0/dist/duckdb-browser-mvp.worker.js"
            ),
          },
          eh: {
            mainModule: import.meta.resolve(
              "npm:@duckdb/duckdb-wasm@1.28.1-dev287.0/dist/duckdb-eh.wasm"
            ),
            mainWorker: import.meta.resolve(
              "npm:@duckdb/duckdb-wasm@1.28.1-dev287.0/dist/duckdb-browser-eh.worker.js"
            ),
          },
        };

        const bundle = JSDELIVR_BUNDLES.eh;

        // Create a DuckDB database
        const worker = new Worker(bundle.mainWorker);
        const logger = new duckdb.ConsoleLogger();
        this.duckdb = new duckdb.AsyncDuckDB(logger, worker);
        await this.duckdb.instantiate(bundle.mainModule);
        console.log("[DuckDBProcessor] DuckDB instantiated successfully");
      }

      // Create a new connection
      if (!this.conn) {
        this.conn = await this.duckdb.connect();
        console.log("[DuckDBProcessor] Connection established successfully");
      }

      return true;
    } catch (error) {
      console.error("[DuckDBProcessor] Error connecting to DuckDB:", error);
      throw new Error(`Failed to connect to DuckDB: ${error.message}`);
    }
  }

  async tableExists() {
    console.log(
      `[DuckDBProcessor] Checking if table exists: ${this.tableName}`
    );
    try {
      // This should just check if the table exists, not create it
      const query = `
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='${this.tableName}'
      `;
      console.log(`[DuckDBProcessor] Running query: ${query}`);
      const result = await this.query(query);
      const exists = result && result.length > 0;
      console.log(
        `[DuckDBProcessor] Table ${this.tableName} exists: ${exists}`
      );
      return exists;
    } catch (error) {
      console.error(`[DuckDBProcessor] Error checking table: ${error}`);
      return false;
    }
  }

  async cleanup() {
    try {
      if (this.conn) {
        // Drop the table if it exists
        await this.conn.query(`DROP TABLE IF EXISTS ${this.tableName}`);
        await this.conn.close();
        this.conn = null;
      }
      if (this.duckdb) {
        await this.duckdb.terminate();
        this.duckdb = null;
      }
    } catch (error) {
      console.error("Error during cleanup:", error);
      // Continue with cleanup even if there's an error
    }
  }

  // Override the existing close method
  async close() {
    await this.cleanup();
  }

  // Override the existing terminate method
  async terminate() {
    await this.cleanup();
  }

  // Add this helper method to safely handle column names
  safeColumnName(column) {
    // Handle both string and object column definitions
    const columnName = typeof column === "string" ? column : column.column;
    if (!columnName) {
      throw new Error(`Invalid column definition: ${JSON.stringify(column)}`);
    }
    // Escape and quote the column name
    return `"${columnName.replace(/"/g, '""')}"`;
  }

  async getTypeFromDuckDB(column) {
    try {
      const escapedColumn = this.safeColumnName(column);
      const query = `
        SELECT typeof(${escapedColumn}) as col_type
      FROM ${this.tableName}
        WHERE ${escapedColumn} IS NOT NULL
      LIMIT 1
    `;

      const result = await this.conn.query(query);
      const resultArray = result.toArray();

      if (resultArray.length === 0) {
        // Handle the case where the result set is empty
        return "ordinal"; // Or some other default type
      }

      const type = resultArray[0].col_type.toLowerCase();

      // Map DuckDB types to our column types
      if (type.includes("varchar") || type.includes("text")) {
        return "ordinal";
      }
      if (
        type.includes("float") ||
        type.includes("double") ||
        type.includes("decimal") ||
        type.includes("integer") ||
        type.includes("bigint")
      ) {
        return "continuous";
      }
      if (type.includes("date") || type.includes("timestamp")) {
        return "date";
      }
      return "ordinal";
    } catch (error) {
      console.error("Error getting type from DuckDB:", error);
      return "ordinal";
    }
  }

  getDuckDBType(type) {
    switch (type?.toUpperCase()) {
      case "BIGINT":
      case "HUGEINT":
      case "UBIGINT":
        return "bigint";
      case "DOUBLE":
      case "REAL":
      case "FLOAT":
        return "number";
      case "INTEGER":
      case "SMALLINT":
      case "TINYINT":
      case "USMALLINT":
      case "UINTEGER":
      case "UTINYINT":
        return "integer";
      case "BOOLEAN":
        return "boolean";
      case "DATE":
      case "TIMESTAMP":
      case "TIMESTAMP WITH TIME ZONE":
        return "date";
      case "VARCHAR":
      case "UUID":
        return "string";
      default:
        if (/^DECIMAL\(/.test(type)) return "integer";
        return "other";
    }
  }

  /**
   * Bins data from a DuckDB table based on the column type.
   * For continuous data, creates equal-width bins between 5th and 95th percentiles.
   */
  // async binDataWithDuckDB(column, type, maxOrdinalBins = 20) {
  //   const escapedColumn = this.safeColumnName(column);
  //   let query;
  //   switch (type) {
  //     case "continuous":
  //       // Get column type for proper casting
  //       const typeQuery = `SELECT typeof(${escapedColumn}) as col_type
  //                       FROM ${this.tableName}
  //                       WHERE ${escapedColumn} IS NOT NULL
  //                       LIMIT 1`;
  //       const typeResult = await this.logQuery(typeQuery, "Get Column Type");
  //       const typeArray = typeResult.toArray();
  //       const colType = typeArray.length > 0 ? typeArray[0].col_type : null;
  //       if (!colType) {
  //         console.warn(
  //           `Column ${escapedColumn} has no non-null values, defaulting to ordinal type.`
  //         );
  //         return this.binDataWithDuckDB(column, "ordinal", maxOrdinalBins);
  //       }
  //       // Create 10 equal-width bins between 5th and 95th percentiles
  //       query = `
  //       WITH stats AS (
  //         SELECT
  //           PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY ${escapedColumn}) as p05,
  //           PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ${escapedColumn}) as p95
  //         FROM ${this.tableName}
  //         WHERE ${escapedColumn} IS NOT NULL
  //       ),
  //       numbers AS (
  //         SELECT unnest(generate_series(0, 10))::DOUBLE as bin_number
  //       ),
  //       bin_edges AS (
  //         SELECT
  //           p05,
  //           p95,
  //           (p95 - p05) / 10.0 as bin_width,
  //           bin_number
  //         FROM stats, numbers
  //       )
  //       SELECT
  //         CAST(p05 + (bin_number * bin_width) AS DOUBLE) as x0,
  //         CAST(p05 + ((bin_number + 1.0) * bin_width) AS DOUBLE) as x1,
  //         COUNT(*) as length
  //       FROM ${this.tableName}
  //       CROSS JOIN bin_edges
  //       WHERE ${escapedColumn} IS NOT NULL
  //         AND ${escapedColumn} >= p05
  //         AND ${escapedColumn} <= p95
  //         AND ${escapedColumn} >= CAST(p05 + (bin_number * bin_width) AS DOUBLE)
  //         AND ${escapedColumn} < CAST(p05 + ((bin_number + 1.0) * bin_width) AS DOUBLE)
  //       GROUP BY bin_number, p05, bin_width
  //       ORDER BY x0;
  //     `;
  //       break;
  //     case "date":
  //       query = `
  //       SELECT
  //         date_trunc('day', ${escapedColumn}) as x0,
  //         date_trunc('day', ${escapedColumn}) + INTERVAL '1 day' as x1,
  //         COUNT(*) as length
  //       FROM ${this.tableName}
  //       WHERE ${escapedColumn} IS NOT NULL
  //       GROUP BY date_trunc('day', ${escapedColumn})
  //       ORDER BY x0
  //     `;
  //       break;
  //     case "ordinal":
  //       query = `
  //       SELECT
  //         ${escapedColumn} as key,
  //         ${escapedColumn} as x0,
  //         ${escapedColumn} as x1,
  //         COUNT(*) as length
  //       FROM ${this.tableName}
  //       WHERE ${escapedColumn} IS NOT NULL
  //       GROUP BY ${escapedColumn}
  //       ORDER BY length DESC
  //       LIMIT ${maxOrdinalBins}
  //     `;
  //       break;
  //   }

  //   console.log("Executing binning query:", {
  //     column,
  //     type,
  //     query,
  //   });

  //   const result = await this.conn.query(query);
  //   const binned = result.toArray().map((row) => ({
  //     ...row,
  //     x0: type === "date" ? new Date(row.x0) : row.x0,
  //     x1: type === "date" ? new Date(row.x1) : row.x1,
  //   }));

  //   console.log("Binning result:", binned);
  //   return binned;
  // }

  /**
   * Bins data from a DuckDB table based on the column type.
   * Uses DuckDB's built-in histogram function for continuous data.
   */
  async binDataWithDuckDB(column, type, maxOrdinalBins = 20) {
    const escapedColumn = this.safeColumnName(column);

    // Use DuckDB's native histogram function for continuous data
    if (type === "continuous") {
      // Get column type for proper casting
      const typeQuery = `SELECT typeof(${escapedColumn}) as col_type
                    FROM ${this.tableName}
                    WHERE ${escapedColumn} IS NOT NULL
                    LIMIT 1`;
      const typeResult = await this.logQuery(typeQuery, "Get Column Type");
      const typeArray = typeResult.toArray();
      const colType = typeArray.length > 0 ? typeArray[0].col_type : null;

      if (!colType) {
        console.warn(
          `Column ${escapedColumn} has no non-null values, defaulting to ordinal type.`
        );
        return this.binDataWithDuckDB(column, "ordinal", maxOrdinalBins);
      }

      // Use histogram function with filtering to exclude extreme outliers
      const query = `
      WITH stats AS (
        SELECT 
          PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY ${escapedColumn}) as p05,
          PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ${escapedColumn}) as p95
        FROM ${this.tableName}
        WHERE ${escapedColumn} IS NOT NULL
      )
      SELECT histogram(${escapedColumn}) AS hist
      FROM ${this.tableName}, stats
      WHERE ${escapedColumn} IS NOT NULL
        AND ${escapedColumn} >= p05 
        AND ${escapedColumn} <= p95
    `;

      console.log("Executing continuous binning query:", query);

      const histResult = await this.conn.query(query);
      const histData = histResult.toArray()[0]?.hist;

      if (!histData) {
        console.warn(`Column ${escapedColumn} has no histogram data`);
        return [];
      }

      // Convert histogram output to the expected format
      const bins = Object.entries(histData).map(([key, count]) => ({
        key: key === "null" ? null : parseFloat(key),
        x0: key === "null" ? null : parseFloat(key),
        x1: key === "null" ? null : parseFloat(key),
        length: count,
      }));

      // Sort by key for continuous data
      bins.sort((a, b) => {
        if (a.key === null) return -1;
        if (b.key === null) return 1;
        return a.key - b.key;
      });

      console.log("Continuous binning result:", bins);
      return bins;
    }

    // Handle date and ordinal types with existing approach
    let query;

    if (type === "date") {
      query = `
      SELECT
        date_trunc('day', ${escapedColumn}) as x0,
        date_trunc('day', ${escapedColumn}) + INTERVAL '1 day' as x1,
        COUNT(*) as length
      FROM ${this.tableName}
      WHERE ${escapedColumn} IS NOT NULL
      GROUP BY date_trunc('day', ${escapedColumn})
      ORDER BY x0
    `;
    } else if (type === "ordinal") {
      query = `
      SELECT
        ${escapedColumn} as key,
        ${escapedColumn} as x0,
        ${escapedColumn} as x1,
        COUNT(*) as length
      FROM ${this.tableName}
      WHERE ${escapedColumn} IS NOT NULL
      GROUP BY ${escapedColumn}
      ORDER BY length DESC
      LIMIT ${maxOrdinalBins}
    `;
    }

    console.log("Executing binning query:", {
      column,
      type,
      query,
    });

    const result = await this.conn.query(query);
    const binned = result.toArray().map((row) => ({
      ...row,
      x0: type === "date" ? new Date(row.x0) : row.x0,
      x1: type === "date" ? new Date(row.x1) : row.x1,
    }));

    console.log("Binning result:", binned);
    return binned;
  }

  async getQuartiles(column) {
    const query = `
      SELECT 
        MIN(${column}) as min_val,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ${column}) as q1,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ${column}) as median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ${column}) as q3,
        MAX(${column}) as max_val
      FROM ${this.tableName}
      WHERE ${column} IS NOT NULL
    `;

    const result = await this.logQuery(query, "Calculate Quartiles");
    return result.toArray()[0];
  }

  async loadData(source, format) {
    try {
      // Check if data is already loaded
      const tableExists = await this.tableExists();
      if (tableExists) {
        console.log(
          `[DuckDBProcessor] Table ${this.tableName} already exists, skipping data load`
        );
        return;
      }

      // Rest of your existing loadData code
      if (!source) {
        throw new Error("No data source provided");
      }

      // Drop existing table if it exists
      await this.conn.query(`DROP TABLE IF EXISTS ${this.tableName}`);

      if (Array.isArray(source)) {
        // Validate array data
        if (source.length === 0) {
          throw new Error("Empty data array provided");
        }
        await this.loadJSONData(source);
      } else if (source instanceof File) {
        // Handle File object
        await this.loadFileData(source, format);
      } else if (typeof source === "string") {
        if (!format) {
          throw new Error(
            "Format must be specified for URL/file path data sources"
          );
        }
        await this.loadURLData(source, format);
      } else {
        throw new Error("Unsupported data source type");
      }

      // Verify data loading
      const countResult = await this.conn.query(
        `SELECT COUNT(*) as count FROM ${this.tableName}`
      );
      const count = countResult.toArray()[0].count;

      if (count === 0) {
        throw new Error("No data was loaded");
      }

      // Check table structure
      const structureQuery = `DESCRIBE ${this.tableName}`;
      const structure = await this.conn.query(structureQuery);
      console.log("Table structure:", structure.toArray());
    } catch (error) {
      const errorMessage = `Failed to load data: ${error.message}`;
      console.error(errorMessage);
      throw new Error(errorMessage);
    }
  }

  inferSchema(obj) {
    const schema = {};
    console.log("Inferring schema for object:", obj);

    for (const [key, value] of Object.entries(obj)) {
      const inferredType = this.inferColumnType(key, value);
      schema[key] = inferredType;
      console.log(
        `Inferred type for column '${key}': ${inferredType} (value: ${value})`
      );
    }

    return schema;
  }

  inferColumnType(columnName, value) {
    // Early return for null/undefined to defer type inference to next non-null value
    if (value === null || value === undefined) {
      console.log(
        `Column '${columnName}': Null/undefined value, deferring to VARCHAR`
      );
      return "VARCHAR";
    }

    // Handle Date objects
    if (value instanceof Date) {
      console.log(`Column '${columnName}': Date detected`);
      return "TIMESTAMP";
    }

    // Handle BigInt, large integers and numeric strings
    if (
      typeof value === "bigint" ||
      (typeof value === "string" && /^\d+$/.test(value) && value.length > 9) ||
      (typeof value === "number" && (value > 2147483647 || value < -2147483648))
    ) {
      console.log(
        `Column '${columnName}': Large number detected, using VARCHAR`
      );
      return "VARCHAR";
    }

    // Handle regular numbers
    if (typeof value === "number") {
      if (Number.isInteger(value)) {
        return "INTEGER";
      }
      return "DOUBLE";
    }

    // Handle booleans
    if (typeof value === "boolean") {
      return "BOOLEAN";
    }

    // Default to VARCHAR for strings and other types
    return "VARCHAR";
  }

  async loadJSONData(data) {
    try {
      if (data.length === 0) {
        throw new Error("Empty data array provided");
      }

      // Check if table already exists
      const tableExists = await this.tableExists();
      if (tableExists) {
        console.log(
          `Table ${this.tableName} already exists, skipping data load`
        );
        return;
      }

      // Find first non-null row for schema inference
      const firstValidRow = data.find(
        (row) => row !== null && Object.keys(row).length > 0
      );
      if (!firstValidRow) {
        throw new Error("No valid data rows found for schema inference");
      }

      // Infer schema from sample data
      const sampleSize = Math.min(100, data.length);
      const dataSample = data.slice(0, sampleSize);
      const columnValues = {};

      dataSample.forEach((row) => {
        Object.entries(row).forEach(([key, value]) => {
          if (value != null) {
            columnValues[key] = columnValues[key] || [];
            columnValues[key].push(value);
          }
        });
      });

      const schema = {};
      Object.entries(columnValues).forEach(([column, values]) => {
        const lastValue = values[values.length - 1];
        schema[column] = this.inferColumnType(column, lastValue);
      });

      console.log("Inferred schema:", schema);

      // Create table with inferred schema
      const createTableSQL = this.generateCreateTableSQL(schema);
      await this.conn.query(createTableSQL);

      // Prepare bulk insert
      const columns = Object.keys(schema)
        .map((col) => `"${col}"`)
        .join(", ");
      const values = data.map((row) => {
        const rowValues = Object.entries(schema).map(([col, type]) => {
          const value = row[col];
          return this.formatValueForSQL(value, type);
        });
        return `(${rowValues.join(", ")})`;
      });

      // Insert data in larger chunks
      const chunkSize = 5000;
      for (let i = 0; i < values.length; i += chunkSize) {
        const chunk = values.slice(i, i + chunkSize);
        const insertQuery = `INSERT INTO ${
          this.tableName
        } (${columns}) VALUES ${chunk.join(", ")}`;
        await this.conn.query(insertQuery);
        console.log(
          `Inserted chunk ${Math.floor(i / chunkSize) + 1}/${Math.ceil(
            values.length / chunkSize
          )}`
        );
      }

      console.log(
        `Successfully loaded ${data.length} rows into table ${this.tableName}`
      );
    } catch (error) {
      console.error("Failed to load JSON data:", error);
      throw new Error(`Failed to load JSON data: ${error.message}`);
    }
  }

  async insertBatch(batch, schema) {
    const columns = Object.keys(schema)
      .map((col) => `"${col}"`)
      .join(", ");
    const values = batch
      .map((row) => {
        const rowValues = Object.entries(schema).map(([col, type]) => {
          const value = row[col];
          return this.formatValueForSQL(value, type);
        });
        return `(${rowValues.join(", ")})`;
      })
      .join(", ");

    const insertQuery = `INSERT INTO ${this.tableName} (${columns}) VALUES ${values}`;
    try {
      await this.conn.query(insertQuery);
    } catch (error) {
      console.error("Insert batch failed:", {
        error,
        firstRow: batch[0],
        schema,
      });
      throw error;
    }
  }

  formatValueForSQL(value, type) {
    if (value === null || value === undefined) {
      return "NULL";
    }

    switch (type) {
      case "VARCHAR":
        // Handle large numbers as strings
        if (
          typeof value === "number" &&
          (value > 2147483647 || value < -2147483648)
        ) {
          return `'${value.toString()}'`;
        }
        return `'${String(value).replace(/'/g, "''")}'`;
      case "TIMESTAMP":
        return value instanceof Date
          ? `'${value.toISOString()}'`
          : `'${value}'`;
      case "BOOLEAN":
        return value ? "TRUE" : "FALSE";
      case "INTEGER":
        // Safety check for integers
        if (value > 2147483647 || value < -2147483648) {
          return `'${value.toString()}'`; // Convert to VARCHAR if too large
        }
        return value;
      case "DOUBLE":
        return value;
      default:
        return `'${String(value).replace(/'/g, "''")}'`;
    }
  }

  async loadFileData(file, format) {
    try {
      const buffer = await file.arrayBuffer();
      const uint8Array = new Uint8Array(buffer);

      if (format === "parquet") {
        await this.duckdb.registerFileBuffer(file.name, uint8Array);
        await this.conn.query(`
          CREATE TABLE ${this.tableName} AS
          SELECT * FROM parquet_scan('${file.name}')
        `);
      } else if (format === "csv") {
        await this.duckdb.registerFileBuffer(file.name, uint8Array);
        await this.conn.query(`
          CREATE TABLE ${this.tableName} AS
          SELECT * FROM read_csv_auto('${file.name}')
        `);
      } else {
        throw new Error("Unsupported file format");
      }
    } catch (error) {
      throw new Error(`Failed to load file: ${error.message}`);
    }
  }

  async loadURLData(url, format) {
    try {
      const response = await fetch(url);
      const buffer = await response.arrayBuffer();
      const uint8Array = new Uint8Array(buffer);
      const filename = url.split("/").pop();

      await this.loadFileData(
        new File([uint8Array], filename, { type: `application/${format}` }),
        format
      );
    } catch (error) {
      throw new Error(`Failed to load URL data: ${error.message}`);
    }
  }

  // Add new method for handling unique columns
  async getUniqueColumnData(column) {
    const query = `
      SELECT DISTINCT "${column}" as value, COUNT(*) as count
      FROM ${this.tableName}
      WHERE "${column}" IS NOT NULL
      GROUP BY "${column}"
      ORDER BY value
    `;

    const result = await this.query(query);
    return result.map((row) => ({
      key: row.value,
      x0: row.value,
      x1: row.value,
      length: row.count,
    }));
  }

  escape(name) {
    return `"${name}"`;
  }

  async describeColumn(column) {
    const query = `DESCRIBE ${this.escape(this.tableName)}`;
    const result = await this.conn.query(query);
    const columnInfo = result
      .toArray()
      .find((row) => row.column_name === column);
    return {
      name: columnInfo.column_name,
      type: this.getDuckDBType(columnInfo.column_type),
      nullable: columnInfo.null !== "NO",
      databaseType: columnInfo.column_type,
    };
  }

  generateCreateTableSQL(schema) {
    const columns = Object.entries(schema)
      .map(([name, type]) => `"${name}" ${type}`)
      .join(", ");
    return `CREATE TABLE ${this.tableName} (${columns})`;
  }

  // Add this method to your Histogram class
  async logQuery(query, context = "") {
    console.group(`DuckDB Query: ${context}`);
    console.log("SQL:", query);
    try {
      const result = await this.conn.query(query);
      console.log("Result:", result.toArray());
      console.groupEnd();
      return result;
    } catch (error) {
      console.error("Query Error:", error);
      console.groupEnd();
      throw error;
    }
  }

  async query(sql) {
    try {
      if (!this.conn) {
        console.log(
          "[DuckDBProcessor] No connection, attempting to connect..."
        );
        await this.connect();
      }

      const result = await this.conn.query(sql);
      return result.toArray();
    } catch (error) {
      console.error("[DuckDBProcessor] Query error:", error);
      throw error;
    }
  }

  async close() {
    if (this.conn) {
      await this.conn.close();
    }
  }

  async terminate() {
    if (this.duckdb) {
      await this.duckdb.terminate();
    }
  }

  async dropTable() {
    if (this.conn) {
      await this.conn.query(`DROP TABLE IF EXISTS ${this.tableName}`);
    }
  }

  async aggregateData({ column, aggregation = "SUM", groupBy = null }) {
    try {
      const escapedColumn = this.safeColumnName(column);
      const escapedGroupBy = groupBy ? this.safeColumnName(groupBy) : null;

      let query;
      if (escapedGroupBy) {
        query = `
          SELECT ${escapedGroupBy} as group_key, ${aggregation}(${escapedColumn}) as aggregate_value
        FROM ${this.tableName}
          GROUP BY ${escapedGroupBy}
        ORDER BY aggregate_value DESC
      `;
      } else {
        query = `
          SELECT ${aggregation}(${escapedColumn}) as aggregate_value
        FROM ${this.tableName}
      `;
      }

      const result = await this.conn.query(query);
      return result.toArray();
    } catch (error) {
      throw new Error(`Aggregation query failed: ${error.message}`);
    }
  }

  async getSummaryStatistics(column) {
    // Returns basic summary statistics for a numeric column using DuckDB's native functions
    const query = `
      SELECT 
        COUNT(${column}) as count,
        AVG(${column}) as avg,
        MIN(${column}) as min,
        MAX(${column}) as max,
        STDDEV(${column}) as stddev
      FROM ${this.tableName}
      WHERE ${column} IS NOT NULL
    `;
    try {
      const result = await this.conn.query(query);
      return result.toArray()[0];
    } catch (error) {
      throw new Error(`Summary statistics query failed: ${error.message}`);
    }
  }

  async getSortedData({ sortColumns, order = "ASC" }) {
    // sortColumns: Array of column names
    const orderBy = sortColumns.map((col) => `${col} ${order}`).join(", ");
    const query = `
      SELECT *
      FROM ${this.tableName}
      ORDER BY ${orderBy}
    `;
    try {
      const result = await this.conn.query(query);
      return result.toArray();
    } catch (error) {
      throw new Error(`Sorting query failed: ${error.message}`);
    }
  }

  async getFilteredData(filterClause) {
    // filterClause: a valid SQL WHERE clause (e.g., "age > 30 AND status = 'active'")
    const query = `
      SELECT *
      FROM ${this.tableName}
      WHERE ${filterClause}
    `;
    try {
      const result = await this.conn.query(query);
      return result.toArray();
    } catch (error) {
      throw new Error(`Filtering query failed: ${error.message}`);
    }
  }

  async getDataPage({
    page = 0,
    pageSize = 100,
    sortColumns = [],
    order = "ASC",
    filterClause = "1=1",
  }) {
    // Lazy loading / pagination
    const offset = page * pageSize;
    let orderBy = "";
    if (sortColumns.length) {
      orderBy = `ORDER BY ${sortColumns
        .map((col) => `${col} ${order}`)
        .join(", ")}`;
    }
    const query = `
      SELECT *
      FROM ${this.tableName}
      WHERE ${filterClause}
      ${orderBy}
      LIMIT ${pageSize} OFFSET ${offset}
    `;
    try {
      const result = await this.conn.query(query);
      return result.toArray();
    } catch (error) {
      throw new Error(`Pagination query failed: ${error.message}`);
    }
  }

  async applyFilter(filterConditions) {
    try {
      // Process filter conditions to ensure safe column names
      const processedConditions = filterConditions.map((condition) => {
        const { column, operator, value } = condition;
        const escapedColumn = this.safeColumnName(column);
        const escapedValue =
          typeof value === "string" ? `'${value.replace(/'/g, "''")}'` : value;
        return `${escapedColumn} ${operator} ${escapedValue}`;
      });

      const whereClause = processedConditions.join(" AND ");
      const query = `
        SELECT *, ROWID
        FROM ${this.tableName}
        WHERE ${whereClause}
        LIMIT ${this.options.rowsPerPage}
      `;

      return await this.conn.query(query);
    } catch (error) {
      console.error("Filter query failed:", error);
      throw error;
    }
  }

  async applySorting(sortColumns) {
    try {
      // Process sort columns to ensure safe column names
      const orderByClause = sortColumns
        .map((sort) => {
          const { column, direction } = sort;
          const escapedColumn = this.safeColumnName(column);
          return `${escapedColumn} ${direction || "ASC"}`;
        })
        .join(", ");

      const query = `
        SELECT *, ROWID
        FROM ${this.tableName}
        ORDER BY ${orderByClause}
        LIMIT ${this.options.rowsPerPage}
      `;

      return await this.conn.query(query);
    } catch (error) {
      console.error("Sort query failed:", error);
      throw error;
    }
  }

  async getValueDistribution(column, bins = 10) {
    try {
      const escapedColumn = this.safeColumnName(column);
      const query = `
        WITH stats AS (
          SELECT 
            MIN(${escapedColumn}) as min_val,
            MAX(${escapedColumn}) as max_val,
            (MAX(${escapedColumn}) - MIN(${escapedColumn})) / ${bins} as bin_width
          FROM ${this.tableName}
          WHERE ${escapedColumn} IS NOT NULL
        )
        SELECT 
          min_val + (bucket * bin_width) as bin_start,
          min_val + ((bucket + 1) * bin_width) as bin_end,
          COUNT(*) as count
        FROM ${this.tableName}, stats
        CROSS JOIN generate_series(0, ${bins - 1}) as t(bucket)
        WHERE ${escapedColumn} >= min_val + (bucket * bin_width)
          AND ${escapedColumn} < min_val + ((bucket + 1) * bin_width)
        GROUP BY bucket, min_val, bin_width
        ORDER BY bin_start;
      `;

      return await this.conn.query(query);
    } catch (error) {
      console.error("Distribution query failed:", error);
      throw error;
    }
  }

  /**
   * Execute a natural language query and return the results
   * @param {string} nlQuery - Natural language query
   * @param {Object} options - Query options
   * @returns {Promise<Array>} - Query results
   */
  async executeNaturalLanguageQuery(nlQuery, options = {}) {
    try {
      if (!this.conn) {
        await this.connect();
      }

      console.log(
        `[DuckDBProcessor] Processing natural language query: ${nlQuery}`
      );

      // Get column metadata for better query understanding
      const columns = await this.getColumnMetadata();

      // Parse natural language query
      const sqlQuery = this.parseNaturalLanguageQuery(
        nlQuery,
        columns,
        options
      );

      console.log(`[DuckDBProcessor] Translated to SQL: ${sqlQuery}`);

      // Execute the SQL query
      const result = await this.query(sqlQuery);
      return result;
    } catch (error) {
      console.error("[DuckDBProcessor] Natural language query error:", error);
      throw error;
    }
  }

  /**
   * Parse natural language into SQL
   * @param {string} nlQuery - Natural language query
   * @param {Array} columns - Column metadata
   * @param {Object} options - Additional options
   * @returns {string} - SQL query
   */
  parseNaturalLanguageQuery(nlQuery, columns, options = {}) {
    const query = nlQuery.toLowerCase();
    const limit = options.limit || 1000;

    // Try to match percentile-related queries
    if (query.includes("percentile")) {
      return this.handlePercentileQuery(query, columns);
    }

    // Try to match statistical queries
    if (this.containsStatisticalTerms(query)) {
      return this.handleStatisticalQuery(query, columns);
    }

    // Try to match comparison queries
    if (this.containsComparisonTerms(query)) {
      return this.handleComparisonQuery(query, columns);
    }

    // Default to a simple SELECT query with limit
    return `SELECT * FROM ${this.tableName} LIMIT ${limit}`;
  }

  /**
   * Check if the query contains statistical terms
   * @param {string} query - The natural language query
   * @returns {boolean} - Whether the query contains statistical terms
   */
  containsStatisticalTerms(query) {
    const terms = [
      "average",
      "mean",
      "median",
      "mode",
      "maximum",
      "max",
      "minimum",
      "min",
      "sum",
      "total",
      "count",
      "standard deviation",
      "variance",
      "outlier",
      "anomaly",
      "distribution",
    ];

    return terms.some((term) => query.includes(term));
  }

  /**
   * Check if the query contains comparison terms
   * @param {string} query - The natural language query
   * @returns {boolean} - Whether the query contains comparison terms
   */
  containsComparisonTerms(query) {
    const terms = [
      "greater than",
      "less than",
      "equal to",
      "between",
      "more than",
      "less than",
      "at least",
      "at most",
      "higher",
      "lower",
      "above",
      "below",
    ];

    return terms.some((term) => query.includes(term));
  }

  /**
   * Handle percentile-based queries
   * @param {string} query - The natural language query
   * @param {Array} columns - Column metadata
   * @returns {string} - SQL query
   */
  handlePercentileQuery(query, columns) {
    // Extract column name and percentile value
    const column = this.findMostLikelyColumn(query, columns);
    if (!column) {
      throw new Error("Column not specified or recognized in the query");
    }

    // Extract percentile value
    let percentile = 0.5; // Default to median (50th percentile)
    const percentileMatch = query.match(/(\d+)(st|nd|rd|th)?\s+percentile/i);
    if (percentileMatch) {
      percentile = parseInt(percentileMatch[1], 10) / 100;
    } else if (query.includes("first percentile")) {
      percentile = 0.01;
    } else if (query.includes("last percentile")) {
      percentile = 0.99;
    }

    // Determine if we're looking for values above or below the percentile
    if (
      query.includes("above") ||
      query.includes("greater than") ||
      query.includes("more than")
    ) {
      return `
        SELECT * FROM ${this.tableName}
        WHERE "${column}" > (
          SELECT PERCENTILE_CONT(${percentile}) WITHIN GROUP (ORDER BY "${column}")
          FROM ${this.tableName}
        )
        ORDER BY "${column}" DESC
        LIMIT 1000
      `;
    } else if (query.includes("below") || query.includes("less than")) {
      return `
        SELECT * FROM ${this.tableName}
        WHERE "${column}" < (
          SELECT PERCENTILE_CONT(${percentile}) WITHIN GROUP (ORDER BY "${column}")
          FROM ${this.tableName}
        )
        ORDER BY "${column}" ASC
        LIMIT 1000
      `;
    } else {
      // Just return the percentile value
      return `
        SELECT PERCENTILE_CONT(${percentile}) WITHIN GROUP (ORDER BY "${column}") as percentile_value
        FROM ${this.tableName}
      `;
    }
  }

  /**
   * Handle statistical queries
   * @param {string} query - The natural language query
   * @param {Array} columns - Column metadata
   * @returns {string} - SQL query
   */
  handleStatisticalQuery(query, columns) {
    const column = this.findMostLikelyColumn(query, columns);
    if (!column) {
      throw new Error("Column not specified or recognized in the query");
    }

    // Select appropriate statistical operation
    if (query.includes("average") || query.includes("mean")) {
      if (
        query.includes("above") ||
        query.includes("greater than") ||
        query.includes("higher than")
      ) {
        return `
          WITH avg_val AS (SELECT AVG("${column}") as avg FROM ${this.tableName})
          SELECT * FROM ${this.tableName}, avg_val
          WHERE "${column}" > avg_val.avg
          ORDER BY "${column}" DESC
          LIMIT 1000
        `;
      } else if (
        query.includes("below") ||
        query.includes("less than") ||
        query.includes("lower than")
      ) {
        return `
          WITH avg_val AS (SELECT AVG("${column}") as avg FROM ${this.tableName})
          SELECT * FROM ${this.tableName}, avg_val
          WHERE "${column}" < avg_val.avg
          ORDER BY "${column}" ASC
          LIMIT 1000
        `;
      } else {
        return `SELECT AVG("${column}") as average_value FROM ${this.tableName}`;
      }
    }

    if (query.includes("median")) {
      return `SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "${column}") as median_value FROM ${this.tableName}`;
    }

    if (
      query.includes("maximum") ||
      query.includes("max") ||
      query.includes("highest") ||
      query.includes("largest")
    ) {
      return `
        WITH max_val AS (SELECT MAX("${column}") as max FROM ${this.tableName})
        SELECT * FROM ${this.tableName}, max_val
        WHERE "${column}" = max_val.max
        LIMIT 1000
      `;
    }

    if (
      query.includes("minimum") ||
      query.includes("min") ||
      query.includes("lowest") ||
      query.includes("smallest")
    ) {
      return `
        WITH min_val AS (SELECT MIN("${column}") as min FROM ${this.tableName})
        SELECT * FROM ${this.tableName}, min_val
        WHERE "${column}" = min_val.min
        LIMIT 1000
      `;
    }

    if (query.includes("outlier") || query.includes("anomaly")) {
      return `
        WITH stats AS (
          SELECT 
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "${column}") AS q1,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "${column}") AS q3
          FROM ${this.tableName}
        )
        SELECT * FROM ${this.tableName}
        CROSS JOIN stats
        WHERE "${column}" < (q1 - 1.5 * (q3 - q1)) OR "${column}" > (q3 + 1.5 * (q3 - q1))
        ORDER BY "${column}" DESC
        LIMIT 1000
      `;
    }

    if (query.includes("distribution")) {
      return `
        SELECT 
          MIN("${column}") as min_value,
          PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "${column}") as q1,
          PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "${column}") as median,
          AVG("${column}") as mean,
          PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "${column}") as q3,
          MAX("${column}") as max_value,
          STDDEV("${column}") as std_dev
        FROM ${this.tableName}
      `;
    }

    // Default statistical summary
    return `
      SELECT 
        COUNT(*) as count,
        MIN("${column}") as min_value,
        MAX("${column}") as max_value,
        AVG("${column}") as avg_value,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "${column}") as median_value
      FROM ${this.tableName}
    `;
  }

  /**
   * Handle comparison queries
   * @param {string} query - The natural language query
   * @param {Array} columns - Column metadata
   * @returns {string} - SQL query
   */
  handleComparisonQuery(query, columns) {
    const column = this.findMostLikelyColumn(query, columns);
    if (!column) {
      throw new Error("Column not specified or recognized in the query");
    }

    // Extract comparison value
    let value = null;
    let operator = null;

    // Check for common comparisons
    if (
      query.includes("greater than") ||
      query.includes("more than") ||
      query.includes("higher than")
    ) {
      operator = ">";
      const match = query.match(/(greater|more|higher) than\s+(\d+(\.\d+)?)/i);
      if (match) value = match[2];
    } else if (
      query.includes("less than") ||
      query.includes("lower than") ||
      query.includes("smaller than")
    ) {
      operator = "<";
      const match = query.match(/(less|lower|smaller) than\s+(\d+(\.\d+)?)/i);
      if (match) value = match[2];
    } else if (query.includes("equal to") || query.includes("equals")) {
      operator = "=";
      const match = query.match(/(equal to|equals)\s+(\d+(\.\d+)?)/i);
      if (match) value = match[2];
    }

    if (operator && value !== null) {
      return `
        SELECT * FROM ${this.tableName}
        WHERE "${column}" ${operator} ${value}
        ORDER BY "${column}" ${operator === ">" ? "DESC" : "ASC"}
        LIMIT 1000
      `;
    }

    // Check for "between" queries
    if (query.includes("between")) {
      const match = query.match(
        /between\s+(\d+(\.\d+)?)\s+and\s+(\d+(\.\d+)?)/i
      );
      if (match) {
        const lowerBound = match[1];
        const upperBound = match[3];
        return `
          SELECT * FROM ${this.tableName}
          WHERE "${column}" BETWEEN ${lowerBound} AND ${upperBound}
          ORDER BY "${column}" ASC
          LIMIT 1000
        `;
      }
    }

    // Default to simple ordering
    if (query.includes("ascending") || query.includes("increasing")) {
      return `
        SELECT * FROM ${this.tableName}
        ORDER BY "${column}" ASC
        LIMIT 1000
      `;
    } else if (query.includes("descending") || query.includes("decreasing")) {
      return `
        SELECT * FROM ${this.tableName}
        ORDER BY "${column}" DESC
        LIMIT 1000
      `;
    }

    // If we can't determine a comparison, just return all data sorted by the column
    return `
      SELECT * FROM ${this.tableName}
      ORDER BY "${column}" ASC
      LIMIT 1000
    `;
  }

  /**
   * Find the column most likely referenced in a query
   * @param {string} query - The natural language query
   * @param {Array} columns - Column metadata
   * @returns {string|null} - Column name or null if not found
   */
  findMostLikelyColumn(query, columns) {
    // Exact column name match
    for (const col of columns) {
      if (query.includes(col.name.toLowerCase())) {
        return col.name;
      }
    }

    // Match column name with spaces instead of underscores
    for (const col of columns) {
      const formattedName = col.name.toLowerCase().replace(/_/g, " ");
      if (query.includes(formattedName)) {
        return col.name;
      }
    }

    // If the query mentions only one numeric column, use it
    const numericColumns = columns.filter((col) => col.isNumeric);
    if (numericColumns.length === 1) {
      return numericColumns[0].name;
    }

    // If no matches, try to find the most relevant numeric column based on query context
    // This is a simple heuristic that could be improved
    if (query.includes("price") || query.includes("cost")) {
      const priceColumn = columns.find(
        (col) =>
          col.name.toLowerCase().includes("price") ||
          col.name.toLowerCase().includes("cost") ||
          col.name.toLowerCase().includes("amount")
      );
      if (priceColumn) return priceColumn.name;
    }

    if (
      query.includes("date") ||
      query.includes("time") ||
      query.includes("when")
    ) {
      const dateColumn = columns.find(
        (col) =>
          col.name.toLowerCase().includes("date") ||
          col.name.toLowerCase().includes("time") ||
          col.name.toLowerCase().includes("year")
      );
      if (dateColumn) return dateColumn.name;
    }

    if (query.includes("age") || query.includes("old")) {
      const ageColumn = columns.find(
        (col) =>
          col.name.toLowerCase().includes("age") ||
          col.name.toLowerCase().includes("year")
      );
      if (ageColumn) return ageColumn.name;
    }

    // Return null if no suitable column found
    return null;
  }

  /**
   * Get metadata for all columns in the table
   * @returns {Promise<Array>} - Column metadata
   */
  async getColumnMetadata() {
    try {
      const columnsQuery = `DESCRIBE ${this.tableName}`;
      const result = await this.conn.query(columnsQuery);
      const columns = result.toArray();

      return columns.map((col) => ({
        name: col.column_name,
        type: col.column_type,
        isNumeric: [
          "INTEGER",
          "BIGINT",
          "DOUBLE",
          "FLOAT",
          "DECIMAL",
          "HUGEINT",
          "TINYINT",
          "SMALLINT",
          "UBIGINT",
          "UINTEGER",
          "USMALLINT",
          "UTINYINT",
        ].some((t) => col.column_type.toUpperCase().includes(t)),
      }));
    } catch (error) {
      console.error("[DuckDBProcessor] Error getting column metadata:", error);
      return [];
    }
  }
}
