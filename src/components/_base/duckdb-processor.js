import * as duckdb from "npm:@duckdb/duckdb-wasm";

export class DuckDBDataProcessor {
  constructor(duckdbConnection, tableName) {
    this.duckdb = duckdbConnection;
    this.tableName = tableName;
    this.conn = null; // DuckDB connection
  }

  async connect() {
    if (!this.duckdb) {
      try {
        // Import DuckDB only when needed
        // Define the bundles for DuckDB
        const bundle = await duckdb.selectBundle({
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
        });

        const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
        const worker = new Worker(bundle.mainWorker);

        this.duckdb = new duckdb.AsyncDuckDB(logger, worker);
        await this.duckdb.instantiate(bundle.mainModule);
        this.conn = await this.duckdb.connect();
      } catch (error) {
        throw new Error(`Failed to initialize DuckDB: ${error.message}`);
      }
    }

    try {
      // Close any existing connection before creating a new one
      if (this.conn) {
        await this.conn.close();
      }
      this.conn = await this.duckdb.connect();

      // Clean up any existing table with the same name
      await this.conn.query(`DROP TABLE IF EXISTS ${this.tableName}`);
    } catch (error) {
      throw new Error(
        `Failed to establish DuckDB connection: ${error.message}`
      );
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

  // async binDataWithDuckDB(column, type, maxOrdinalBins = 20) {
  //   let query;

  //   switch (type) {
  //     case "continuous":
  //       // Query the column type to ensure proper casting.
  //       const typeQuery = `
  //         SELECT typeof(${column}) as col_type
  //         FROM ${this.tableName}
  //         WHERE ${column} IS NOT NULL
  //         LIMIT 1
  //       `;
  //       const typeResult = await this.logQuery(typeQuery, "Get Column Type");
  //       const typeArray = typeResult.toArray();
  //       const colType = typeArray.length > 0 ? typeArray[0].col_type : null;

  //       // If there are no non-null values, fall back to ordinal binning.
  //       if (!colType) {
  //         console.warn(
  //           `Column ${column} has no non-null values, defaulting to ordinal type.`
  //         );
  //         return this.binDataWithDuckDB(column, "ordinal", maxOrdinalBins);
  //       }

  //       // Map the column type to a DuckDB numeric type.
  //       const numericType = this.getDuckDBType(colType);

  //       const rangeQuery = `
  //         SELECT
  //           MIN(${column}) as min_val,
  //           PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY ${column}) as p01_val,
  //           PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY ${column}) as p99_val,
  //           MAX(${column}) as max_val,
  //           COUNT(*) as total_count
  //         FROM ${this.tableName}
  //         WHERE ${column} IS NOT NULL AND ${column} > 0
  //       `;
  //       const rangeResult = await this.logQuery(rangeQuery, "Get Range");
  //       const range = rangeResult.toArray()[0];

  //       if (!range.min_val || !range.p99_val) {
  //         console.warn(
  //           `Column ${column} has no positive values, falling back to regular binning.`
  //         );
  //         return this.binDataWithDuckDB(column, "ordinal", maxOrdinalBins);
  //       }

  //       // Calculate Quartiles and IQR for positive values
  //       const quartilesQuery = `
  //         SELECT
  //           PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ${column}) as q1,
  //           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ${column}) as q3
  //         FROM ${this.tableName}
  //         WHERE ${column} IS NOT NULL AND ${column} > 0
  //       `;
  //       const quartilesResult = await this.logQuery(
  //         quartilesQuery,
  //         "Get Quartiles for IQR"
  //       );
  //       const quartiles = quartilesResult.toArray()[0];
  //       const q1 = quartiles.q1;
  //       const q3 = quartiles.q3;
  //       const iqr = q3 - q1;
  //       const upperFence = q3 + 1.5 * iqr;

  //       // Determine mainUpperBound using the IQR method
  //       const mainUpperBound = Math.min(upperFence, range.max_val);
  //       const mainLowerBound = range.p01_val || range.min_val;

  //       const outlierCapMultiplier = 1.2;
  //       const outlierCap = Math.min(
  //         range.max_val,
  //         mainUpperBound * outlierCapMultiplier
  //       );

  //       // The following query constructs bins using several common table expressions (CTEs):
  //       // 1. main_data: Selects values between mainLowerBound and mainUpperBound.
  //       // 2. bin_bounds: Computes logarithmic spacing parameters for 10 bins over the main range.
  //       // 3. bin_edges: Generates logarithmically spaced edges.
  //       // 4. numbered_edges: Numbers the edges sequentially.
  //       // 5. bins: Aggregates the counts for each main bin.
  //       // 6. underliers: Captures data below mainLowerBound.
  //       // 7. outliers: Captures data above mainUpperBound.
  //       // The final SELECT unions these results and orders them by x0.
  //       query = `
  //         WITH main_data AS (
  //           SELECT ${column}
  //           FROM ${this.tableName}
  //           WHERE ${column} IS NOT NULL
  //             AND ${column} >= ${mainLowerBound}
  //             AND ${column} <= ${mainUpperBound}
  //         ),
  //         bin_bounds AS (
  //           SELECT
  //             ${mainLowerBound} as min_val,
  //             ${mainUpperBound} as max_val,
  //             (LN(${mainUpperBound}) - LN(${mainLowerBound})) / 10.0 as log_width
  //         ),
  //         bin_edges AS (
  //           SELECT
  //             EXP(LN(min_val) + (value * log_width)) as edge
  //           FROM bin_bounds, generate_series(0, 10) as g(value)
  //         ),
  //         numbered_edges AS (
  //           SELECT
  //             edge,
  //             ROW_NUMBER() OVER (ORDER BY edge) as rn
  //           FROM bin_edges
  //         ),
  //         bins AS (
  //           SELECT
  //             e1.edge as x0,
  //             e2.edge as x1,
  //             COUNT(d.${column}) as length
  //           FROM numbered_edges e1
  //           JOIN numbered_edges e2 ON e2.rn = e1.rn + 1
  //           LEFT JOIN main_data d
  //             ON d.${column} >= e1.edge
  //             AND d.${column} < e2.edge
  //           GROUP BY e1.edge, e2.edge, e1.rn
  //           HAVING e1.edge < e2.edge
  //         ),
  //         underliers AS (
  //           SELECT
  //             'lower' as type,
  //             ${range.min_val} as x0,
  //             ${mainLowerBound} as x1,
  //             COUNT(*) as length
  //           FROM ${this.tableName}
  //           WHERE ${column} < ${mainLowerBound}
  //           HAVING COUNT(*) > 0
  //         ),
  //         outliers AS (
  //           SELECT
  //             'upper' as type,
  //             ${mainUpperBound} as x0,
  //             ${outlierCap} as x1,
  //             COUNT(*) as length
  //           FROM ${this.tableName}
  //           WHERE ${column} > ${mainUpperBound}
  //           HAVING COUNT(*) > 0
  //         )
  //         SELECT x0, x1, length FROM bins
  //         UNION ALL
  //         SELECT x0, x1, length FROM underliers
  //         UNION ALL
  //         SELECT x0, x1, length FROM outliers
  //         ORDER BY x0
  //       `;
  //       break;

  //     case "date":
  //       // For date columns, group data by day.
  //       query = `
  //         SELECT
  //           date_trunc('day', ${column}) as x0,
  //           date_trunc('day', ${column}) + INTERVAL '1 day' as x1,
  //           COUNT(*) as length
  //         FROM ${this.tableName}
  //         WHERE ${column} IS NOT NULL
  //         GROUP BY date_trunc('day', ${column})
  //         ORDER BY x0
  //       `;
  //       break;

  //     case "ordinal":
  //       // For ordinal data, group by distinct values and limit to maxOrdinalBins.
  //       query = `
  //         SELECT
  //           ${column} as key,
  //           ${column} as x0,
  //           ${column} as x1,
  //           COUNT(*) as length
  //         FROM ${this.tableName}
  //         WHERE ${column} IS NOT NULL
  //         GROUP BY ${column}
  //         ORDER BY length DESC
  //         LIMIT ${maxOrdinalBins}
  //       `;
  //       break;
  //   }

  //   // Execute the constructed query.
  //   const result = await this.conn.query(query);
  //   let bins = result.toArray().map((row) => ({
  //     ...row,
  //     // Convert date strings to Date objects when binning dates.
  //     x0: type === "date" ? new Date(row.x0) : row.x0,
  //     x1: type === "date" ? new Date(row.x1) : row.x1,
  //   }));

  //   console.log(">>> Bins for data:", column, bins);

  //   // For ordinal data, if the maximum distinct bins are reached,
  //   // compute an additional "Other" bin for remaining categories.
  //   if (type === "ordinal" && bins.length === maxOrdinalBins) {
  //     const othersQuery = `
  //       WITH ranked AS (
  //         SELECT ${column}, COUNT(*) as cnt
  //         FROM ${this.tableName}
  //         WHERE ${column} IS NOT NULL
  //         GROUP BY ${column}
  //         ORDER BY cnt DESC
  //         OFFSET ${maxOrdinalBins - 1}
  //       )
  //       SELECT SUM(cnt) as length
  //       FROM ranked
  //     `;
  //     const othersResult = await this.conn.query(othersQuery);
  //     const othersCount = othersResult.toArray()[0].length;

  //     if (othersCount > 0) {
  //       bins.push({
  //         key: "Other",
  //         x0: "Other",
  //         x1: "Other",
  //         length: othersCount,
  //       });
  //     }
  //   }

  //   return bins;
  // }

  /**
   * Bins data from a DuckDB table based on the column type.
   * For continuous data, creates equal-width bins between 5th and 95th percentiles.
   */
  async binDataWithDuckDB(column, type, maxOrdinalBins = 20) {
    let query;

    switch (type) {
      case "continuous":
        // Get column type for proper casting
        const typeQuery = `SELECT typeof(${column}) as col_type
                        FROM ${this.tableName}
                        WHERE ${column} IS NOT NULL
                        LIMIT 1`;
        const typeResult = await this.logQuery(typeQuery, "Get Column Type");
        const typeArray = typeResult.toArray();
        const colType = typeArray.length > 0 ? typeArray[0].col_type : null;

        if (!colType) {
          console.warn(
            `Column ${column} has no non-null values, defaulting to ordinal type.`
          );
          return this.binDataWithDuckDB(column, "ordinal", maxOrdinalBins);
        }

        // Create 10 equal-width bins between 5th and 95th percentiles
        query = `
        WITH stats AS (
          SELECT 
            PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY ${column}) as p05,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ${column}) as p95
          FROM ${this.tableName}
          WHERE ${column} IS NOT NULL
        ),
        numbers AS (
          SELECT unnest(generate_series(0, 10))::DOUBLE as bin_number
        ),
        bin_edges AS (
          SELECT 
            p05,
            p95,
            (p95 - p05) / 10.0 as bin_width,
            bin_number
          FROM stats, numbers
        )
        SELECT
          CAST(p05 + (bin_number * bin_width) AS DOUBLE) as x0,
          CAST(p05 + ((bin_number + 1.0) * bin_width) AS DOUBLE) as x1,
          COUNT(*) as length
        FROM ${this.tableName}
        CROSS JOIN bin_edges
        WHERE ${column} IS NOT NULL
          AND ${column} >= p05 
          AND ${column} <= p95
          AND ${column} >= CAST(p05 + (bin_number * bin_width) AS DOUBLE)
          AND ${column} < CAST(p05 + ((bin_number + 1.0) * bin_width) AS DOUBLE)
        GROUP BY bin_number, p05, bin_width
        ORDER BY x0;
      `;
        break;

      case "date":
        query = `
        SELECT
          date_trunc('day', ${column}) as x0,
          date_trunc('day', ${column}) + INTERVAL '1 day' as x1,
          COUNT(*) as length
        FROM ${this.tableName}
        WHERE ${column} IS NOT NULL
        GROUP BY date_trunc('day', ${column})
        ORDER BY x0
      `;
        break;

      case "ordinal":
        query = `
        SELECT
          ${column} as key,
          ${column} as x0,
          ${column} as x1,
          COUNT(*) as length
        FROM ${this.tableName}
        WHERE ${column} IS NOT NULL
        GROUP BY ${column}
        ORDER BY length DESC
        LIMIT ${maxOrdinalBins}
      `;
        break;
    }

    const result = await this.conn.query(query);
    return result.toArray().map((row) => ({
      ...row,
      x0: type === "date" ? new Date(row.x0) : row.x0,
      x1: type === "date" ? new Date(row.x1) : row.x1,
    }));
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
      // Validate input
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

  async loadJSONData(data) {
    try {
      if (data.length === 0) {
        throw new Error("Empty data array provided");
      }

      // Infer schema from the first object
      const schema = this.inferSchema(data[0]);

      // Create table with properly escaped column names
      const columns = Object.entries(schema)
        .map(([name, type]) => `${this.safeColumnName(name)} ${type}`)
        .join(", ");

      const createTableSQL = `CREATE TABLE ${this.tableName} (${columns})`;
      await this.conn.query(createTableSQL);

      // Insert data in batches using SQL INSERT with escaped column names
      const batchSize = 1000;
      for (let i = 0; i < data.length; i += batchSize) {
        const batch = data.slice(i, i + batchSize);

        // Generate INSERT query with escaped column names
        const columnList = Object.keys(schema)
          .map((col) => this.safeColumnName(col))
          .join(", ");

        const values = batch
          .map((row) => {
            const rowValues = Object.keys(schema).map((col) => {
              let val = row[col];
              if (val === null || val === undefined) return "NULL";

              // Format Date values to 'YYYY-MM-DD HH:MM:SS'
              if (val instanceof Date) {
                val = val.toISOString().slice(0, 19).replace("T", " ");
              }
              // Convert all values to strings for consistency with schema
              return `'${String(val).replace(/'/g, "''")}'`;
            });
            return `(${rowValues.join(", ")})`;
          })
          .join(", ");

        const insertQuery = `INSERT INTO ${this.tableName} (${columnList}) VALUES ${values}`;
        await this.conn.query(insertQuery);
      }
    } catch (error) {
      console.error("Failed to load JSON data:", error);
      throw new Error(`Failed to load JSON data: ${error.message}`);
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

  inferSchema(obj) {
    const schema = {};
    for (const [key, value] of Object.entries(obj)) {
      if (value instanceof Date) {
        schema[key] = "TIMESTAMP";
      } else if (typeof value === "number") {
        schema[key] = Number.isInteger(value) ? "INTEGER" : "DOUBLE";
      } else if (typeof value === "boolean") {
        schema[key] = "BOOLEAN";
      } else {
        schema[key] = "VARCHAR";
      }
    }
    return schema;
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
    const result = await this.conn.query(sql);
    return result.toArray();
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
}
