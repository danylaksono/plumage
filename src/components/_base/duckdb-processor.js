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

      // Find first non-null row for schema inference
      const firstValidRow = data.find(
        (row) => row !== null && Object.keys(row).length > 0
      );
      if (!firstValidRow) {
        throw new Error("No valid data rows found for schema inference");
      }

      // Analyze sample of data for better type inference
      const sampleSize = Math.min(100, data.length);
      const dataSample = data.slice(0, sampleSize);
      console.log(`Analyzing ${sampleSize} rows for type inference`);

      // Collect all non-null values for each column
      const columnValues = {};
      dataSample.forEach((row) => {
        Object.entries(row).forEach(([key, value]) => {
          if (value != null) {
            columnValues[key] = columnValues[key] || [];
            columnValues[key].push(value);
          }
        });
      });

      // Infer schema using the most appropriate type for each column
      const schema = {};
      Object.entries(columnValues).forEach(([column, values]) => {
        // Try to infer type from the most recent non-null value
        const lastValue = values[values.length - 1];
        schema[column] = this.inferColumnType(column, lastValue);
      });

      console.log("Inferred schema:", schema);

      // Create table with inferred schema
      const createTableSQL = this.generateCreateTableSQL(schema);
      console.log("Creating table with SQL:", createTableSQL);
      await this.conn.query(createTableSQL);

      // Insert data in batches
      const batchSize = 1000;
      for (let i = 0; i < data.length; i += batchSize) {
        const batch = data.slice(i, i + batchSize);
        await this.insertBatch(batch, schema);
        console.log(
          `Inserted batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(
            data.length / batchSize
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
