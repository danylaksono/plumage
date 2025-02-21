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
      throw new Error(`Failed to establish DuckDB connection: ${error.message}`);
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
      console.error('Error during cleanup:', error);
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
    const columnName = typeof column === 'string' ? column : column.column;
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
        return "ordinal";
      }

      const type = resultArray[0].col_type.toLowerCase();

      // Map DuckDB types to our column types
      if (type.includes('varchar') || type.includes('text')) {
        return "ordinal";
      }
      if (type.includes('float') || type.includes('double') || type.includes('decimal') || 
          type.includes('integer') || type.includes('bigint')) {
        return "continuous";
      }
      if (type.includes('date') || type.includes('timestamp')) {
        return "date";
      }
      return "ordinal";
    } catch (error) {
      console.error('Error getting type from DuckDB:', error);
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

  async binDataWithDuckDB(column, type, maxOrdinalBins = 20, filterClause = "1=1") {
    try {
      const escapedColumn = this.safeColumnName(column);
      
      // Check if this is a unique/ID column
      const uniqueCheckQuery = `
        SELECT COUNT(*) as total_count, COUNT(DISTINCT ${escapedColumn}) as unique_count
        FROM ${this.tableName}
        WHERE ${escapedColumn} IS NOT NULL
      `;
      const uniqueCheck = await this.conn.query(uniqueCheckQuery);
      const result = uniqueCheck.toArray()[0];
      
      // If the column has all unique values, return a single bin
      if (result.total_count === result.unique_count) {
        return [{
          key: 'unique',
          x0: 'unique',
          x1: 'unique',
          length: result.total_count,
          count: result.total_count
        }];
      }

      // For non-unique columns, proceed with normal binning
      let query;
      switch (type?.toLowerCase()) {
        case "continuous":
          query = `
            WITH stats AS (
              SELECT
                MIN(${escapedColumn}) as min_val,
                MAX(${escapedColumn}) as max_val,
                (MAX(${escapedColumn}) - MIN(${escapedColumn})) / 10.0 as bin_width
              FROM ${this.tableName}
              WHERE ${escapedColumn} IS NOT NULL
              AND ${filterClause}
            ),
            bins AS (
              SELECT
                min_val + ((value - 1) * bin_width) as x0,
                min_val + (value * bin_width) as x1
              FROM generate_series(1, 10) vals(value), stats
              WHERE min_val IS NOT NULL AND max_val IS NOT NULL
            )
            SELECT
              x0,
              x1,
              COUNT(*) as length,
              MIN(${escapedColumn}) as min_val,
              MAX(${escapedColumn}) as max_val,
              AVG(${escapedColumn}) as mean
            FROM ${this.tableName}
            CROSS JOIN bins
            WHERE ${escapedColumn} >= x0 
            AND ${escapedColumn} < x1
            AND ${filterClause}
            GROUP BY x0, x1
            ORDER BY x0
          `;
          break;

        case "date":
          query = `
            WITH date_bounds AS (
              SELECT
                date_trunc('day', MIN(${escapedColumn})) as min_date,
                date_trunc('day', MAX(${escapedColumn})) as max_date
              FROM ${this.tableName}
              WHERE ${escapedColumn} IS NOT NULL
              AND ${filterClause}
            ),
            date_series AS (
              SELECT 
                generate_series(
                  min_date,
                  max_date,
                  INTERVAL '1 day'
                ) as date_bin
              FROM date_bounds
            )
            SELECT
              date_bin as x0,
              date_bin + INTERVAL '1 day' as x1,
              COUNT(*) as length
            FROM ${this.tableName}
            JOIN date_series ON date_trunc('day', ${escapedColumn}) = date_bin
            WHERE ${filterClause}
            GROUP BY date_bin
            ORDER BY date_bin
          `;
          break;

        case "ordinal":
        default:
          query = `
            WITH value_counts AS (
              SELECT
                ${escapedColumn} as key,
                COUNT(*) as length
              FROM ${this.tableName}
              WHERE ${escapedColumn} IS NOT NULL
              AND ${filterClause}
              GROUP BY ${escapedColumn}
              ORDER BY length DESC
              LIMIT ${maxOrdinalBins}
            )
            SELECT
              key,
              key as x0,
              key as x1,
              length,
              length as count
            FROM value_counts
            UNION ALL
            SELECT
              'Other' as key,
              'Other' as x0,
              'Other' as x1,
              SUM(cnt) as length,
              SUM(cnt) as count
            FROM (
              SELECT COUNT(*) as cnt
              FROM ${this.tableName}
              WHERE ${escapedColumn} IS NOT NULL
              AND ${filterClause}
              AND ${escapedColumn} NOT IN (SELECT key FROM value_counts)
              GROUP BY ${escapedColumn}
            ) as others
            HAVING SUM(cnt) > 0
            ORDER BY length DESC
          `;
          break;
      }

      const binResult = await this.conn.query(query);
      const bins = binResult.toArray().map(row => ({
        ...row,
        x0: type === "date" ? new Date(row.x0) : row.x0,
        x1: type === "date" ? new Date(row.x1) : row.x1
      }));

      return bins;
    } catch (error) {
      console.error('Error in binDataWithDuckDB:', error);
      return [];
    }
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
        await this.loadFileData(source, format);
      } else if (typeof source === "string") {
        if (!format) {
          throw new Error("Format must be specified for URL/file path data sources");
        }
        await this.loadURLData(source, format);
      } else {
        throw new Error("Unsupported data source type");
      }

      // Verify data loading
      const countQuery = `SELECT COUNT(*) as count FROM ${this.tableName}`;
      const countResult = await this.conn.query(countQuery);
      const count = countResult.toArray()[0].count;

      if (count === 0) {
        throw new Error("No data was loaded into the table");
      }

      // Check table structure
      const structureQuery = `DESCRIBE ${this.tableName}`;
      const structure = await this.conn.query(structureQuery);
      console.log('Table structure:', structure.toArray());

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
          .map(col => this.safeColumnName(col))
          .join(", ");

        const values = batch.map(row => {
          const rowValues = Object.keys(schema).map(col => {
            const val = row[col];
            if (val === null || val === undefined) return "NULL";
            // Convert all values to strings for consistency with schema
            return `'${String(val).replace(/'/g, "''")}'`;
          });
          return `(${rowValues.join(", ")})`;
        }).join(", ");

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
        // Check if it's an ID column (common naming patterns for IDs)
        const isIdColumn = 
          key.toLowerCase().includes('id') ||
          key.toLowerCase() === 'uprn' ||
          key.toLowerCase().includes('identifier');

        // Use VARCHAR for IDs, number types for other numeric values
        if (isIdColumn) {
          schema[key] = "VARCHAR";
        } else {
          schema[key] = Number.isInteger(value) ? "INTEGER" : "DOUBLE";
        }
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
      const processedConditions = filterConditions.map(condition => {
        const { column, operator, value } = condition;
        const escapedColumn = this.safeColumnName(column);
        const escapedValue = typeof value === 'string' 
          ? `'${value.replace(/'/g, "''")}'` 
          : value;
        return `${escapedColumn} ${operator} ${escapedValue}`;
      });

      const whereClause = processedConditions.join(' AND ');
      const query = `
        SELECT *, ROWID
        FROM ${this.tableName}
        WHERE ${whereClause}
        LIMIT ${this.options.rowsPerPage}
      `;

      return await this.conn.query(query);
    } catch (error) {
      console.error('Filter query failed:', error);
      throw error;
    }
  }

  async applySorting(sortColumns) {
    try {
      // Process sort columns to ensure safe column names
      const orderByClause = sortColumns.map(sort => {
        const { column, direction } = sort;
        const escapedColumn = this.safeColumnName(column);
        return `${escapedColumn} ${direction || 'ASC'}`;
      }).join(', ');

      const query = `
        SELECT *, ROWID
        FROM ${this.tableName}
        ORDER BY ${orderByClause}
        LIMIT ${this.options.rowsPerPage}
      `;

      return await this.conn.query(query);
    } catch (error) {
      console.error('Sort query failed:', error);
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
      console.error('Distribution query failed:', error);
      throw error;
    }
  }
}
