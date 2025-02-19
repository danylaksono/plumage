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
    } else {
      this.conn = await this.duckdb.connect();
    }
  }

  async getTypeFromDuckDB(column) {
    const query = `
      SELECT typeof(${column}) as col_type
      FROM ${this.tableName}
      WHERE ${column} IS NOT NULL
      LIMIT 1
    `;

    const result = await this.conn.query(query);
    const type = result.toArray()[0].col_type.toLowerCase();

    if (type.includes("float") || type.includes("integer")) return "continuous";
    if (type.includes("date") || type.includes("timestamp")) return "date";
    return "ordinal";
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

  async binDataWithDuckDB(column, type, maxOrdinalBins = 20) {
    let query;

    switch (type) {
      case "continuous":
        // First get the column type to handle casting properly
        const typeQuery = `SELECT typeof(${column}) as col_type
                          FROM ${this.tableName}
                          WHERE ${column} IS NOT NULL
                          LIMIT 1`;
        const typeResult = await this.logQuery(typeQuery, "Get Column Type");
        const colType = typeResult.toArray()[0].col_type;
        const numericType = this.getDuckDBType(colType);

        query = `
          WITH stats AS (
            SELECT
              MIN(${column}) as min_val,
              MAX(${column}) as max_val,
              COUNT(*) as n,
              (MAX(${column}) - MIN(${column})) as range
            FROM ${this.tableName}
            WHERE ${column} IS NOT NULL
          ),
          bin_params AS (
            SELECT
              min_val,
              max_val,
              (max_val - min_val) / 10.0 as bin_width
            FROM stats
          ),
          bins AS (
            SELECT
              min_val + (CAST(value - 1 AS ${colType}) * bin_width) as x0,
              min_val + (CAST(value AS ${colType}) * bin_width) as x1
            FROM generate_series(1, 10) vals(value), bin_params
          )
          SELECT
            x0,
            x1,
            COUNT(${column}) as length
          FROM ${this.tableName}
          CROSS JOIN bins
          WHERE ${column} >= x0 AND ${column} < x1
          GROUP BY x0, x1
          ORDER BY x0
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
    let bins = result.toArray().map((row) => ({
      ...row,
      x0: type === "date" ? new Date(row.x0) : row.x0,
      x1: type === "date" ? new Date(row.x1) : row.x1,
    }));

    if (type === "ordinal" && bins.length === maxOrdinalBins) {
      const othersQuery = `
        WITH ranked AS (
          SELECT ${column}, COUNT(*) as cnt
          FROM ${this.tableName}
          WHERE ${column} IS NOT NULL
          GROUP BY ${column}
          ORDER BY cnt DESC
          OFFSET ${maxOrdinalBins - 1}
        )
        SELECT SUM(cnt) as length
        FROM ranked
      `;

      const othersResult = await this.conn.query(othersQuery);
      const othersCount = othersResult.toArray()[0].length;

      if (othersCount > 0) {
        bins.push({
          key: "Other",
          x0: "Other",
          x1: "Other",
          length: othersCount,
        });
      }
    }

    return bins;
  }

  async loadData(source, format) {
    try {
      // Drop existing table if it exists
      await this.conn.query(`DROP TABLE IF EXISTS ${this.tableName}`);

      if (Array.isArray(source)) {
        // Handle JavaScript array of objects
        await this.loadJSONData(source);
      } else if (source instanceof File) {
        // Handle File object
        await this.loadFileData(source, format);
      } else if (typeof source === "string") {
        // Handle URL
        await this.loadURLData(source, format);
      } else {
        throw new Error("Unsupported data source");
      }

      // Verify data loading
      const countResult = await this.conn.query(
        `SELECT COUNT(*) as count FROM ${this.tableName}`
      );
      const count = countResult.toArray()[0].count;

      if (count === 0) {
        throw new Error("No data was loaded");
      }
    } catch (error) {
      throw new Error(`Failed to load data: ${error.message}`);
    }
  }

  async loadJSONData(data) {
    try {
      if (data.length === 0) {
        throw new Error("Empty data array provided");
      }

      // Infer schema from the first object
      const schema = this.inferSchema(data[0]);
      const createTableSQL = this.generateCreateTableSQL(schema);

      // Create the table
      await this.conn.query(createTableSQL);
      console.log("Table created successfully");

      // Insert data in batches using SQL INSERT
      const batchSize = 1000;
      for (let i = 0; i < data.length; i += batchSize) {
        const batch = data.slice(i, i + batchSize);

        // Generate INSERT query for the batch
        const columns = Object.keys(schema)
          .map((col) => `"${col}"`)
          .join(", ");
        const values = batch
          .map((row) => {
            const rowValues = Object.values(row).map((val) => {
              if (val === null || val === undefined) return "NULL";
              if (typeof val === "string")
                return `'${val.replace(/'/g, "''")}'`; // Escape single quotes
              if (val instanceof Date) return `'${val.toISOString()}'`; // Format dates
              return val;
            });
            return `(${rowValues.join(", ")})`;
          })
          .join(", ");

        const insertQuery = `INSERT INTO ${this.tableName} (${columns}) VALUES ${values}`;
        await this.conn.query(insertQuery);
        console.log(`Inserted batch ${i / batchSize + 1}`);
      }

      console.log("JSON data loaded successfully");
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
}
