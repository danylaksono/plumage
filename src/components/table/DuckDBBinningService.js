import * as duckdb from "npm:@duckdb/duckdb-wasm";
import { DuckDBDataProcessor } from "../_base/duckdb-processor.js";

export class DuckDBBinningService {
  constructor(DuckDBDataProcessor) {
    // Validate that a DuckDBProcessor instance is provided
    if (!DuckDBDataProcessor) {
      throw new Error("DuckDBProcessor is required");
    }
    this.duckdb = DuckDBDataProcessor;
    // Initialize a cache to store binning results and avoid redundant computations
    this.cache = new Map();
  }

  async checkConnection() {
    // Verify that the DuckDB instance and connection are initialized
    if (!this.duckdb || !this.duckdb.conn) {
      throw new Error("DuckDB processor is not initialized");
    }
    try {
      // Test the connection with a simple query
      await this.duckdb.conn.query("SELECT 1");
    } catch (error) {
      throw new Error(`DuckDB connection failed: ${error.message}`);
    }
  }

  async getBinningForColumn(
    columnName,
    type,
    maxBins = 20,
    binningStrategy = "default"
  ) {
    // Validate input parameters
    if (!columnName || !this.duckdb) {
      throw new Error("Invalid column name or DuckDB instance");
    }

    // Create a unique cache key based on parameters to ensure distinct results
    const cacheKey = `${columnName}-${type}-${maxBins}-${binningStrategy}`;
    if (this.cache.has(cacheKey)) {
      // Return cached result if available to improve performance
      return this.cache.get(cacheKey);
    }

    try {
      await this.checkConnection();
      let binningData;

      // Handle supported column types using binDataWithDuckDB or custom logic
      switch (type) {
        case "continuous":
          // Use the built-in binDataWithDuckDB method for continuous data
          // Note: SorterTable.js expects the raw array of bins, not wrapped in an object
          binningData = await this.duckdb.binDataWithDuckDB(
            columnName,
            type,
            maxBins
          );
          // Note: we're not wrapping the data in {type, bins} as SorterTable expects raw bin array
          break;

        case "date":
        case "ordinal":
          // Delegate to the existing binDataWithDuckDB method for these types
          binningData = await this.duckdb.binDataWithDuckDB(
            columnName,
            type,
            maxBins
          );
          // Format the result to match expected structure
          binningData = {
            type,
            bins: binningData.map((row) => ({
              x0: row.x0,
              x1: row.x1,
              length: Number(row.length),
              ...(type === "ordinal" && { key: row.key }),
            })),
          };
          break;

        case "boolean":
          // Custom binning for boolean columns (true/false)
          binningData = await this.getBooleanBins(columnName);
          break;

        case "string":
          // Custom binning for string columns with top categories and "Other"
          binningData = await this.getStringBins(columnName, maxBins);
          break;

        default:
          throw new Error(`Unsupported column type: ${type}`);
      }

      // Store the result in the cache before returning
      this.cache.set(cacheKey, binningData);
      return binningData;
    } catch (error) {
      console.error(`Error getting bins for column ${columnName}:`, error);
      throw error;
    }
  }

  async getBooleanBins(columnName) {
    await this.checkConnection();
    // Escape column name to prevent SQL injection
    const escapedColumn = this.duckdb.safeColumnName(columnName);

    // Query counts for true and false values
    const query = `
      SELECT 
        ${escapedColumn} as key,
        COUNT(*) as length,
        ARRAY_AGG(${escapedColumn}) as values
      FROM ${this.duckdb.tableName}
      WHERE ${escapedColumn} IS NOT NULL
      GROUP BY ${escapedColumn}
      ORDER BY key
    `;

    const bins = await this.duckdb.query(query);

    // Format bins for boolean type
    return {
      type: "boolean",
      bins: bins.map((bin) => ({
        key: bin.key === null ? "NULL" : bin.key.toString(),
        x0: bin.key,
        x1: bin.key, // For boolean, x0 and x1 are the same value
        length: Number(bin.length),
        values: bin.values,
      })),
    };
  }

  async getStringBins(columnName, maxBins) {
    await this.checkConnection();
    // Escape column name to prevent SQL injection
    const escapedColumn = this.duckdb.safeColumnName(columnName);

    // Query top N string categories and group remaining into "Other"
    const query = `
      WITH category_counts AS (
        SELECT 
          ${escapedColumn} as key,
          COUNT(*) as length
        FROM ${this.duckdb.tableName}
        WHERE ${escapedColumn} IS NOT NULL
        GROUP BY ${escapedColumn}
        ORDER BY length DESC
        LIMIT ${maxBins - 1}
      ),
      other_count AS (
        SELECT 
          'Other' as key,
          COUNT(*) as length,
          ARRAY_AGG(${escapedColumn}) as values
        FROM ${this.duckdb.tableName}
        WHERE ${escapedColumn} IS NOT NULL
          AND ${escapedColumn} NOT IN (SELECT key FROM category_counts)
      )
      SELECT key, length, ARRAY_AGG(${escapedColumn}) as values
      FROM category_counts
      UNION ALL
      SELECT key, length, values
      FROM other_count
      WHERE length > 0
    `;

    const bins = await this.duckdb.query(query);

    // Format bins for string type
    return {
      type: "string",
      bins: bins.map((bin) => ({
        key: bin.key,
        x0: bin.key,
        x1: bin.key, // For string, x0 and x1 are the same value
        length: Number(bin.length),
        values: bin.values,
      })),
    };
  }

  clearCache() {
    // Clear the cache when data changes significantly (e.g., after filtering or loading new data)
    this.cache.clear();
  }
}
