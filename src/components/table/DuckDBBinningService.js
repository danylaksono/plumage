import * as duckdb from "npm:@duckdb/duckdb-wasm";

export class DuckDBBinningService {
  constructor(duckDBProcessor) {
    if (!duckDBProcessor) {
      throw new Error("DuckDBProcessor is required");
    }
    this.duckdb = duckDBProcessor;
  }

  async checkConnection() {
    if (!this.duckdb || !this.duckdb.conn) {
      throw new Error("DuckDB processor is not initialized");
    }
    try {
      // Ensure the connection is active
      await this.duckdb.conn.query("SELECT 1");
    } catch (error) {
      throw new Error(`DuckDB connection failed: ${error.message}`);
    }
  }

  async getBinningForColumn(columnName, type, maxBins = 20) {
    if (!columnName || !this.duckdb) {
      throw new Error("Column name and DuckDB processor are required");
    }
    try {
      await this.checkConnection();
      // If type isn't provided, infer it
      if (!type) {
        type = await this.duckdb.getTypeFromDuckDB(columnName);
      }
      console.log(`Creating bins for ${columnName}:`, {
        type,
        maxBins,
        tableName: this.duckdb.tableName,
      });

      // Get binning data from DuckDB
      const result = await this.duckdb.binDataWithDuckDB(
        columnName,
        type,
        maxBins
      );
      console.log(`Raw binning data for ${columnName}:`, result);

      // Transform the data based on type
      switch (type) {
        case "continuous":
          return {
            type: "continuous",
            bins: result.map((bin) => ({
              x0: Number(bin.x0),
              x1: Number(bin.x1),
              length: Number(bin.length),
              count: Number(bin.length),
            })),
            nominals: [],
          };

        case "ordinal":
          return {
            type: "ordinal",
            bins: result.map((bin) => ({
              key: bin.key,
              x0: bin.x0,
              x1: bin.x1,
              length: Number(bin.length),
              count: Number(bin.length),
            })),
            nominals: result.map((bin) => bin.key),
          };

        case "date":
          return {
            type: "date",
            bins: result.map((bin) => ({
              x0: new Date(bin.x0),
              x1: new Date(bin.x1),
              length: Number(bin.length),
              count: Number(bin.length),
              key: new Date(bin.x0),
            })),
            nominals: [],
          };

        default:
          return {
            type: "ordinal",
            bins: result.map((bin) => ({
              key: bin.key || bin.x0,
              x0: bin.x0,
              x1: bin.x1,
              length: Number(bin.length),
              count: Number(bin.length),
            })),
            nominals: result.map((bin) => bin.key || bin.x0),
          };
      }
    } catch (error) {
      console.error(`Binning failed for column ${columnName}:`, error);
      // Return empty bins rather than throwing to allow graceful fallback
      return {
        type: type || "ordinal",
        bins: [],
        nominals: [],
      };
    }
  }

  async getContinuousBins(columnName, maxBins) {
    await this.checkConnection();

    const escapedColumn = this.duckdb.safeColumnName(columnName);
    const result = await this.duckdb.binDataWithDuckDB(
      columnName,
      "continuous",
      maxBins
    );

    return {
      type: "continuous",
      bins: result.map((bin) => ({
        x0: bin.x0,
        x1: bin.x1,
        length: bin.length,
        count: bin.length,
        mean: bin.mean,
        median: bin.median,
        min: bin.min,
        max: bin.max,
      })),
    };
  }

  async getOrdinalBins(columnName, maxBins) {
    await this.checkConnection();

    const escapedColumn = this.duckdb.safeColumnName(columnName);
    const result = await this.duckdb.binDataWithDuckDB(
      columnName,
      "ordinal",
      maxBins
    );

    console.log(`Raw ordinal result for ${columnName}:`, result);

    return {
      type: "ordinal",
      bins: result.map((bin) => ({
        key: bin.value,
        x0: bin.value,
        x1: bin.value,
        length: bin.count,
        count: bin.count,
        mean: bin.mean,
        median: bin.median,
        min: bin.min,
        max: bin.max,
      })),
      nominals: result.map((bin) => bin.value),
    };
  }

  async getDateBins(columnName, maxBins) {
    await this.checkConnection();

    const escapedColumn = this.duckdb.safeColumnName(columnName);
    const result = await this.duckdb.binDataWithDuckDB(
      columnName,
      "date",
      maxBins
    );

    return {
      type: "date",
      bins: result.map((bin) => ({
        x0: bin.bin_start,
        x1: bin.bin_end,
        length: bin.count,
        count: bin.count,
        min: bin.min,
        max: bin.max,
        key: bin.bin_start,
      })),
    };
  }
}
