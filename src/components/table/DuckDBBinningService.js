import * as duckdb from "npm:@duckdb/duckdb-wasm";

export class DuckDBBinningService {
  constructor(duckDBProcessor) {
    if (!duckDBProcessor) {
      throw new Error("DuckDBProcessor is required");
    }
    this.duckdb = duckDBProcessor;
  }

  async checkConnection() {
    if (!this.duckdb) {
      throw new Error("DuckDB processor is not initialized");
    }
    try {
      await this.duckdb.waitForConnection();
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

      switch (type) {
        case "continuous":
          return await this.getContinuousBins(columnName, maxBins);
        case "ordinal":
          return await this.getOrdinalBins(columnName, maxBins);
        case "date":
          return await this.getDateBins(columnName, maxBins);
        default:
          return await this.getOrdinalBins(columnName, maxBins);
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
