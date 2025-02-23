import { DuckDBDataProcessor } from "./duckdb-processor.js";
import * as d3 from "npm:d3";

export class ChartConfig {
  constructor(config = {}) {
    const defaults = {
      width: 600,
      height: 400,
      margin: { top: 20, right: 20, bottom: 40, left: 40 },
      colors: ["steelblue", "orange"],
      selectionMode: "single", // 'single', 'multiple', 'drag'
      axis: true,
      dataSource: null,
      dataFormat: null,
      column: null, // for single-column charts
      columns: [], // for multi-column charts
    };

    return { ...defaults, ...config };
  }
}

export class BaseVisualization {
  constructor(config) {
    const defaults = {
      width: 600,
      height: 400,
      margin: { top: 20, right: 20, bottom: 40, left: 40 },
      colors: ["steelblue", "orange"],
      selectionMode: "single", // 'single', 'multiple', 'drag'
      axis: true,
      dataSource: null,
      dataFormat: null,
    };

    this.config = { ...defaults, ...config };
    this.dispatch = d3.dispatch("selectionChanged");
    this.initialized = false;
    this.dataProcessor = null;
    this.tableName = `data_${Math.random().toString(36).substr(2, 9)}`;

    console.log("[BaseVisualization] Initializing with config:", this.config);

    // Add data source validation
    if (!config.dataSource && !config.dataProcessor) {
      console.warn(
        "[BaseVisualization] No data source or processor provided in config:",
        config
      );
    }
  }

  async initialize() {
    console.log("[BaseVisualization] Starting initialization");
    if (!this.initialized) {
      this.createSvg();
    }

    await this.setupDuckDB();

    if (this.config.dataSource) {
      console.log(
        "[BaseVisualization] Loading data source:",
        this.config.dataSource
      );
      await this.loadData(this.config.dataSource, this.config.dataFormat);
    }

    console.log("[BaseVisualization] Initialization complete");
    return this;
  }

  async setupDuckDB() {
    try {
      console.log(
        "[BaseVisualization] Setting up DuckDB with table:",
        this.tableName
      );
      this.dataProcessor = new DuckDBDataProcessor(null, this.tableName);
      await this.dataProcessor.connect();
    } catch (error) {
      console.error("[BaseVisualization] DuckDB setup failed:", error);
      throw new Error(`Failed to initialize DuckDB: ${error.message}`);
    }
  }

  async loadData(source, format) {
    try {
      if (!source) {
        console.error(
          "[BaseVisualization] Cannot load data: No data source provided"
        );
        return;
      }

      console.log("[BaseVisualization] Loading data:", {
        source: Array.isArray(source) ? `Array[${source.length}]` : source,
        format,
        tableName: this.tableName,
      });

      await this.dataProcessor.loadData(source, format);
      const count = await this.query(
        "SELECT COUNT(*) as count FROM " + this.tableName
      );
      console.log("[BaseVisualization] Loaded rows:", count[0].count);

      // Validate data after loading
      const sampleQuery = `SELECT * FROM ${this.tableName} LIMIT 1`;
      const sample = await this.query(sampleQuery);
      console.log("[BaseVisualization] Sample data:", sample[0]);
    } catch (error) {
      console.error("[BaseVisualization] Data loading failed:", error);
      throw new Error(`Failed to load data: ${error.message}`);
    }
  }

  createSvg() {
    const { width, height, margin } = this.config;
    console.log("[BaseVisualization] Creating SVG with dimensions:", {
      width,
      height,
      margin,
    });

    this.svg = d3
      .create("svg")
      .attr("width", width + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom)
      .attr("viewBox", [
        0,
        0,
        width + margin.left + margin.right,
        height + margin.top + margin.bottom,
      ])
      .attr("style", "max-width: 100%; height: auto;");

    this.g = this.svg
      .append("g")
      .attr("transform", `translate(${margin.left},${margin.top})`);

    this.initialized = true;

    console.log("[BaseVisualization] SVG created with viewBox:", [
      0,
      0,
      width + margin.left + margin.right,
      height + margin.top + margin.bottom,
    ]);
    return this.svg.node();
  }

  async query(sql) {
    console.log("[BaseVisualization] Executing query:", sql);
    const result = await this.dataProcessor.query(sql);
    console.log("[BaseVisualization] Query result rows:", result.length);
    return result;
  }

  on(event, callback) {
    this.dispatch.on(event, callback);
    return this;
  }

  async destroy() {
    if (this.dataProcessor) {
      await this.dataProcessor.dropTable();
      await this.dataProcessor.close();
      await this.dataProcessor.terminate();
    }
    if (this.svg) {
      this.svg.remove();
    }
    this.initialized = false;
  }

  /**
   * Interface that all chart types must implement
   */
  static chartInterface = {
    initialize: async () => {},
    update: async (data) => {},
    destroy: async () => {},
    highlightData: async (indices) => {},
    highlightDataByValue: async (values) => {},
    getSelectedData: async () => {},
    on: (event, callback) => {},
  };

  static isLargeNumber(value) {
    // Check if the value is a BigInt
    if (typeof value === 'bigint') return true;
    
    // Check if the value is a number larger than MAX_SAFE_INTEGER
    if (typeof value === 'number' && !Number.isSafeInteger(value)) return true;
    
    // Check if the value is a string representation of a large number
    if (typeof value === 'string') {
      const num = Number(value);
      if (!Number.isSafeInteger(num)) return true;
    }
    
    return false;
  }

  static getSafeType(value) {
    if (value instanceof Date) return "TIMESTAMP";
    if (BaseVisualization.isLargeNumber(value)) return "VARCHAR";
    if (typeof value === "number") {
      return Number.isInteger(value) ? "INTEGER" : "DOUBLE";
    }
    if (typeof value === "boolean") return "BOOLEAN";
    return "VARCHAR";
  }

  async validateData(data) {
    if (!Array.isArray(data)) {
      throw new Error("Data must be an array");
    }
    
    const firstValidRow = data.find(row => row !== null && Object.keys(row).length > 0);
    if (!firstValidRow) {
      throw new Error("No valid data rows found");
    }

    // Log data sample for debugging
    console.log("Data sample:", {
      first: firstValidRow,
      sampleSize: Math.min(data.length, 5),
      totalRows: data.length
    });

    return true;
  }
}
