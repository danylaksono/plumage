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
      skipDataLoading: false, // Add this flag to control data loading behavior
    };

    this.config = { ...defaults, ...config };
    this.dispatch = d3.dispatch("selectionChanged");
    this.initialized = false;
    this.dataProcessor = null;
    this.tableName =
      config.tableName || `data_${Math.random().toString(36).substr(2, 9)}`;

    console.log("[BaseVisualization] Initializing with config:", this.config);

    // Add data source validation
    if (
      !config.dataSource &&
      !config.dataProcessor &&
      !config.skipDataLoading
    ) {
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

    this.showLoading();
    try {
      // Setup DuckDB if needed
      if (!this.dataProcessor) {
        await this.setupDuckDB();
      }

      // Only load data if it's not being skipped and we have a data source
      if (!this.config.skipDataLoading && this.config.dataSource) {
        console.log(
          "[BaseVisualization] Loading data source:",
          this.config.dataSource
        );
        await this.loadData(this.config.dataSource, this.config.dataFormat);
      }
    } finally {
      this.hideLoading();
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

    // Add loading overlay
    this.loadingOverlay = this.svg
      .append("g")
      .attr("class", "loading-overlay")
      .style("display", "none");

    this.loadingOverlay
      .append("rect")
      .attr("width", width + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom)
      .attr("fill", "rgba(255, 255, 255, 0.8)");

    this.loadingOverlay
      .append("text")
      .attr("x", (width + margin.left + margin.right) / 2)
      .attr("y", (height + margin.top + margin.bottom) / 2)
      .attr("text-anchor", "middle")
      .attr("dominant-baseline", "middle")
      .text("Loading...");

    this.initialized = true;

    console.log("[BaseVisualization] SVG created with viewBox:", [
      0,
      0,
      width + margin.left + margin.right,
      height + margin.top + margin.bottom,
    ]);
    return this.svg.node();
  }

  showLoading() {
    if (this.loadingOverlay) {
      this.loadingOverlay.style("display", null);
    }
  }

  hideLoading() {
    if (this.loadingOverlay) {
      this.loadingOverlay.style("display", "none");
    }
  }

  async query(sql) {
    console.log("[BaseVisualization] Executing query:", sql);
    const result = await this.dataProcessor.query(sql);
    console.log("[BaseVisualization] Query result rows:", result.length);
    return result;
  }

  /**
   * Execute a natural language query against the data
   * @param {string} nlQuery - Natural language query
   * @returns {Promise<Array>} - Query results
   */
  async naturalLanguageQuery(nlQuery) {
    console.log(
      "[BaseVisualization] Processing natural language query:",
      nlQuery
    );

    this.showLoading();
    try {
      const sql = await this.translateNaturalLanguageToSQL(nlQuery);
      console.log("[BaseVisualization] Translated to SQL:", sql);

      const result = await this.query(sql);
      return result;
    } catch (error) {
      console.error("[BaseVisualization] Natural language query error:", error);
      throw new Error(
        `Failed to process natural language query: ${error.message}`
      );
    } finally {
      this.hideLoading();
    }
  }

  /**
   * Translate natural language to SQL
   * @param {string} nlQuery - Natural language query
   * @returns {Promise<string>} - SQL query
   */
  async translateNaturalLanguageToSQL(nlQuery) {
    if (!nlQuery || typeof nlQuery !== "string") {
      throw new Error("Invalid query: Query must be a non-empty string");
    }

    // Convert query to lowercase for easier pattern matching
    const query = nlQuery.toLowerCase();

    // Get table columns to reference in query
    const columns = await this.getTableColumns();
    console.log("[BaseVisualization] Available columns:", columns);

    // Identify columns mentioned in the query
    const mentionedColumns = columns.filter((col) =>
      query.includes(col.name.toLowerCase())
    );

    // Rules for query translation
    if (
      query.includes("first percentile") ||
      query.includes("1st percentile")
    ) {
      const column = this.findColumnInQuery(query, columns);
      if (!column)
        throw new Error("Column not specified or recognized in the query");

      return `SELECT * FROM ${this.tableName} 
              WHERE "${column}" <= (SELECT PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY "${column}")
              FROM ${this.tableName})`;
    }

    if (
      query.includes("last percentile") ||
      query.includes("99th percentile")
    ) {
      const column = this.findColumnInQuery(query, columns);
      if (!column)
        throw new Error("Column not specified or recognized in the query");

      return `SELECT * FROM ${this.tableName} 
              WHERE "${column}" >= (SELECT PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY "${column}")
              FROM ${this.tableName})`;
    }

    // Handle queries about maximum values
    if (
      query.includes("maximum") ||
      query.includes("highest") ||
      query.includes("largest")
    ) {
      const column = this.findColumnInQuery(query, columns);
      if (!column)
        throw new Error("Column not specified or recognized in the query");

      return `SELECT * FROM ${this.tableName} 
              WHERE "${column}" = (SELECT MAX("${column}") FROM ${this.tableName})`;
    }

    // Handle queries about minimum values
    if (
      query.includes("minimum") ||
      query.includes("lowest") ||
      query.includes("smallest")
    ) {
      const column = this.findColumnInQuery(query, columns);
      if (!column)
        throw new Error("Column not specified or recognized in the query");

      return `SELECT * FROM ${this.tableName} 
              WHERE "${column}" = (SELECT MIN("${column}") FROM ${this.tableName})`;
    }

    // Handle queries about averages
    if (query.includes("average") || query.includes("mean")) {
      const column = this.findColumnInQuery(query, columns);
      if (!column)
        throw new Error("Column not specified or recognized in the query");

      if (query.includes("above") || query.includes("greater than")) {
        return `SELECT * FROM ${this.tableName} 
                WHERE "${column}" > (SELECT AVG("${column}") FROM ${this.tableName})`;
      } else if (query.includes("below") || query.includes("less than")) {
        return `SELECT * FROM ${this.tableName} 
                WHERE "${column}" < (SELECT AVG("${column}") FROM ${this.tableName})`;
      } else {
        // Return the average value instead of filtered data
        return `SELECT AVG("${column}") as average_value FROM ${this.tableName}`;
      }
    }

    // Handle queries for outliers
    if (query.includes("outlier") || query.includes("anomaly")) {
      const column = this.findColumnInQuery(query, columns);
      if (!column)
        throw new Error("Column not specified or recognized in the query");

      // Using IQR method to identify outliers
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
      `;
    }

    // Handle basic filtering with comparison operators
    const comparisonPatterns = [
      { regex: /greater than (\d+(\.\d+)?)/i, operator: ">" },
      { regex: /less than (\d+(\.\d+)?)/i, operator: "<" },
      { regex: /equal to (\d+(\.\d+)?)/i, operator: "=" },
    ];

    for (const col of mentionedColumns) {
      for (const pattern of comparisonPatterns) {
        const match = query.match(
          new RegExp(`${col.name.toLowerCase()} ${pattern.regex.source}`)
        );
        if (match) {
          const value = match[1];
          return `SELECT * FROM ${this.tableName} WHERE "${col.name}" ${pattern.operator} ${value}`;
        }
      }
    }

    // Use AI to handle complex queries
    try {
      console.log("[BaseVisualization] Using AI for complex query translation");

      // Create schema information to help the AI understand the data structure
      const schemaInfo = columns
        .map(
          (col) =>
            `${col.name} (${col.type}${col.isNumeric ? ", numeric" : ""})`
        )
        .join("\n");

      // Import required libraries (ensure these are added to your dependencies)
      const { OpenAI } = await import("openai");

      const openai = new OpenAI({
        baseURL: "https://openrouter.ai/api/v1",
        apiKey: process.env.OPENROUTER_API_KEY,
        defaultHeaders: {
          "HTTP-Referer": "https://plumage.visualization",
          "X-Title": "Plumage Visualization",
        },
      });

      // Create a prompt that instructs the AI how to generate SQL
      const systemMessage = `You are a SQL expert. Convert natural language queries to valid SQL for DuckDB.
Table name: ${this.tableName}
Table schema:
${schemaInfo}

Generate ONLY the SQL query with no explanations or markdown. 
Ensure all column names are properly quoted with double quotes.
Limit results to 1000 rows if appropriate.`;

      const completion = await openai.chat.completions.create({
        model: "openai/gpt-4o",
        messages: [
          { role: "system", content: systemMessage },
          { role: "user", content: nlQuery },
        ],
        temperature: 0.1, // Low temperature for more deterministic outputs
        max_tokens: 300, // Limit response size
      });

      const generatedSQL = completion.choices[0].message.content.trim();
      console.log("[BaseVisualization] AI-generated SQL:", generatedSQL);

      // Basic validation to ensure the response is SQL
      if (
        generatedSQL.toUpperCase().includes("SELECT") &&
        generatedSQL.toUpperCase().includes("FROM") &&
        generatedSQL.includes(this.tableName)
      ) {
        return generatedSQL;
      } else {
        throw new Error("Generated SQL appears invalid");
      }
    } catch (error) {
      console.error("[BaseVisualization] AI query translation failed:", error);

      // Default to returning all data with a limit
      if (query.includes("all") || query.includes("everything")) {
        return `SELECT * FROM ${this.tableName} LIMIT 1000`;
      }

      throw new Error(
        "Could not understand the query. Please try a different phrasing."
      );
    }
  }

  /**
   * Find a column mentioned in a query
   * @param {string} query - The natural language query
   * @param {Array} columns - Available columns
   * @returns {string|null} - Column name or null if not found
   */
  findColumnInQuery(query, columns) {
    // First try exact matches
    for (const col of columns) {
      if (query.includes(col.name.toLowerCase())) {
        return col.name;
      }
    }

    // Try alternative phrasings
    const columnWords = columns.map((col) =>
      col.name.toLowerCase().split("_").join(" ")
    );
    for (let i = 0; i < columns.length; i++) {
      if (query.includes(columnWords[i])) {
        return columns[i].name;
      }
    }

    return null;
  }

  /**
   * Get table columns and their data types
   * @returns {Promise<Array>} - Column information
   */
  async getTableColumns() {
    try {
      // Query DuckDB for column information
      const columnsQuery = `DESCRIBE ${this.tableName}`;
      const result = await this.dataProcessor.query(columnsQuery);

      return result.map((col) => ({
        name: col.column_name,
        type: col.column_type,
        isNumeric: [
          "INTEGER",
          "DOUBLE",
          "DECIMAL",
          "FLOAT",
          "BIGINT",
          "HUGEINT",
        ].includes(col.column_type.toUpperCase()),
      }));
    } catch (error) {
      console.error("[BaseVisualization] Error fetching columns:", error);
      return [];
    }
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
      this.loadingOverlay = null;
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
    if (typeof value === "bigint") return true;

    // Check if the value is a number larger than MAX_SAFE_INTEGER
    if (typeof value === "number" && !Number.isSafeInteger(value)) return true;

    // Check if the value is a string representation of a large number
    if (typeof value === "string") {
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

    const firstValidRow = data.find(
      (row) => row !== null && Object.keys(row).length > 0
    );
    if (!firstValidRow) {
      throw new Error("No valid data rows found");
    }

    // Log data sample for debugging
    console.log("Data sample:", {
      first: firstValidRow,
      sampleSize: Math.min(data.length, 5),
      totalRows: data.length,
    });

    return true;
  }
}
