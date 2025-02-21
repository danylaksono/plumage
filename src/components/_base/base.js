import { DuckDBDataProcessor } from "./duckdb-processor.js";
import * as d3 from "npm:d3";

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
  }

  async initialize() {
    if (!this.initialized) {
      this.createSvg();
    }

    await this.setupDuckDB();

    if (this.config.dataSource) {
      await this.loadData(this.config.dataSource, this.config.dataFormat);
    }

    return this;
  }

  async setupDuckDB() {
    try {
      this.dataProcessor = new DuckDBDataProcessor(null, this.tableName);
      await this.dataProcessor.connect();
    } catch (error) {
      throw new Error(`Failed to initialize DuckDB: ${error.message}`);
    }
  }

  async loadData(source, format) {
    try {
      await this.dataProcessor.loadData(source, format);
    } catch (error) {
      throw new Error(`Failed to load data: ${error.message}`);
    }
  }

  createSvg() {
    const { width, height, margin } = this.config;

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
    return this.svg.node();
  }

  async query(sql) {
    return await this.dataProcessor.query(sql);
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
}
