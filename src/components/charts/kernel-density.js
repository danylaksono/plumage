import * as d3 from "npm:d3";
import { BaseVisualization } from "../_base/base.js";

export class KernelDensityPlot extends BaseVisualization {
  constructor(config) {
    const defaults = {
      bandwidth: 0.4, // KDE bandwidth parameter
      kernelType: "gaussian", // Type of kernel function
      showArea: true, // Whether to show area under the curve
      smoothingFactor: 100, // Number of points to evaluate density
    };

    super({ ...defaults, ...config });
    this.data = [];
    this.density = [];
    this.selectedData = new Set();
    this.highlightedData = null;
  }

  async initialize() {
    if (!this.initialized) {
      await super.initialize();
      this.createScales();
      this.createDensityLine();
    }
    return this;
  }

  createScales() {
    this.xScale = d3.scaleLinear().range([0, this.config.width]);
    this.yScale = d3.scaleLinear().range([this.config.height, 0]);
  }

  createDensityLine() {
    this.densityLine = d3
      .line()
      .x((d) => this.xScale(d[0]))
      .y((d) => this.yScale(d[1]));

    this.densityArea = d3
      .area()
      .x((d) => this.xScale(d[0]))
      .y0(this.config.height)
      .y1((d) => this.yScale(d[1]));
  }

  async update(data = null) {
    if (!this.initialized) {
      await this.initialize();
    }

    if (data) {
      this.data = data;
    } else if (this.dataProcessor) {
      const query = `SELECT "${this.config.column}" as value FROM ${this.tableName}`;
      const result = await this.query(query);
      this.data = result.map((d) => d.value);
    }

    this.computeDensity();
    this.updateScales();
    this.draw();
    return this;
  }

  computeDensity() {
    // Compute kernel density estimation
    const kde = this.kernelDensityEstimator(
      this.kernelEpanechnikov(this.config.bandwidth),
      this.xScale.ticks(this.config.smoothingFactor)
    );
    this.density = kde(this.data);
  }

  updateScales() {
    const extent = d3.extent(this.data);
    this.xScale.domain(extent);
    this.yScale.domain([0, d3.max(this.density, (d) => d[1])]);
  }

  draw() {
    // Clear previous elements
    this.g.selectAll("*").remove();

    // Draw area if enabled
    if (this.config.showArea) {
      this.g
        .append("path")
        .datum(this.density)
        .attr("class", "density-area")
        .attr("fill", this.config.colors[0])
        .attr("opacity", 0.3)
        .attr("d", this.densityArea);
    }

    // Draw line
    this.g
      .append("path")
      .datum(this.density)
      .attr("class", "density-line")
      .attr("fill", "none")
      .attr("stroke", this.config.colors[0])
      .attr("stroke-width", 1.5)
      .attr("d", this.densityLine);

    // Draw axes if enabled
    if (this.config.axis) {
      this.drawAxes();
    }
  }

  drawAxes() {
    const xAxis = d3.axisBottom(this.xScale);
    const yAxis = d3.axisLeft(this.yScale);

    this.g
      .append("g")
      .attr("class", "x-axis")
      .attr("transform", `translate(0,${this.config.height})`)
      .call(xAxis);

    this.g.append("g").attr("class", "y-axis").call(yAxis);
  }

  // Kernel density estimation functions
  kernelDensityEstimator(kernel, X) {
    return function (V) {
      return X.map((x) => [x, d3.mean(V, (v) => kernel(x - v))]);
    };
  }

  kernelEpanechnikov(bandwidth) {
    return (x) =>
      Math.abs((x /= bandwidth)) <= 1 ? (0.75 * (1 - x * x)) / bandwidth : 0;
  }

  // Required interface methods
  async highlightData(indices) {
    if (!indices || indices.length === 0) {
      this.highlightedData = null;
    } else {
      const query = `
        SELECT "${this.config.column}" as value
        FROM ${this.tableName}
        WHERE rowid IN (${indices.join(",")})
      `;
      const result = await this.query(query);
      this.highlightedData = result.map((d) => d.value);
    }
    this.update();
  }

  async highlightDataByValue(values) {
    if (!values || values.length === 0) {
      this.highlightedData = null;
    } else {
      this.highlightedData = values;
    }
    this.update();
  }

  async getSelectedData() {
    if (this.selectedData.size === 0) return [];

    const values = Array.from(this.selectedData);
    const query = `
      SELECT *
      FROM ${this.tableName}
      WHERE "${this.config.column}" IN (${values
      .map((v) => `'${v}'`)
      .join(",")})
    `;
    return await this.query(query);
  }

  async destroy() {
    await super.destroy();
  }
}
