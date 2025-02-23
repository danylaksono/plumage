import * as d3 from "npm:d3";
import { BaseVisualization } from "../_base/base.js";

export class ViolinPlot extends BaseVisualization {
  constructor(config) {
    const defaults = {
      bandwidth: 0.4,
      kernelType: "gaussian",
      showMedian: true,
      showQuartiles: true,
      maxWidth: 50, // Maximum width of the violin shape
      padding: 0.1, // Padding between violins for multiple groups
    };

    super({ ...defaults, ...config });
    this.data = [];
    this.violinData = [];
    this.selectedData = new Set();
    this.highlightedData = null;
    this.brushedRange = null;
  }

  async initialize() {
    if (!this.initialized) {
      await super.initialize();
      this.createScales();
      this.createViolin();
      this.setupBrush();
    }
    return this;
  }

  createScales() {
    this.xScale = d3.scaleLinear().range([0, this.config.maxWidth]); // For the width of the violin shape

    this.yScale = d3.scaleLinear().range([this.config.height, 0]);
  }

  createViolin() {
    this.violinArea = d3
      .area()
      .x0((d) => -this.xScale(d[1]))
      .x1((d) => this.xScale(d[1]))
      .y((d) => this.yScale(d[0]))
      .curve(d3.curveCatmullRom);
  }

  setupBrush() {
    this.brush = d3
      .brushY()
      .extent([
        [0, 0],
        [this.config.maxWidth * 2, this.config.height],
      ])
      .on("end", this.handleBrush.bind(this));
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

    this.computeViolin();
    this.updateScales();
    this.draw();
    return this;
  }

  computeViolin() {
    // Compute kernel density estimation
    const kde = this.kernelDensityEstimator(
      this.kernelEpanechnikov(this.config.bandwidth),
      this.yScale.ticks(100)
    );
    this.violinData = kde(this.data);

    // Calculate statistics
    this.stats = {
      median: d3.median(this.data),
      q1: d3.quantile(this.data, 0.25),
      q3: d3.quantile(this.data, 0.75),
    };
  }

  updateScales() {
    // Update y scale based on data extent
    this.yScale.domain(d3.extent(this.data));

    // Update x scale based on maximum density
    this.xScale.domain([0, d3.max(this.violinData, (d) => d[1])]);
  }

  draw() {
    // Clear previous elements
    this.g.selectAll("*").remove();

    const violinG = this.g
      .append("g")
      .attr("transform", `translate(${this.config.maxWidth},0)`);

    // Draw the violin shape
    violinG
      .append("path")
      .datum(this.violinData)
      .attr("class", "violin")
      .attr("d", this.violinArea)
      .attr("fill", this.config.colors[0])
      .attr("opacity", 0.8)
      .attr("stroke", "none");

    if (this.config.showMedian) {
      // Draw median line
      violinG
        .append("line")
        .attr("class", "median")
        .attr("x1", -this.config.maxWidth / 2)
        .attr("x2", this.config.maxWidth / 2)
        .attr("y1", this.yScale(this.stats.median))
        .attr("y2", this.yScale(this.stats.median))
        .attr("stroke", "white")
        .attr("stroke-width", 2);
    }

    if (this.config.showQuartiles) {
      // Draw quartile lines
      violinG
        .selectAll(".quartile")
        .data([this.stats.q1, this.stats.q3])
        .enter()
        .append("line")
        .attr("class", "quartile")
        .attr("x1", -this.config.maxWidth / 3)
        .attr("x2", this.config.maxWidth / 3)
        .attr("y1", (d) => this.yScale(d))
        .attr("y2", (d) => this.yScale(d))
        .attr("stroke", "white")
        .attr("stroke-width", 1)
        .attr("stroke-dasharray", "3,3");
    }

    // Add brush
    const brushG = this.g
      .append("g")
      .attr("class", "brush")
      .attr(
        "transform",
        `translate(${this.config.maxWidth - this.config.maxWidth},0)`
      );

    brushG.call(this.brush);

    // Restore brush extent if there was a previous selection
    if (this.brushedRange) {
      brushG.call(this.brush.move, this.brushedRange);
    }

    // Add axes if enabled
    if (this.config.axis) {
      this.drawAxes();
    }
  }

  drawAxes() {
    const yAxis = d3.axisLeft(this.yScale);

    this.g
      .append("g")
      .attr("class", "y-axis")
      .attr("transform", `translate(0,0)`)
      .call(yAxis);
  }

  handleBrush(event) {
    if (!event.selection) {
      // If brush is cleared
      this.brushedRange = null;
      this.selectedData.clear();
      this.dispatch.call("selectionChanged", this, []);
      return;
    }

    this.brushedRange = event.selection;
    const [y0, y1] = event.selection.map(this.yScale.invert);

    // Query data within the brushed range
    const query = `
      SELECT *
      FROM ${this.tableName}
      WHERE "${this.config.column}" >= ${y0}
      AND "${this.config.column}" <= ${y1}
    `;

    this.query(query).then((selectedData) => {
      this.selectedData = new Set(
        selectedData.map((d) => d[this.config.column])
      );
      this.dispatch.call("selectionChanged", this, selectedData);
    });
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
      .map((v) => (typeof v === "string" ? `'${v}'` : v))
      .join(",")})
    `;
    return await this.query(query);
  }

  async destroy() {
    if (this.brush) {
      this.g.selectAll(".brush").remove();
    }
    await super.destroy();
  }
}
