import { BaseVisualization } from "./base.js";
import * as d3 from "npm:d3";

export class ScatterPlot extends BaseVisualization {
  constructor(config) {
    const scatterDefaults = {
      xColumn: null,
      yColumn: null,
      radius: 5,
      opacity: 0.6,
    };

    super({ ...scatterDefaults, ...config });
    this.selectedPoints = new Set();
  }

  async processData() {
    const query = `
      SELECT 
        ${this.config.xColumn},
        ${this.config.yColumn},
        COUNT(*) as count
      FROM ${this.tableName}
      WHERE ${this.config.xColumn} IS NOT NULL 
        AND ${this.config.yColumn} IS NOT NULL
      GROUP BY ${this.config.xColumn}, ${this.config.yColumn}
    `;

    this.data = await this.query(query);

    this.xScale = d3
      .scaleLinear()
      .domain(d3.extent(this.data, (d) => d[this.config.xColumn]))
      .range([0, this.config.width]);

    this.yScale = d3
      .scaleLinear()
      .domain(d3.extent(this.data, (d) => d[this.config.yColumn]))
      .range([this.config.height, 0]);
  }

  draw() {
    this.g
      .selectAll(".point")
      .data(this.data)
      .join("circle")
      .attr("class", "point")
      .attr("cx", (d) => this.xScale(d[this.config.xColumn]))
      .attr("cy", (d) => this.yScale(d[this.config.yColumn]))
      .attr("r", this.config.radius)
      .attr("fill", this.config.colors[0])
      .attr("opacity", this.config.opacity)
      .on("click", (event, d) => this.handleClick(event, d));

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

  async handleClick(event, d) {
    const query = `
      SELECT *
      FROM ${this.tableName}
      WHERE ${this.config.xColumn} = ${d[this.config.xColumn]}
        AND ${this.config.yColumn} = ${d[this.config.yColumn]}
    `;

    const selectedData = await this.query(query);
    this.dispatch.call("selectionChanged", this, selectedData);

    d3.select(event.currentTarget).attr("fill", this.config.colors[1]);
  }

  async update() {
    if (!this.initialized) {
      await this.initialize();
    }

    await this.processData();
    this.draw();
    return this;
  }
}
