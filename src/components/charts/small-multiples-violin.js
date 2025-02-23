import { BaseSmallMultiples } from "../_base/small-multiples.js";
import { ViolinPlot } from "./violin-plot.js";

export class SmallMultiplesViolin extends BaseSmallMultiples {
  constructor(config) {
    const violinDefaults = {
      bandwidth: 0.4,
      showMedian: true,
      showQuartiles: true,
      violinWidth: 220, // Reduced width for better spacing
      violinHeight: 200, // Adjusted height
      gap: { horizontal: 60, vertical: 60 }, // Increased gaps
      margin: {
        top: 30, // Adjusted margins
        right: 30,
        bottom: 40,
        left: 50,
      },
    };

    // Merge margins
    const margin = {
      ...violinDefaults.margin,
      ...(config.margin || {}),
    };

    // Calculate effective dimensions
    const effectiveWidth =
      (config.violinWidth || violinDefaults.violinWidth) -
      margin.left -
      margin.right;
    const effectiveHeight =
      (config.violinHeight || violinDefaults.violinHeight) -
      margin.top -
      margin.bottom;

    super({
      ...violinDefaults,
      ...config,
      ChartClass: ViolinPlot,
      chartWidth: config.violinWidth || violinDefaults.violinWidth,
      chartHeight: config.violinHeight || violinDefaults.violinHeight,
      margin,
      gap: config.gap || violinDefaults.gap,
      showAxis: config.showAxis ?? true,
    });

    // Configure violin plot specifics
    this.violinConfig = {
      showMedian: config.showMedian ?? violinDefaults.showMedian,
      showQuartiles: config.showQuartiles ?? violinDefaults.showQuartiles,
      bandwidth: config.bandwidth ?? violinDefaults.bandwidth,
      maxWidth: effectiveWidth * 0.4, // Violin width relative to chart width
      colors: config.colors,
      margin,
    };

    // Calculate total width needed
    const totalColumns = Math.ceil(Math.sqrt(this.config.columns.length));
    const minWidth =
      totalColumns * (this.config.chartWidth + this.config.gap.horizontal) +
      margin.left +
      margin.right;

    // Adjust total width if needed
    if (this.config.width < minWidth) {
      this.config.width = minWidth;
    }
  }

  calculateMinimumWidth() {
    const chartsPerRow = Math.ceil(Math.sqrt(this.config.columns.length));
    return (
      chartsPerRow * (this.config.chartWidth + this.config.gap.horizontal) +
      this.config.margin.left +
      this.config.margin.right
    );
  }

  async createChart(column, index) {
    // Calculate position
    const chartsPerRow = Math.floor(
      (this.config.width - this.config.margin.left - this.config.margin.right) /
        (this.config.chartWidth + this.config.gap.horizontal)
    );
    const row = Math.floor(index / chartsPerRow);
    const col = index % chartsPerRow;

    // Calculate position with gaps
    const xOffset = col * (this.config.chartWidth + this.config.gap.horizontal);
    const yOffset = row * (this.config.chartHeight + this.config.gap.vertical);

    // Create chart configuration
    const chartConfig = {
      ...this.violinConfig,
      column,
      width:
        this.config.chartWidth -
        this.violinConfig.margin.left -
        this.violinConfig.margin.right,
      height:
        this.config.chartHeight -
        this.violinConfig.margin.top -
        this.violinConfig.margin.bottom,
      dataSource: this.config.dataSource,
      showAxis: this.config.showAxis,
      tableName: this.tableName,
    };

    const chart = new ViolinPlot(chartConfig);

    // Create the chart group
    const chartGroup = this.g
      .append("g")
      .attr("transform", `translate(${xOffset},${yOffset})`)
      .attr("class", `chart-group chart-group-${column}`);

    // Add title
    if (this.config.showTitle) {
      chartGroup
        .append("text")
        .attr("class", "chart-title")
        .attr("x", this.config.chartWidth / 2)
        .attr("y", 0)
        .attr("dy", "1em")
        .attr("text-anchor", "middle")
        .attr("font-size", "12px")
        .text(column);
    }

    // Create content group
    const contentGroup = chartGroup
      .append("g")
      .attr(
        "transform",
        `translate(${this.violinConfig.margin.left},${this.violinConfig.margin.top})`
      );

    chart.g = contentGroup;
    await chart.initialize();
    await chart.update(this.config.dataSource);

    return chart;
  }

  getChartsPerRow() {
    const availableWidth =
      this.config.width - this.config.margin.left - this.config.margin.right;
    const chartTotalWidth = this.config.chartWidth + this.config.gap.horizontal;
    return Math.max(1, Math.floor(availableWidth / chartTotalWidth));
  }

  async update() {
    console.log("[SmallMultiplesViolin] Starting update");

    // Ensure this.g is defined
    if (!this.g) {
      this.createSvg();
    }

    // Clear existing content
    this.g.selectAll("*").remove();
    this.charts = [];

    // Create charts
    for (let i = 0; i < this.config.columns.length; i++) {
      const column = this.config.columns[i];
      const chart = await this.createChart(column, i);
      this.charts.push(chart);
    }

    // Setup linked interactivity after all charts are created
    this.setupLinkedInteractivity();

    console.log(
      "[SmallMultiplesViolin] Update complete with charts:",
      this.charts.length
    );
  }
}
