import * as d3 from "npm:d3";

export class HistogramController {
  constructor(data, options = {}) {
    this.container = document.createElement("div");
    this.data = data;
    this.options = {
      width: 120,
      height: 60,
      margin: { top: 5, right: 8, bottom: 10, left: 8 },
      colors: ["steelblue", "#666"], // Primary and secondary colors
      ...options,
    };

    this.originalData = {
      bins: [],
      type: options.type || "ordinal",
    };

    this.currentData = {
      bins: [],
      type: options.type || "ordinal",
    };

    this.selected = new Set();
    this.highlightedData = null;
    this.baseOpacity = 0.3;
    this.setupContainer();
    this.initialize(data, options);
  }

  setupContainer() {
    Object.assign(this.container.style, {
      width: this.options.width + "px",
      height: this.options.height + "px",
      position: "relative",
    });
  }

  async initialize(data, options) {
    try {
      await this.createBins(data, options);
      this.render();
    } catch (error) {
      console.error("Failed to initialize histogram:", error);
      this.showError();
    }
  }

  async createBins(data, options) {
    if (!data || data.length === 0) {
      return;
    }

    const values = data.filter((d) => d != null);

    if (options.type === "continuous") {
      const binGenerator = d3
        .bin()
        .domain(d3.extent(values))
        .thresholds(options.thresholds || d3.thresholdSturges);

      this.originalData.bins = binGenerator(values).map((bin) => ({
        x0: bin.x0,
        x1: bin.x1,
        length: bin.length,
        values: bin,
      }));

      this.originalData.type = "continuous";
    } else {
      const counts = d3.rollup(
        values,
        (v) => v.length,
        (d) => d
      );
      this.originalData.bins = Array.from(counts, ([key, count]) => ({
        key,
        length: count,
        values: values.filter((v) => v === key),
      }));

      this.originalData.type = "ordinal";
    }

    // Initially, current data is the same as original
    this.currentData = structuredClone(this.originalData);
  }

  render() {
    this.container.innerHTML = "";

    const svg = d3
      .select(this.container)
      .append("svg")
      .attr("width", this.options.width)
      .attr("height", this.options.height);

    const { width, height } = this.getChartDimensions();
    const g = svg
      .append("g")
      .attr(
        "transform",
        `translate(${this.options.margin.left},${this.options.margin.top})`
      );

    // Create scales
    const x = this.createXScale(width);
    const y = this.createYScale(height);

    // If there's a selection in another column and we have highlighted data
    if (this.highlightedData) {
      // Draw original data as background in grey
      this.drawHistogramBars(
        g,
        this.originalData.bins,
        x,
        y,
        "#cccccc",
        this.baseOpacity
      );

      // Draw highlighted data in blue
      const highlightedBins = this.createBinsFromData(this.highlightedData);
      this.drawHistogramBars(
        g,
        highlightedBins,
        x,
        y,
        this.options.colors[0],
        1
      );
    } else if (this.selected.size > 0) {
      // Draw original data as background in grey
      this.drawHistogramBars(
        g,
        this.originalData.bins,
        x,
        y,
        "#cccccc",
        this.baseOpacity
      );

      // Draw selected bins in blue
      const selectedBins = this.originalData.bins.filter((bin) =>
        this.originalData.type === "continuous"
          ? this.selected.has(bin.x0)
          : this.selected.has(bin.key)
      );
      this.drawHistogramBars(g, selectedBins, x, y, this.options.colors[0], 1);
    } else {
      // No selection, draw all bars in blue
      this.drawHistogramBars(
        g,
        this.originalData.bins,
        x,
        y,
        this.options.colors[0],
        1
      );
    }

    // Add brush for continuous data or click handlers for ordinal
    if (this.originalData.type === "continuous") {
      this.setupBrush(svg, width, height);
    } else {
      this.setupOrdinalInteraction(g, x, y);
    }
  }

  getChartDimensions() {
    return {
      width:
        this.options.width -
        this.options.margin.left -
        this.options.margin.right,
      height:
        this.options.height -
        this.options.margin.top -
        this.options.margin.bottom,
    };
  }

  createXScale(width) {
    if (this.originalData.type === "continuous") {
      return d3
        .scaleLinear()
        .domain([
          d3.min(this.originalData.bins, (d) => d.x0),
          d3.max(this.originalData.bins, (d) => d.x1),
        ])
        .range([0, width]);
    } else {
      return d3
        .scaleBand()
        .domain(this.originalData.bins.map((d) => d.key))
        .range([0, width])
        .padding(0.1);
    }
  }

  createYScale(height) {
    const maxCount = d3.max(this.originalData.bins, (d) => d.length);
    return d3.scaleLinear().domain([0, maxCount]).range([height, 0]);
  }

  drawHistogramBars(g, bins, x, y, color, opacity) {
    const { height } = this.getChartDimensions();

    if (this.originalData.type === "continuous") {
      g.selectAll(`.bar-${color}`)
        .data(bins)
        .join("rect")
        .attr("class", `bar-${color}`)
        .attr("x", (d) => x(d.x0))
        .attr("width", (d) => Math.max(0, x(d.x1) - x(d.x0) - 1))
        .attr("y", (d) => y(d.length))
        .attr("height", (d) => height - y(d.length))
        .attr("fill", color)
        .attr("opacity", opacity)
        .attr("rx", 2);
    } else {
      g.selectAll(`.bar-${color}`)
        .data(bins)
        .join("rect")
        .attr("class", `bar-${color}`)
        .attr("x", (d) => x(d.key))
        .attr("width", x.bandwidth())
        .attr("y", (d) => y(d.length))
        .attr("height", (d) => height - y(d.length))
        .attr("fill", (d) =>
          this.selected.has(d.key) ? this.options.colors[0] : color
        )
        .attr("opacity", opacity)
        .attr("rx", 2);
    }
  }

  setupBrush(svg, width, height) {
    const brush = d3
      .brushX()
      .extent([
        [0, 0],
        [width, height],
      ])
      .on("end", (event) => this.handleBrush(event));

    svg
      .append("g")
      .attr("class", "brush")
      .attr(
        "transform",
        `translate(${this.options.margin.left},${this.options.margin.top})`
      )
      .call(brush);
  }

  setupOrdinalInteraction(svg, x, y) {
    const { height } = this.getChartDimensions();

    svg
      .selectAll(".bar-overlay")
      .data(this.originalData.bins)
      .join("rect")
      .attr("class", "bar-overlay")
      .attr("x", (d) => x(d.key))
      .attr("width", x.bandwidth())
      .attr("y", 0)
      .attr("height", height)
      .attr("fill", "transparent")
      .on("click", (event, d) => this.handleBarClick(d))
      .on("mouseover", (event, d) => this.showTooltip(d))
      .on("mouseout", () => this.hideTooltip());
  }

  handleBrush(event) {
    if (!event.selection) {
      this.resetSelection();
      return;
    }

    const [x0, x1] = event.selection;
    const selected = new Set();

    this.originalData.bins.forEach((bin) => {
      if (bin.x0 >= x0 && bin.x1 <= x1) {
        bin.values.forEach((v) => selected.add(v));
      }
    });

    this.updateSelection(selected);
  }

  handleBarClick(bin) {
    const newSelected = new Set(this.selected);

    if (newSelected.has(bin.key)) {
      newSelected.delete(bin.key);
    } else {
      newSelected.add(bin.key);
    }

    this.updateSelection(newSelected);
  }

  updateSelection(selected) {
    this.selected = selected;

    // Get the data for the selected bins
    const selectedData = Array.from(selected).flatMap(
      (key) =>
        this.originalData.bins.find((b) =>
          this.originalData.type === "continuous" ? b.x0 === key : b.key === key
        )?.values || []
    );

    // Update highlighted data
    this.highlightedData = selectedData;

    // Update current data bins based on selection
    if (this.originalData.type === "continuous") {
      this.currentData.bins = this.createBinsFromData(selectedData);
    } else {
      this.currentData.bins = this.originalData.bins.map((bin) => ({
        ...bin,
        length: selected.has(bin.key) ? bin.length : 0,
      }));
    }

    this.render();

    // Notify table of selection if available
    if (this.table) {
      this.table.updateSelection(selected);
    }
  }

  resetSelection() {
    this.selected.clear();
    this.highlightedData = null;
    this.currentData = structuredClone(this.originalData);
    this.render();

    if (this.table) {
      this.table.clearSelection();
    }
  }

  createBinsFromData(data) {
    if (!data || data.length === 0) {
      return [];
    }

    if (this.originalData.type === "continuous") {
      const binGenerator = d3
        .bin()
        .domain([
          d3.min(this.originalData.bins, (d) => d.x0),
          d3.max(this.originalData.bins, (d) => d.x1),
        ])
        .thresholds(this.originalData.bins.map((b) => b.x0));

      return binGenerator(data).map((bin) => ({
        x0: bin.x0,
        x1: bin.x1,
        length: bin.length,
        values: bin,
      }));
    } else {
      const counts = d3.rollup(
        data,
        (v) => v.length,
        (d) => d
      );
      return Array.from(counts, ([key, count]) => ({
        key,
        length: count,
        values: data.filter((v) => v === key),
      }));
    }
  }

  updateData(newData) {
    // Reset selections when data changes
    this.selected.clear();
    this.highlightedData = null;
    this.initialize(newData, this.options);
  }

  showTooltip(bin) {
    // Implement tooltip display
  }

  hideTooltip() {
    // Implement tooltip hiding
  }

  showError() {
    this.container.innerHTML = `
      <div style="color: #ff4444; font-size: 10px; text-align: center; padding: 20px;">
        Error loading data
      </div>
    `;
  }

  getNode() {
    return this.container;
  }
}
