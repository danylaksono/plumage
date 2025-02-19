import * as d3 from "npm:d3";
import _ from "npm:lodash";
import { BaseVisualization } from "./base.js";

export class Histogram extends BaseVisualization {
  constructor(config) {
    const histogramDefaults = {
      column: null,
      binThreshold: null,
      maxOrdinalBins: 20,
      showLabelsBelow: false,
    };

    super({ ...histogramDefaults, ...config });
    console.log("DuckDB Histogram initialized with config:", this.config);

    this.data = [];
    this.bins = [];
    this.selectedBins = new Set();
    this.type = null;

    // Create histogram-specific elements
    if (!this.initialized) {
      this.createHistogramElements();
    }
  }

  createHistogramElements() {
    if (!this.g) {
        this.createSvg();  // Ensure this.g is defined from base
    }
    // Create tooltip
    this.tooltip = d3
      .select("body")
      .append("div")
      .attr("class", "histogram-tooltip")
      .style("opacity", 0)
      .style("position", "absolute")
      .style("background", "white")
      .style("padding", "5px")
      .style("border", "1px solid #ccc");

    // Labels below the histogram
    if (this.config.showLabelsBelow) {
      this.labelGroup = this.g
        .append("g")
        .attr("class", "labels")
        .attr("transform", `translate(0,${this.config.height + 25})`);
    }

    // Brush for drag selection
    this.brush = d3
      .brushX()
      .extent([
        [0, 0],
        [this.config.width, this.config.height],
      ])
      .on("end", this.handleBrush.bind(this));

    // Click outside bins to clear selection
    this.g.on("click", (event) => {
      if (!event.target.classList.contains("bar")) {
        this.clearSelection();
      }
    });
  }

  // Override the update method
  async update(data = null) {
    if (!this.initialized) {
      await this.initialize();
    }

    if (data) {
      this.data = data;
      this.processData();
    } else if (this.dataProcessor) {
      await this.processDataWithDuckDB();
    } else if (this.config.dataSource) {
      this.data = this.config.dataSource;
      this.processData();
    }

    console.log("Histogram: Data after processing:", this.data);
    console.log("Histogram: Bins after processing:", this.bins);
    console.log("Histogram: Type after processing:", this.type);

    this.draw();
    return this;
  }

  async processDataWithDuckDB() {
    const { column } = this.config;
    this.type = await this.dataProcessor.getTypeFromDuckDB(column);
    this.bins = await this.dataProcessor.binDataWithDuckDB(
      column,
      this.type,
      this.config.maxOrdinalBins
    );

    this.xScale = this.createXScale();
    this.yScale = this.createYScale();

    console.log("Histogram: xScale domain:", this.xScale.domain());
    console.log("Histogram: yScale domain:", this.yScale.domain());
  }

  // Keep all your existing methods below, but remove duplicate methods
  // that are already in BaseVisualization:
  // - initialize()
  // - destroy()
  // - createSvg()
  // - logQuery()
  // ...existing histogram-specific methods...

  // Add this simple helper to determine data type from the first datum
  getType(data, column) {
    if (!data || data.length === 0) return "ordinal";
    const sample = data[0][column];
    if (sample instanceof Date) return "date";
    if (typeof sample === "number") return "continuous";
    return "ordinal";
  }

  async getSelectedData() {
    if (this.selectedBins.size === 0) return [];

    const selectedBins = Array.from(this.selectedBins);
    let query;

    if (this.type === "ordinal") {
      const values = selectedBins.map((bin) => `'${bin.key}'`).join(",");
      query = `
        SELECT *
        FROM ${this.tableName}
        WHERE ${this.config.column} IN (${values})
      `;
    } else {
      const conditions = selectedBins
        .map(
          (bin) =>
            `(${this.config.column} >= ${bin.x0} AND ${this.config.column} < ${bin.x1})`
        )
        .join(" OR ");

      query = `
        SELECT *
        FROM ${this.tableName}
        WHERE ${conditions}
      `;
    }

    // Changed from this.duckdb.query to this.conn.query
    const result = await this.dataProcessor.query(query);
    return result;
  }

  async loadJSONData(data) {
    try {
      if (data.length === 0) {
        throw new Error("Empty data array provided");
      }

      // Infer schema from the first object
      const schema = this.inferSchema(data[0]);
      const createTableSQL = this.generateCreateTableSQL(schema);

      // Create the table
      await this.conn.query(createTableSQL);
      console.log("Table created successfully");

      // Insert data in batches using SQL INSERT
      const batchSize = 1000;
      for (let i = 0; i < data.length; i += batchSize) {
        const batch = data.slice(i, i + batchSize);

        // Generate INSERT query for the batch
        const columns = Object.keys(schema)
          .map((col) => `"${col}"`)
          .join(", ");
        const values = batch
          .map((row) => {
            const rowValues = Object.values(row).map((val) => {
              if (val === null || val === undefined) return "NULL";
              if (typeof val === "string")
                return `'${val.replace(/'/g, "''")}'`; // Escape single quotes
              if (val instanceof Date) return `'${val.toISOString()}'`; // Format dates
              return val;
            });
            return `(${rowValues.join(", ")})`;
          })
          .join(", ");

        const insertQuery = `INSERT INTO ${this.tableName} (${columns}) VALUES ${values}`;
        await this.conn.query(insertQuery);
        console.log(`Inserted batch ${i / batchSize + 1}`);
      }

      console.log("JSON data loaded successfully");
    } catch (error) {
      console.error("Failed to load JSON data:", error);
      throw new Error(`Failed to load JSON data: ${error.message}`);
    }
  }

  async loadFileData(file, format) {
    try {
      const buffer = await file.arrayBuffer();
      const uint8Array = new Uint8Array(buffer);

      if (format === "parquet") {
        await this.duckdb.registerFileBuffer(file.name, uint8Array);
        await this.conn.query(`
          CREATE TABLE ${this.tableName} AS 
          SELECT * FROM parquet_scan('${file.name}')
        `);
      } else if (format === "csv") {
        await this.duckdb.registerFileBuffer(file.name, uint8Array);
        await this.conn.query(`
          CREATE TABLE ${this.tableName} AS 
          SELECT * FROM read_csv_auto('${file.name}')
        `);
      } else {
        throw new Error("Unsupported file format");
      }
    } catch (error) {
      throw new Error(`Failed to load file: ${error.message}`);
    }
  }

  async loadURLData(url, format) {
    try {
      const response = await fetch(url);
      const buffer = await response.arrayBuffer();
      const uint8Array = new Uint8Array(buffer);
      const filename = url.split("/").pop();

      await this.loadFileData(
        new File([uint8Array], filename, { type: `application/${format}` }),
        format
      );
    } catch (error) {
      throw new Error(`Failed to load URL data: ${error.message}`);
    }
  }

  inferSchema(obj) {
    const schema = {};
    for (const [key, value] of Object.entries(obj)) {
      if (value instanceof Date) {
        schema[key] = "TIMESTAMP";
      } else if (typeof value === "number") {
        schema[key] = Number.isInteger(value) ? "INTEGER" : "DOUBLE";
      } else if (typeof value === "boolean") {
        schema[key] = "BOOLEAN";
      } else {
        schema[key] = "VARCHAR";
      }
    }
    return schema;
  }

  escape(name) {
    return `"${name}"`;
  }

  async describeColumn(column) {
    const query = `DESCRIBE ${this.escape(this.tableName)}`;
    const result = await this.conn.query(query);
    const columnInfo = result
      .toArray()
      .find((row) => row.column_name === column);
    return {
      name: columnInfo.column_name,
      type: this.getDuckDBType(columnInfo.column_type),
      nullable: columnInfo.null !== "NO",
      databaseType: columnInfo.column_type,
    };
  }

  generateCreateTableSQL(schema) {
    const columns = Object.entries(schema)
      .map(([name, type]) => `"${name}" ${type}`)
      .join(", ");
    return `CREATE TABLE ${this.tableName} (${columns})`;
  }

  createXScale() {
    const {
      type,
      bins,
      config: { width },
    } = this;

    if (type === "ordinal") {
      return d3
        .scaleBand()
        .domain(bins.map((b) => b.key))
        .range([0, width])
        .padding(0.1);
    }

    const extent = d3.extent(bins.flatMap((b) => [b.x0, b.x1]));
    return type === "date"
      ? d3.scaleTime().domain(extent).range([0, width])
      : d3.scaleLinear().domain(extent).range([0, width]);
  }

  createYScale() {
    const max = d3.max(this.bins, (b) => Number(b.length)); // Convert BigInt to Number
    return d3
      .scaleLinear()
      .domain([0, max])
      .nice()
      .range([this.config.height, 0]);
  }

  draw() {
    console.log("Histogram: Drawing with bins:", this.bins);
    this.drawBars(); // Draw bars first
    if (this.config.axis) this.drawAxes(); // Draw axes if enabled
    if (this.config.showLabelsBelow) this.drawLabels(); // Draw labels if enabled

    // Draw brush overlay on top of bars
    if (this.config.selectionMode === "drag") {
      this.g
        .append("g") // Append brush group after bars
        .attr("class", "brush")
        .call(this.brush);
    }
  }

  drawBars() {
    const {
      xScale,
      yScale,
      config: { height },
      bins,
    } = this;

    console.log("Histogram: drawBars - xScale:", xScale);
    console.log("Histogram: drawBars - yScale:", yScale);
    console.log("Histogram: drawBars - height:", height);
    console.log("Histogram: drawBars - bins:", bins);

    const bars = this.g.selectAll(".bar").data(bins, (b) => b.x0);

    bars.exit().remove();

    const enter = bars
      .enter()
      .append("rect")
      .attr("class", "bar")
      .attr("stroke", "white")
      .attr("stroke-width", "1")
      .on("mouseover", (event, d) => this.handleMouseOver(event, d))
      .on("mouseout", (event, d) => this.handleMouseOut(event, d))
      .on("click", (event, d) => this.handleClick(event, d));

    bars
      .merge(enter)
      .attr("x", (b) => xScale(b.x0))
      .attr("y", (b) => yScale(Number(b.length))) // Convert BigInt to Number
      .attr("width", (b) => this.getBarWidth(b))
      .attr("height", (b) => height - yScale(Number(b.length))) // Convert BigInt to Number
      .attr("fill", (b) =>
        this.selectedBins.has(b) ? this.config.colors[1] : this.config.colors[0]
      );
  }

  drawLabels() {
    const { xScale, bins, type } = this;

    // Clear existing labels
    this.labelGroup.selectAll(".label").remove();

    // Calculate total width for centering
    const totalWidth = this.config.width;

    // Update labelGroup position to align with the bars
    this.labelGroup.attr(
      "transform",
      `translate(0,${this.config.height + 25})`
    );

    // Add new labels
    this.labelGroup
      .selectAll(".label")
      .data(bins)
      .enter()
      .append("text")
      .attr("class", "label")
      .attr("text-anchor", "middle")
      .attr("fill", "#333")
      .attr("font-size", "12px")
      .attr("x", (b) => {
        if (type === "ordinal") {
          return xScale(b.x0) + xScale.bandwidth() / 2;
        }
        // For continuous/date data, center between x0 and x1
        const x0 = xScale(b.x0);
        const x1 = xScale(b.x1);
        return x0 + (x1 - x0) / 2;
      })
      .text((b) => {
        if (type === "ordinal") {
          return `${b.key}: ${b.length}`;
        }
        if (type === "date") {
          const formatter = d3.timeFormat("%Y-%m-%d");
          return `${formatter(b.x0)}: ${b.length}`;
        }
        // For continuous numeric data, format numbers nicely
        return `${b.x0.toFixed(1)}-${b.x1.toFixed(1)}: ${b.length}`;
      });
  }

  getBarWidth(b) {
    if (this.type === "ordinal") {
      // Use scaleBand bandwidth for ordinal data
      return this.xScale.bandwidth();
    }

    // For continuous/date data, calculate width from scales
    const width = Math.max(1, this.xScale(b.x1) - this.xScale(b.x0));
    return isNaN(width) ? 0 : width;
  }

  handleMouseOver(event, d) {
    if (this.config.showLabelsBelow) {
      // Hide tooltip when showing labels
      this.tooltip.style("opacity", 0);

      // Clear previous labels
      this.labelGroup.selectAll(".label").remove();

      // Add new label
      this.labelGroup
        .append("text")
        .attr("class", "label")
        .attr("text-anchor", "middle")
        .attr("fill", "#333")
        .attr("font-size", "12px")
        .text(`${d.key || d.x0}: ${d.length}`);
    } else {
      // Show tooltip if labels are disabled
      this.tooltip
        .style("opacity", 1)
        .html(`Category: ${d.key || d.x0}<br>Count: ${d.length}`)
        .style("left", `${event.pageX}px`)
        .style("top", `${event.pageY + 10}px`);
    }

    d3.select(event.currentTarget).attr("fill", this.config.colors[1]);
  }

  handleMouseOut(event) {
    // Clear both tooltip and labels
    this.tooltip.style("opacity", 0);
    if (this.config.showLabelsBelow) {
      this.labelGroup.selectAll(".label").remove();
    }

    const bin = this.bins.find((b) => b.x0 === event.currentTarget.__data__.x0);
    d3.select(event.currentTarget).attr(
      "fill",
      this.selectedBins.has(bin) ? this.config.colors[1] : this.config.colors[0]
    );
  }

  handleClick(event, d) {
    const isSelected = this.selectedBins.has(d);

    if (event.ctrlKey && this.config.selectionMode === "multiple") {
      this.selectedBins.has(d)
        ? this.selectedBins.delete(d)
        : this.selectedBins.add(d);
    } else {
      if (isSelected) {
        this.selectedBins.clear();
      } else {
        this.selectedBins.clear();
        this.selectedBins.add(d);
      }
    }

    // Get selected data and dispatch when ready
    this.getSelectedData().then((selectedData) => {
      this.dispatch.call("selectionChanged", this, selectedData);
    });

    this.drawBars();
  }

  handleBrush(event) {
    if (!event.selection) {
      this.clearSelection();
      return;
    }

    const [x0, x1] = event.selection;
    const selected = this.bins.filter((b) => {
      const binLeft = this.xScale(b.x0);
      const binRight = this.xScale(b.x1);
      return binLeft <= x1 && binRight >= x0;
    });

    this.selectedBins = new Set(selected);
    this.drawBars();

    // Get selected data and dispatch when ready
    this.getSelectedData().then((selectedData) => {
      this.dispatch.call("selectionChanged", this, selectedData);
    });
  }

  clearSelection() {
    this.selectedBins.clear();
    this.drawBars();
    // Return empty array immediately since there's no selection
    this.dispatch.call("selectionChanged", this, []);
  }

  reset() {
    this.selectedBins.clear();
    this.processData();
    this.draw();
  }

  on(event, callback) {
    this.dispatch.on(event, callback);
    return this;
  }

  // Utility methods
  handleLargeOrdinalBins(bins) {
    const sorted = _.orderBy(bins, ["length"], ["desc"]);
    const topBins = sorted.slice(0, this.config.maxOrdinalBins - 1);
    const others = sorted.slice(this.config.maxOrdinalBins - 1);
    const otherBin = {
      key: "Other",
      length: _.sumBy(others, "length"),
      x0: "Other",
      x1: "Other",
    };
    return [...topBins, otherBin];
  }

  drawAxes() {
    // Remove existing axes
    this.g.selectAll(".axis").remove();

    // Create X axis
    const xAxis = d3.axisBottom(this.xScale).tickFormat((d) => {
      if (this.type === "date") {
        return d3.timeFormat("%Y-%m-%d")(d);
      }
      return d;
    });

    this.g
      .append("g")
      .attr("class", "axis x-axis")
      .attr("transform", `translate(0,${this.config.height})`)
      .call(xAxis)
      .selectAll("text")
      .style("text-anchor", "end")
      .attr("transform", "rotate(-45)");

    // Create Y axis
    const yAxis = d3.axisLeft(this.yScale).ticks(5);

    this.g.append("g").attr("class", "axis y-axis").call(yAxis);
  }
}
