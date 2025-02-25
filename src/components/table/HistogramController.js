import * as d3 from "npm:d3";

export class HistogramController {
  constructor(data, options = {}) {
    this.container = document.createElement("div");
    this.data = data;
    this.options = {
      width: 120,
      height: 60,
      margin: { top: 5, right: 8, bottom: 10, left: 8 },
      colors: ["steelblue", "#ccc"], // Primary and secondary colors
      textColor: "#555",
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
    // Initialize with all data highlighted by default
    this.highlightedData = data;
    this.highlightedBins = [];
    this.baseOpacity = 0.7;
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
      console.log("Initializing histogram:", {
        columnName: this.columnName,
        dataLength: data?.length,
        type: options?.type,
        hasBins: Boolean(options?.bins),
      });

      if (options?.bins) {
        // If pre-binned data is provided, use it directly
        this.originalData.bins = options.bins;
        this.originalData.type = options.type || "ordinal";

        // For unique columns, store unique count
        if (options.type === "unique" && options.uniqueCount) {
          this.uniqueCount = options.uniqueCount;
        }
      } else {
        // Otherwise create bins from raw data
        await this.createBins(data, {
          type: options?.type || "ordinal",
          thresholds: options?.thresholds,
          binInfo: options?.binInfo,
        });
      }

      // Store original data type and format
      this.dataType = options?.type || "ordinal";

      // Initially, all data is highlighted by default
      this.highlightedData = data;
      this.highlightedBins = structuredClone(this.originalData.bins);

      this.render();
    } catch (error) {
      console.error("Failed to initialize histogram:", error);
      this.showError("Failed to initialize histogram");
    }
  }

  async createBins(data, options) {
    if (!data || data.length === 0) {
      return;
    }

    const values = data.filter((d) => d != null);

    if (options.type === "continuous") {
      const extent = d3.extent(values);
      const binGenerator = d3
        .bin()
        .domain(extent)
        .thresholds(options.thresholds || d3.thresholdSturges);

      const bins = binGenerator(values);
      this.originalData.bins = bins.map((bin) => ({
        x0: bin.x0,
        x1: bin.x1,
        length: bin.length,
        values: Array.from(bin), // Store original values
        key: bin.x0, // Store x0 as key for consistent reference
      }));

      this.originalData.type = "continuous";
    } else {
      // For ordinal data, preserve exact values
      const uniqueValues = new Map();
      values.forEach((value) => {
        const key = String(value);
        if (!uniqueValues.has(key)) {
          uniqueValues.set(key, {
            originalValue: value,
            count: 0,
            values: [],
          });
        }
        const entry = uniqueValues.get(key);
        entry.count++;
        entry.values.push(value);
      });

      this.originalData.bins = Array.from(uniqueValues.values()).map(
        (entry) => ({
          key: entry.originalValue, // Use original value directly as key
          length: entry.count,
          values: entry.values,
          value: entry.originalValue, // Keep original value for reference
        })
      );

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

    // Check if this is a unique column first
    if (this.options.type === "unique" || this.originalData.type === "unique") {
      this.renderUniqueColumn(svg);
      return;
    }

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

    // Draw background histogram (original data) with secondary color when there's a selection
    if (this.selected.size > 0 || (this.highlightedData && this.highlightedData.length > 0)) {
      this.drawHistogramBars(
        g,
        this.originalData.bins,
        x,
        y,
        this.options.colors[1],
        this.baseOpacity
      );
    }

    // Draw highlighted data histogram with primary color
    if (this.highlightedData && this.highlightedData.length > 0) {
      const highlightedBins = this.createBinsFromData(this.highlightedData);
      this.drawHistogramBars(
        g,
        highlightedBins,
        x,
        y,
        this.options.colors[0],
        1.0
      );
    } else if (!this.selected.size) {
      // If no selection and no highlight, show all data in primary color
      this.drawHistogramBars(
        g,
        this.originalData.bins,
        x,
        y,
        this.options.colors[0],
        1.0
      );
    }

    // Draw selected bins with a stroke if there's a local selection
    if (this.selected.size > 0) {
      let selectedBins;
      if (this.originalData.type === "continuous") {
        const selectedRange = Array.from(this.selected)[0];
        selectedBins = this.originalData.bins.filter(
          (bin) => bin.x0 >= selectedRange.min && bin.x1 <= selectedRange.max
        );
      } else {
        selectedBins = this.originalData.bins.filter((bin) =>
          this.selected.has(bin.key)
        );
      }
      
      this.drawHistogramBars(
        g,
        selectedBins,
        x,
        y,
        this.options.colors[0],
        1.0,
        true
      );
    }

    // Add x-axis for continuous data
    if (this.originalData.type === "continuous") {
      const xAxis = d3
        .axisBottom(x)
        .ticks(3)
        .tickSize(3)
        .tickFormat(d => {
          if (Math.abs(d) >= 1000000) return d3.format(".1f")(d / 1000000) + "M";
          if (Math.abs(d) >= 1000) return d3.format(".1f")(d / 1000) + "k";
          return d3.format(d % 1 === 0 ? "d" : ".1f")(d);
        });

      g.append("g")
        .attr("class", "x-axis")
        .attr("transform", `translate(0,${height})`)
        .call(xAxis)
        .call(g => g.select(".domain").remove())
        .call(g => g.selectAll(".tick line").attr("stroke", "#ddd"))
        .call(g => g.selectAll(".tick text")
          .attr("fill", this.options.textColor)
          .style("font-size", "7px"));
    }

    // Add interaction handlers
    if (this.originalData.type === "continuous") {
      this.setupBrush(svg, width, height);
    } else {
      this.setupOrdinalInteraction(g, x, y);
    }
  }

  renderUniqueColumn(svg) {
    const { width, height } = this.getChartDimensions();
    const g = svg
      .append("g")
      .attr(
        "transform",
        `translate(${this.options.margin.left},${this.options.margin.top})`
      );

    // Create a subtle background
    g.append("rect")
      .attr("width", width)
      .attr("height", height)
      .attr("fill", "#f7f7f7")
      .attr("rx", 3);

    // Add text indicating unique values
    g.append("text")
      .attr("x", width / 2)
      .attr("y", height / 2 - 6)
      .attr("text-anchor", "middle")
      .attr("dominant-baseline", "middle")
      .attr("fill", this.options.textColor)
      .style("font-size", "10px")
      .style("font-style", "italic")
      .text("Unique Values");

    // Add count
    const uniqueCount = this.options.uniqueCount || 0;
    g.append("text")
      .attr("x", width / 2)
      .attr("y", height / 2 + 10)
      .attr("text-anchor", "middle")
      .attr("dominant-baseline", "middle")
      .attr("fill", this.options.textColor)
      .style("font-size", "9px")
      .text(`(${uniqueCount.toLocaleString()} entries)`);
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
      const extent = [
        d3.min(this.originalData.bins, (d) => d.x0),
        d3.max(this.originalData.bins, (d) => d.x1),
      ];
      return d3.scaleLinear().domain(extent).range([0, width]).nice(); // Nice the scale for better brush interaction
    } else {
      return d3
        .scaleBand()
        .domain(this.originalData.bins.map((d) => d.key))
        .range([0, width])
        .padding(0.1);
    }
  }

  createYScale(height) {
    try {
      // Convert BigInts to Numbers if needed before using d3.max
      const maxCount = d3.max(this.originalData.bins, (d) => {
        // Handle potential BigInt values safely
        if (typeof d.length === "bigint") {
          // Convert BigInt to Number safely, if it's small enough
          if (d.length <= Number.MAX_SAFE_INTEGER) {
            return Number(d.length);
          }
          console.warn(
            "BigInt value too large for precise Number conversion:",
            d.length
          );
          return Number.MAX_SAFE_INTEGER; // Use a safe fallback
        }
        return d.length;
      });

      return d3
        .scaleLinear()
        .domain([0, maxCount || 1])
        .range([height, 0]);
    } catch (error) {
      console.error("Error creating Y scale:", error);
      // Fallback to a simple scale if there's an error
      return d3.scaleLinear().domain([0, 1]).range([height, 0]);
    }
  }

  drawHistogramBars(g, bins, x, y, color, opacity, isSelected = false) {
    const { height } = this.getChartDimensions();

    if (this.originalData.type === "continuous") {
      g.selectAll(
        `.bar-${color.replace("#", "")}${isSelected ? "-selected" : ""}`
      )
        .data(bins)
        .join("rect")
        .attr(
          "class",
          `bar-${color.replace("#", "")}${isSelected ? "-selected" : ""}`
        )
        .attr("x", (d) => {
          try {
            return x(d.x0);
          } catch (e) {
            console.warn("Error setting x attribute:", e, d);
            return 0;
          }
        })
        .attr("width", (d) => {
          try {
            return Math.max(0, x(d.x1) - x(d.x0) - 1);
          } catch (e) {
            console.warn("Error setting width attribute:", e, d);
            return 0;
          }
        })
        .attr("y", (d) => {
          try {
            // Handle BigInt conversion
            const length =
              typeof d.length === "bigint" ? Number(d.length) : d.length;
            return y(length);
          } catch (e) {
            console.warn("Error setting y attribute:", e, d);
            return height;
          }
        })
        .attr("height", (d) => {
          try {
            // Handle BigInt conversion
            const length =
              typeof d.length === "bigint" ? Number(d.length) : d.length;
            return height - y(length);
          } catch (e) {
            console.warn("Error setting height attribute:", e, d);
            return 0;
          }
        })
        .attr("fill", color)
        .attr("opacity", opacity)
        .attr("rx", 2) // Rounded corners
        .attr("stroke", isSelected ? "#000" : "none")
        .attr("stroke-width", isSelected ? 1 : 0)
        .attr("shape-rendering", "crispEdges")
        .on("mouseover", function () {
          d3.select(this).attr("opacity", Math.min(1, opacity + 0.2));
        })
        .on("mouseout", function () {
          d3.select(this).attr("opacity", opacity);
        });
    } else {
      g.selectAll(
        `.bar-${color.replace("#", "")}${isSelected ? "-selected" : ""}`
      )
        .data(bins)
        .join("rect")
        .attr(
          "class",
          `bar-${color.replace("#", "")}${isSelected ? "-selected" : ""}`
        )
        .attr("x", (d) => {
          try {
            return x(d.key);
          } catch (e) {
            console.warn("Error setting x attribute:", e, d);
            return 0;
          }
        })
        .attr("width", (d) => {
          try {
            return x.bandwidth();
          } catch (e) {
            console.warn("Error setting width attribute:", e, d);
            return 0;
          }
        })
        .attr("y", (d) => {
          try {
            // Handle BigInt conversion
            const length =
              typeof d.length === "bigint" ? Number(d.length) : d.length;
            return y(length);
          } catch (e) {
            console.warn("Error setting y attribute:", e, d);
            return height;
          }
        })
        .attr("height", (d) => {
          try {
            // Handle BigInt conversion
            const length =
              typeof d.length === "bigint" ? Number(d.length) : d.length;
            return height - y(length);
          } catch (e) {
            console.warn("Error setting height attribute:", e, d);
            return 0;
          }
        })
        .attr("fill", color)
        .attr("opacity", opacity)
        .attr("rx", 2) // Rounded corners
        .attr("stroke", isSelected ? "#000" : "none")
        .attr("stroke-width", isSelected ? 1 : 0)
        .attr("shape-rendering", "crispEdges")
        .on("mouseover", function () {
          d3.select(this).attr("opacity", Math.min(1, opacity + 0.2));
        })
        .on("mouseout", function () {
          d3.select(this).attr("opacity", opacity);
        });
    }
  }

  setupBrush(svg, width, height) {
    svg.selectAll(".brush").remove();

    // Only setup brush for continuous data
    if (this.originalData.type !== "continuous") {
      return;
    }

    const brushG = svg
      .append("g")
      .attr("class", "brush")
      .attr(
        "transform",
        `translate(${this.options.margin.left},${this.options.margin.top})`
      );

    const brush = d3
      .brushX()
      .extent([
        [0, 0],
        [width, height],
      ])
      // Disable brush move - only allow resize
      .on("start brush", (event) => {
        // Remove tooltip during brushing
        this.hideBrushTooltip();
        
        if (!event.selection) return;
        
        // Update brush selection style
        brushG
          .select(".selection")
          .attr("fill", this.options.colors[0])
          .attr("fill-opacity", 0.15);
      })
      .on("end", (event) => {
        if (!event.selection) {
          if (this.table) {
            this.selected.clear();
            this.table.clearSelection();
            this.render();
          }
          return;
        }

        const scale = this.createXScale(width);
        const [x0, x1] = event.selection.map((x) => scale.invert(x));

        // Store selected range
        this.selected = new Set([{ min: x0, max: x1 }]);

        // Convert coordinates to data values
        const selectedValues = this.originalData.bins
          .filter((bin) => bin.x0 <= x1 && bin.x1 >= x0)
          .flatMap((bin) => bin.values || [])
          .filter((v) => v >= x0 && v <= x1);

        // Update visualization
        this.render();

        // Notify table with selected values
        if (this.table && selectedValues.length > 0) {
          this.table.handleHistogramSelection([{ min: x0, max: x1 }], this.columnName);
        }
      });

    brushG.call(brush);

    // Style brush selection
    brushG
      .selectAll(".selection")
      .attr("fill", this.options.colors[0])
      .attr("fill-opacity", 0.15)
      .attr("stroke", this.options.colors[0])
      .attr("stroke-width", 1);

    // Style brush handles
    brushG
      .selectAll(".handle")
      .attr("fill", this.options.colors[0])
      .attr("stroke", "none")
      .attr("width", 3)  // Make handles thinner
      .attr("cursor", "ew-resize");  // Use horizontal resize cursor

    // Remove overlay to prevent interference with tooltips
    brushG.selectAll(".overlay").remove();
  }

  setupOrdinalInteraction(g, x, y) {
    // Create a new group for the overlay bars
    const overlayGroup = g.append("g").attr("class", "overlay-group");

    overlayGroup
      .selectAll(".bar-overlay")
      .data(this.originalData.bins)
      .join("rect")
      .attr("class", "bar-overlay")
      .attr("x", (d) => x(d.key))
      .attr("width", x.bandwidth())
      .attr("y", 0)
      .attr("height", this.getChartDimensions().height)
      .attr("fill", "transparent")
      .attr("cursor", "pointer")
      .on("mouseover", (event, d) => {
        // Highlight the bar being hovered
        d3.select(event.currentTarget.parentNode.parentNode)
          .selectAll(`.bar-${this.options.colors[1].replace("#", "")}`)
          .filter((b) => b.key === d.key)
          .attr("opacity", this.baseOpacity + 0.3);
      })
      .on("mouseout", (event) => {
        // Reset highlight on mouseout
        d3.select(event.currentTarget.parentNode.parentNode)
          .selectAll(`.bar-${this.options.colors[1].replace("#", "")}`)
          .attr("opacity", this.baseOpacity);
      })
      .on("click", (event, d) => {
        event.stopPropagation();

        // Handle selection based on ctrl/meta key
        if (event.ctrlKey || event.metaKey) {
          if (this.selected.has(d.key)) {
            this.selected.delete(d.key);
          } else {
            this.selected.add(d.key);
          }
        } else {
          // Single selection - clear previous selection
          this.selected = new Set([d.key]);
        }

        // Update visualization
        this.render();

        // Notify table with selection using the bin's actual values
        if (this.table) {
          const selectedValues = d.values || [d.key];
          this.table.handleHistogramSelection(selectedValues, this.columnName);
        }
      });
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

  updateSelection(selectedValues, shouldNotifyTable = false) {
    // Clear existing selection if needed
    if (!this.ctrlDown) {
      this.selected.clear();
    }

    if (this.originalData.type === "continuous") {
      // For continuous data, find bins that contain the selected values
      this.originalData.bins.forEach((bin) => {
        if (bin.values.some((v) => selectedValues.has(v))) {
          this.selected.add(bin.x0);
        }
      });
    } else {
      // For ordinal data, find bins that match the selected values
      this.originalData.bins.forEach((bin) => {
        if (bin.values.some((v) => selectedValues.has(v))) {
          this.selected.add(bin.key);
        }
      });
    }

    // Update visual state
    this.render();

    // Notify table if requested
    if (shouldNotifyTable && this.table) {
      this.table.updateSelection(selectedValues, this.columnName);
    }
  }

  resetSelection() {
    this.selected.clear();
    // Don't reset highlighted data here - it should stay as all data by default
    // this.highlightedData = null;
    this.render();
  }

  createBinsFromData(data) {
    if (!data || data.length === 0) {
      return [];
    }

    // If we have access to DuckDB binning, prefer that for accuracy
    if (this.table?.binningService && this.columnName) {
      // This bin creation will be handled by updateData using DuckDB
      return this.originalData.bins;
    }

    // Otherwise fall back to D3 binning
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

  async updateData(newData, options = {}) {
    try {
      // Reset selection when data changes
      this.selected.clear();

      // Check if we have direct access to DuckDB processing
      if (this.table && this.table.duckDBProcessor && this.columnName) {
        // For table-connected histograms, we should use DuckDB to get full dataset statistics
        // rather than just the visible rows
        if (options.type === "unique") {
          // For unique columns, get accurate count of unique values directly from DuckDB
          const uniqueQuery = `
            SELECT COUNT(DISTINCT "${this.columnName}") as unique_count 
            FROM ${this.table.duckDBTableName}
          `;
          const uniqueResult = await this.table.duckDBProcessor.query(
            uniqueQuery
          );
          this.options.uniqueCount = uniqueResult[0].unique_count;
        } else if (
          this.dataType === "continuous" ||
          this.dataType === "ordinal"
        ) {
          // Get fresh binning for the entire dataset using DuckDB
          const bins = await this.table.binningService.getBinningForColumn(
            this.columnName,
            this.dataType,
            this.options.maxOrdinalBins || 20
          );

          // Update the bins with the latest full dataset statistics
          this.originalData.bins = bins;
        }

        // Set visible data as highlighted data
        this.highlightedData = newData;
      } else {
        // Regular bin creation for disconnected histograms
        await this.createBins(newData, {
          type: this.originalData.type,
          thresholds: options.thresholds || this.options.thresholds,
          binInfo: options.binInfo,
        });

        // Set all data as highlighted by default
        this.highlightedData = newData;
      }

      this.render();
    } catch (error) {
      console.error("Failed to update histogram data:", error);
      this.showError("Failed to update data");
    }
  }

  showTooltip(event, g, x, y) {
    // Remove any existing tooltip
    this.hideTooltip();

    const { width, height } = this.getChartDimensions();

    // Get mouse position relative to container
    const [xPos, yPos] = d3.pointer(event, g.node());

    if (xPos < 0 || xPos > width || yPos < 0 || yPos > height) return;

    // Find the bin at this position
    let hoverBin;

    if (this.originalData.type === "continuous") {
      const dataX = x.invert(xPos);
      hoverBin = this.originalData.bins.find(
        (bin) => dataX >= bin.x0 && dataX <= bin.x1
      );
    } else {
      const bandWidth = width / this.originalData.bins.length;
      const binIndex = Math.floor(xPos / bandWidth);
      hoverBin = this.originalData.bins[binIndex];
    }

    if (!hoverBin) return;

    // Create tooltip
    const tooltip = d3
      .select(this.container)
      .append("div")
      .attr("class", "histogram-tooltip")
      .style("position", "absolute")
      .style("background", "white")
      .style("padding", "5px")
      .style("border", "1px solid #ddd")
      .style("border-radius", "3px")
      .style("font-size", "9px")
      .style("box-shadow", "0 1px 3px rgba(0,0,0,0.1)")
      .style("pointer-events", "none")
      .style("z-index", "10");

    // Position tooltip
    let left = event.offsetX + 5;
    let top = event.offsetY - 28;

    // Keep tooltip within container bounds
    if (left + 60 > this.options.width) left = this.options.width - 65;
    if (top < 5) top = event.offsetY + 15;

    tooltip.style("left", `${left}px`).style("top", `${top}px`);

    // Tooltip content
    if (this.originalData.type === "continuous") {
      tooltip.html(
        `Range: ${hoverBin.x0.toFixed(1)} - ${hoverBin.x1.toFixed(1)}<br>` +
          `Count: ${hoverBin.length}`
      );
    } else {
      tooltip.html(`Value: ${hoverBin.key}<br>` + `Count: ${hoverBin.length}`);
    }
  }

  hideTooltip() {
    d3.select(this.container).select(".histogram-tooltip").remove();
  }

  showBrushTooltip(event, start, end) {
    this.hideBrushTooltip();

    if (typeof start !== "number" || typeof end !== "number") return;

    // Create tooltip
    const tooltip = d3
      .select(this.container)
      .append("div")
      .attr("class", "brush-tooltip")
      .style("position", "absolute")
      .style("background", "rgba(0,0,0,0.7)")
      .style("color", "white")
      .style("padding", "3px 6px")
      .style("border-radius", "3px")
      .style("font-size", "9px")
      .style("pointer-events", "none")
      .style("z-index", "10");

    // Get correct position based on event type
    let left, top;
    if (event.sourceEvent) {
      // During an interactive brush
      left = event.sourceEvent.offsetX;
      top = event.sourceEvent.offsetY - 25;
    } else {
      // For programmatic events
      const midpoint =
        this.options.margin.left +
        (this.options.width -
          this.options.margin.left -
          this.options.margin.right) /
          2;
      left = midpoint;
      top = this.options.height / 4;
    }

    tooltip.style("left", `${left}px`).style("top", `${top}px`);

    // Format numbers appropriately
    const formatNumber = d3.format(".2~f");
    const startVal = formatNumber(start);
    const endVal = formatNumber(end);

    // Tooltip content
    tooltip.html(`Range: ${startVal} - ${endVal}`);
  }

  hideBrushTooltip() {
    d3.select(this.container).select(".brush-tooltip").remove();
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
