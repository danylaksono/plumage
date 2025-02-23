import * as d3 from "npm:d3";
import { BaseVisualization } from "../_base/base.js";

export class ViolinPlot extends BaseVisualization {
  constructor(config) {
    const defaults = {
      bandwidth: 0.4,
      kernelType: "gaussian",
      showMedian: true,
      showQuartiles: true,
      maxWidth: 50,
      padding: 0.1,
      margin: { top: 20, right: 20, bottom: 30, left: 50 },
      showTooltip: false, // added default for tooltip
      showLabelsBelow: false, // added default for labels
      colors: {
        main: "#69b3a2",
        selected: "#2c5282", // darker blue
        hover: "#63b3ed", // lighter blue
        median: "#ffffff",
        quartiles: "#ffffff"
      },
      animation: {
        duration: 800,
        ease: d3.easeCubic
      },
      tooltip: {
        offset: { x: 10, y: 10 },
        format: {
          value: (d) => d.toFixed(2),
          density: (d) => d.toFixed(3)
        }
      },
      brush: {
        enabled: true,
        fillOpacity: 0.3,
        debounceTime: 250 // ms
      }
    };

    super({ ...defaults, ...config });

    this.stats = null;
    this.data = [];
    this.violinData = [];

    // Calculate effective dimensions
    this.effectiveWidth =
      this.config.width - this.config.margin.left - this.config.margin.right;
    this.effectiveHeight =
      this.config.height - this.config.margin.top - this.config.margin.bottom;

    console.log("[ViolinPlot] Initialized dimensions:", {
      config: {
        width: this.config.width,
        height: this.config.height,
        margin: this.config.margin,
        maxWidth: this.config.maxWidth,
      },
      effective: {
        width: this.effectiveWidth,
        height: this.effectiveHeight,
      },
    });
  }

  async initialize() {
    if (!this.initialized) {
      await super.initialize();

      if (!this.g) {
        console.error("[ViolinPlot] No group element provided for violin plot");
        return this;
      }

      // Create tooltip if enabled
      if (this.config.showTooltip && !this.tooltip) {
        this.tooltip = d3.select("body")
          .append("div")
          .attr("class", "violin-tooltip")
          .style("opacity", 0)
          .style("position", "absolute")
          .style("background", "rgba(255, 255, 255, 0.95)")
          .style("padding", "8px 12px")
          .style("border-radius", "4px")
          .style("box-shadow", "0 2px 4px rgba(0,0,0,0.1)")
          .style("pointer-events", "none")
          .style("font-size", "12px")
          .style("z-index", "1000");
      }

      // Create label group if enabled
      if (this.config.showLabelsBelow && !this.labelGroup) {
        this.labelGroup = this.g
          .append("g")
          .attr("class", "violin-labels")
          .attr("transform", `translate(0,${this.config.height + 25})`);
      }

      this.createScales();
      this.addResizeHandler();
    }
    return this;
  }

  createScales() {
    // Create scales without margins initially
    this.xScale = d3
      .scaleLinear()
      .range([0, this.config.maxWidth])
      .domain([0, 1]);

    this.yScale = d3
      .scaleLinear()
      .range([this.effectiveHeight, 0])
      .domain([0, 1]);

    console.log("[ViolinPlot] Scales created:", {
      effectiveWidth: this.effectiveWidth,
      effectiveHeight: this.effectiveHeight,
      maxWidth: this.config.maxWidth,
    });
  }

  createViolin() {
    console.log("[ViolinPlot] Creating violin area generator");
    // Create and store the area generator
    this.violinArea = d3
      .area()
      .x0((d) => -this.xScale(d[1]))
      .x1((d) => this.xScale(d[1]))
      .y((d) => this.yScale(d[0]))
      .curve(d3.curveCatmullRom);

    console.log("[ViolinPlot] Area generator created with scales:", {
      xDomain: this.xScale.domain(),
      yDomain: this.yScale.domain(),
    });
  }

  setupBrush() {
    const violinWidth = this.config.maxWidth;
    const centerX = this.effectiveWidth / 2;

    // Remove existing brush if any
    if (this.brushGroup) {
        this.brushGroup.remove();
    }

    this.brush = d3.brushY()
        .extent([
            [-violinWidth, 0],
            [violinWidth, this.effectiveHeight]
        ])
        .on("start brush", (event) => this.handleBrush(event))
        .on("end", (event) => this.handleBrushEnd(event));

    // Create new brush group
    this.brushGroup = this.g
        .append("g")
        .attr("class", "brush")
        .attr("transform", `translate(${centerX},0)`);

    // Initialize the brush
    this.brushGroup.call(this.brush);

    // Style the brush selection area
    this.brushGroup.selectAll(".selection")
        .style("fill", this.config.colors.main)
        .style("fill-opacity", this.config.brush.fillOpacity)
        .style("stroke", this.config.colors.selected)
        .style("stroke-width", 1.5);

    console.log("[ViolinPlot] Brush setup complete:", {
        violinWidth,
        centerX,
        height: this.effectiveHeight
    });
  }

  // Utility function for debouncing
  debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
      const later = () => {
        clearTimeout(timeout);
        func(...args);
      };
      clearTimeout(timeout);
      timeout = setTimeout(later, wait);
    };
  }

  handleBrush(event) {
    if (!event.selection) {
      this.g.selectAll(".violin")
        .style("opacity", 1)
        .style("fill", this.config.colors.main);
      return;
    }

    const [y0, y1] = event.selection.map(this.yScale.invert);
    
    // Visual feedback during brushing
    this.g.selectAll(".violin")
      .style("opacity", 0.4)
      .style("fill", this.config.colors.main);

    // Highlight the brushed area
    this.g.selectAll(".violin")
      .filter(d => {
        const y = this.yScale(d[0]);
        return y >= event.selection[0] && y <= event.selection[1];
      })
      .style("opacity", 1)
      .style("fill", this.config.colors.selected);
  }

  handleBrushEnd(event) {
    if (!event.selection) {
        this.brushedRange = null;
        this.selectedData = new Set();
        this.dispatch.call("selectionChanged", this, []);
        
        // Reset visualization
        this.g.selectAll(".violin")
            .style("opacity", 1)
            .style("fill", this.config.colors.main);
        return;
    }

    const [y0, y1] = event.selection.map(this.yScale.invert);
    
    // Ensure min and max values are in correct order
    const minValue = Math.min(y0, y1);
    const maxValue = Math.max(y0, y1);

    // Query data within the brushed range
    const query = `
        SELECT *
        FROM ${this.tableName}
        WHERE "${this.config.column}" >= ${minValue}
        AND "${this.config.column}" <= ${maxValue}
    `;

    console.log("[ViolinPlot] Brush query:", {
        minValue,
        maxValue,
        query
    });

    this.query(query).then((selectedData) => {
        if (selectedData.length === 0) {
            console.warn("[ViolinPlot] No data found in selected range");
            return;
        }

        this.selectedData = new Set(selectedData.map(d => d[this.config.column]));
        this.dispatch.call("selectionChanged", this, Array.from(this.selectedData));

        // Update visual feedback
        this.g.selectAll(".violin")
            .style("opacity", 0.4)
            .style("fill", this.config.colors.main);

        // Highlight the selected portion
        const [brushY0, brushY1] = event.selection;
        this.g.selectAll(".violin")
            .filter(d => {
                const y = this.yScale(d[0]);
                return y >= brushY0 && y <= brushY1;
            })
            .style("opacity", 1)
            .style("fill", this.config.colors.selected);

    }).catch(error => {
        console.error("[ViolinPlot] Error querying brushed data:", error);
    });
}

  async update(data = null) {
    if (!this.initialized) {
      await this.initialize();
    }

    try {
      if (data) {
        this.data = data
          .map((d) => {
            const value = d[this.config.column];
            return value != null && !isNaN(value) ? +value : null;
          })
          .filter((d) => d !== null);
      } else if (this.dataProcessor) {
        const query = `SELECT "${this.config.column}" as value 
                      FROM ${this.tableName} 
                      WHERE "${this.config.column}" IS NOT NULL 
                      AND CAST("${this.config.column}" AS DOUBLE) IS NOT NULL`;
        const result = await this.query(query);
        this.data = result.map((d) => +d.value);
      }

      if (!this.data || this.data.length === 0) {
        console.warn(
          "[ViolinPlot] No valid data for column:",
          this.config.column
        );
        return this;
      }

      // Calculate statistics first
      const sortedData = [...this.data].sort((a, b) => a - b);
      this.stats = {
        median: d3.median(sortedData),
        q1: d3.quantile(sortedData, 0.25),
        q3: d3.quantile(sortedData, 0.75),
        min: d3.min(sortedData),
        max: d3.max(sortedData),
        std: d3.deviation(sortedData),
      };

      // Create scales
      const range = this.stats.max - this.stats.min;
      const padding = range * 0.05;
      this.yScale = d3
        .scaleLinear()
        .domain([this.stats.min - padding, this.stats.max + padding])
        .range([this.effectiveHeight, 0]);

      this.xScale = d3
        .scaleLinear()
        .domain([0, 1])
        .range([0, this.config.maxWidth]);

      // Compute violin plot data
      const bandwidth = this.stats.std * Math.pow(this.data.length, -0.2);
      const kernel = this.kernelEpanechnikov(bandwidth);
      const kdePoints = d3.range(
        this.stats.min - padding,
        this.stats.max + padding,
        range / 100
      );

      const kde = this.kernelDensityEstimator(kernel, kdePoints);
      this.violinData = kde(sortedData);

      // Draw the violin plot
      this.draw();
    } catch (error) {
      console.error("[ViolinPlot] Error in update:", error);
    }

    return this;
  }

  updateScales() {
    if (!this.stats) return;

    const range = this.stats.max - this.stats.min;
    const padding = range * 0.05;
    this.yScale.domain([this.stats.min - padding, this.stats.max + padding]);

    console.log(`[ViolinPlot ${this.config.column}] Scale updated:`, {
      yDomain: this.yScale.domain(),
      range,
      padding,
    });
  }

  computeViolin() {
    if (!Array.isArray(this.data) || this.data.length === 0) return;

    try {
      const sortedData = [...this.data].sort((a, b) => a - b);

      // Calculate statistics
      this.stats = {
        median: d3.median(sortedData),
        q1: d3.quantile(sortedData, 0.25),
        q3: d3.quantile(sortedData, 0.75),
        min: d3.min(sortedData),
        max: d3.max(sortedData),
        std: d3.deviation(sortedData),
      };

      // Generate evenly spaced points for KDE
      const range = this.stats.max - this.stats.min;
      const padding = range * 0.1;
      const n = sortedData.length;

      // Use Scott's rule for bandwidth selection
      const bandwidth = this.stats.std * Math.pow(n, -1 / 5);

      // Create points for density estimation
      const numPoints = 50;
      const step = (range + 2 * padding) / (numPoints - 1);
      const points = Array.from(
        { length: numPoints },
        (_, i) => this.stats.min - padding + i * step
      );

      // Compute kernel density estimation
      const densities = points.map((x) => {
        const values = sortedData.map((v) => {
          const z = (x - v) / bandwidth;
          // Use Gaussian kernel
          return Math.exp(-0.5 * z * z) / Math.sqrt(2 * Math.PI);
        });
        return [x, d3.mean(values) / bandwidth || 0];
      });

      // Normalize densities to [0, 1]
      const maxDensity = d3.max(densities, (d) => d[1]);
      this.violinData = densities.map((d) => [d[0], d[1] / maxDensity]);

      console.log("[ViolinPlot] Violin data computed:", {
        points: this.violinData.length,
        range: [this.stats.min, this.stats.max],
        bandwidth,
        maxDensity,
      });
    } catch (error) {
      console.error("[ViolinPlot] Error computing violin:", error);
      this.violinData = [];
    }
  }

  draw() {
    this.g.selectAll("*").remove();

    // Remove gradient definition
    // this.g.select("defs").remove();
    // const defs = this.g.append("defs");
    // const gradient = defs
    //   .append("linearGradient")
    //   .attr("id", "violin-gradient")
    //   .attr("x1", "0%")
    //   .attr("y1", "0%")
    //   .attr("x2", "100%")
    //   .attr("y2", "0%");
    // gradient
    //   .append("stop")
    //   .attr("offset", "0%")
    //   .attr("stop-color", this.config.colors[0]);
    // gradient
    //   .append("stop")
    //   .attr("offset", "100%")
    //   .attr("stop-color", this.config.colors[1]);

    if (!this.violinData || this.violinData.length === 0) return;

    // Update y scale with data range
    const yRange = this.stats.max - this.stats.min;
    const yPadding = yRange * 0.1;
    this.yScale.domain([this.stats.min - yPadding, this.stats.max + yPadding]);

    // Calculate violin dimensions
    const violinWidth = this.config.maxWidth;
    const centerX = this.effectiveWidth / 2;

    const violinG = this.g
      .append("g")
      .attr("transform", `translate(${centerX},0)`);

    // Create violin area
    const violinArea = d3
      .area()
      .x0((d) => -violinWidth * d[1])
      .x1((d) => violinWidth * d[1])
      .y((d) => this.yScale(d[0]))
      .curve(d3.curveCatmullRom);

    // Draw violin shape with single color fill, stroke and transition
    violinG
      .append("path")
      .datum(this.violinData)
      .attr("class", "violin")
      .attr("d", violinArea)
      .attr("fill", this.config.colors[0]) // Use a single color
      .attr("opacity", 0.9)
      .attr("stroke", "#333")
      .attr("stroke-width", 1)
      .transition()
      .duration(800)
      .ease(d3.easeCubic);

    // Enable drag interactivity with brush if configured
    if (this.config.selectionMode === "drag" && this.config.brush.enabled) {
      // Remove any existing brush
      if (this.brushGroup) {
        this.brushGroup.remove();
      }
      this.setupBrush();
    }

    // Draw y-axis if enabled
    if (this.config.showAxis) {
      this.drawAxes();
    }

    // Add overlay for interactivity if tooltip or labels are enabled
    if (this.config.showTooltip || this.config.showLabelsBelow) {
      violinG
        .append("rect")
        .attr("class", "overlay")
        .attr("x", -violinWidth)
        .attr("y", 0)
        .attr("width", violinWidth * 2)
        .attr("height", this.effectiveHeight)
        .style("fill", "transparent")
        .on("mousemove", (event) =>
          this.handleMouseMove(event, violinG, violinWidth)
        )
        .on("mouseout", () => this.handleMouseOut());
    }

    // Draw median line
    if (this.config.showMedian) {
      violinG
        .append("line")
        .attr("class", "median")
        .attr("x1", -violinWidth * 0.9)
        .attr("x2", violinWidth * 0.9)
        .attr("y1", this.yScale(this.stats.median))
        .attr("y2", this.yScale(this.stats.median))
        .attr("stroke", "white")
        .attr("stroke-width", 2);
    }

    // Draw quartiles
    if (this.config.showQuartiles) {
      [this.stats.q1, this.stats.q3].forEach((value) => {
        violinG
          .append("line")
          .attr("class", "quartile")
          .attr("x1", -violinWidth * 0.7)
          .attr("x2", violinWidth * 0.7)
          .attr("y1", this.yScale(value))
          .attr("y2", this.yScale(value))
          .attr("stroke", "white")
          .attr("stroke-width", 1)
          .attr("stroke-dasharray", "3,3");
      });
    }

    // Add title
    if (this.config.title) {
      this.g
        .append("text")
        .attr("class", "violin-title")
        .attr("x", centerX)
        .attr("y", -10) // Adjust position as needed
        .attr("text-anchor", "middle")
        .style("font-size", "14px")
        .text(this.config.title);
    }

    console.log("[ViolinPlot] Draw complete with dimensions:", {
      width: this.effectiveWidth,
      height: this.effectiveHeight,
      violinWidth,
      centerX,
    });
  }

  drawAxes() {
    const yAxis = d3.axisLeft(this.yScale);

    this.g
      .append("g")
      .attr("class", "y-axis")
      .attr("transform", `translate(-10,0)`) // Adjust the x-translation to move the axis closer
      .call(yAxis);
  }

  // Kernel density estimation functions
  kernelDensityEstimator(kernel, X) {
    return function (V) {
      if (!V || V.length === 0) {
        console.warn("[ViolinPlot] Empty data array for KDE");
        return [];
      }

      // Debug data input
      console.log("[ViolinPlot] KDE input data:", {
        values: V.slice(0, 5),
        domain: [Math.min(...V), Math.max(...V)],
        samplePoints: X.slice(0, 5),
      });

      // First calculate raw density values with debug checks
      const densities = X.map((x) => {
        const values = V.map((v) => {
          const k = kernel(x - v);
          if (isNaN(k)) {
            console.warn("[ViolinPlot] Invalid kernel value for:", { x, v });
          }
          return k;
        });
        const mean = d3.mean(values) || 0;
        return [x, mean];
      }).filter((d) => !isNaN(d[0]) && !isNaN(d[1]));

      // Debug density output
      console.log("[ViolinPlot] Raw density:", {
        points: densities.length,
        sample: densities.slice(0, 5),
        range: [d3.min(densities, (d) => d[1]), d3.max(densities, (d) => d[1])],
      });

      // Scale the densities to [0, 1]
      const maxDensity = d3.max(densities, (d) => d[1]) || 1;
      const scaledDensities = densities.map((d) => [d[0], d[1] / maxDensity]);

      // Debug final output
      console.log("[ViolinPlot] Scaled density:", {
        points: scaledDensities.length,
        sample: scaledDensities.slice(0, 5),
      });

      return scaledDensities;
    };
  }

  kernelEpanechnikov(bandwidth) {
    if (bandwidth <= 0) {
      console.warn("[ViolinPlot] Invalid bandwidth, using default");
      bandwidth = 1;
    }

    return (x) => {
      const scaled = x / bandwidth;
      return Math.abs(scaled) <= 1
        ? (0.75 * (1 - scaled * scaled)) / bandwidth
        : 0;
    };
  }

  violinArea(d) {
    if (!d || d.length === 0) {
      console.error("[ViolinPlot] Invalid data for violin area");
      return "";
    }
    console.log(
      "[ViolinPlot] Generating violin area path for",
      d.length,
      "points"
    );
    return d3
      .area()
      .x0((d) => -this.xScale(d[1]))
      .x1((d) => this.xScale(d[1]))
      .y((d) => this.yScale(d[0]))
      .curve(d3.curveCatmullRom);
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
    if (this.resizeHandler) {
      window.removeEventListener('resize', this.resizeHandler);
    }
    await super.destroy();
  }

  // New interactivity methods
  handleMouseMove(event, group, violinWidth) {
    const [mx, my] = d3.pointer(event, group.node());
    const yValue = this.yScale.invert(my);
    
    // Find closest point with improved precision
    const closest = this.violinData.reduce((prev, curr) => {
      const prevDiff = Math.abs(prev[0] - yValue);
      const currDiff = Math.abs(curr[0] - yValue);
      return currDiff < prevDiff ? curr : prev;
    });

    // Highlight the violin plot section being hovered
    group.select(".violin")
      .style("fill", this.config.colors.hover)
      .style("transition", "fill 0.2s ease");

    if (this.config.showTooltip && this.tooltip) {
      const { offset } = this.config.tooltip;
      const tooltipWidth = this.tooltip.node().offsetWidth;
      const tooltipHeight = this.tooltip.node().offsetHeight;
      
      // Calculate position with smart positioning
      const pos = this.calculateTooltipPosition(
        event.pageX, event.pageY,
        tooltipWidth, tooltipHeight,
        offset
      );

      const actualValue = this.findClosestDataPoint(closest[0]);
      
      this.tooltip
        .style("opacity", 1)
        .html(this.formatTooltipContent(closest, actualValue))
        .style("left", `${pos.left}px`)
        .style("top", `${pos.top}px`);
    }
  }

  calculateTooltipPosition(x, y, width, height, offset) {
    const windowWidth = window.innerWidth;
    const windowHeight = window.innerHeight;
    
    let left = x + offset.x;
    let top = y + offset.y;

    // Smart positioning to avoid window bounds
    if (left + width > windowWidth) {
      left = x - width - offset.x;
    }
    if (top + height > windowHeight) {
      top = y - height - offset.y;
    }

    return { left, top };
  }

  formatTooltipContent(point, actualValue) {
    const { format } = this.config.tooltip;
    return `
      <div style="font-weight: bold; margin-bottom: 4px">
        Value: ${format.value(actualValue || point[0])}
      </div>
      <div>
        Density: ${format.density(point[1])}
      </div>
    `;
  }

  handleMouseOut() {
    // Hide tooltip if enabled
    if (this.config.showTooltip && this.tooltip) {
      this.tooltip.style("opacity", 0);
    }
    // Clear label group if enabled
    if (this.config.showLabelsBelow && this.labelGroup) {
      this.labelGroup.selectAll("text").remove();
    }
  }

  addResizeHandler() {
    if (!this.resizeHandler) {
      this.resizeHandler = this.debounce(() => {
        this.updateDimensions();
        this.draw();
      }, 250);
      
      window.addEventListener('resize', this.resizeHandler);
    }
  }

  updateDimensions() {
    const container = d3.select(this.container).node();
    if (!container) return;

    const bbox = container.getBoundingClientRect();
    this.config.width = bbox.width;
    this.config.height = bbox.height;

    this.effectiveWidth = this.config.width - this.config.margin.left - this.config.margin.right;
    this.effectiveHeight = this.config.height - this.config.margin.top - this.config.margin.bottom;

    // Update scales
    this.updateScales();
  }

  findClosestDataPoint(value) {
    if (!this.data || this.data.length === 0) return null;
    
    return this.data.reduce((closest, current) => {
      const currentDiff = Math.abs(current - value);
      const closestDiff = Math.abs(closest - value);
      return currentDiff < closestDiff ? current : closest;
    });
  }
}
