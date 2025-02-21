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
      console.log('Initializing histogram:', {
        columnName: this.columnName,
        dataLength: data?.length,
        type: options?.type,
        hasBins: Boolean(options?.bins)
      });

      await this.createBins(data, {
        type: options?.type || 'ordinal',
        thresholds: options?.thresholds,
        preBinnedData: options?.bins  // Use pre-binned data if available
      });
      
      // Ensure we have valid data before rendering
      if (this.originalData.bins && this.originalData.bins.length > 0) {
        this.render();
      } else {
        this.showError('No data to display');
      }
    } catch (error) {
      console.error('Failed to initialize histogram:', error);
      this.showError();
    }
  }

  async createBins(data, options) {
    if (!data || data.length === 0) {
      return;
    }

    const values = data.filter(d => d != null);
    
    if (options.type === "continuous") {
      const extent = d3.extent(values);
      const binGenerator = d3.bin()
        .domain(extent)
        .thresholds(options.thresholds || d3.thresholdSturges);
      
      const bins = binGenerator(values);
      this.originalData.bins = bins.map(bin => ({
        x0: bin.x0,
        x1: bin.x1,
        length: bin.length,
        values: Array.from(bin),  // Store original values
        key: bin.x0  // Store x0 as key for consistent reference
      }));
      
      this.originalData.type = "continuous";
    } else {
      // For ordinal data, preserve exact values
      const uniqueValues = new Map();
      values.forEach(value => {
        const key = String(value);
        if (!uniqueValues.has(key)) {
          uniqueValues.set(key, {
            originalValue: value,
            count: 0,
            values: []
          });
        }
        const entry = uniqueValues.get(key);
        entry.count++;
        entry.values.push(value);
      });

      this.originalData.bins = Array.from(uniqueValues.values()).map(entry => ({
        key: entry.originalValue,  // Use original value directly as key
        length: entry.count,
        values: entry.values,
        value: entry.originalValue  // Keep original value for reference
      }));

      this.originalData.type = "ordinal";
    }

    // Initially, current data is the same as original
    this.currentData = structuredClone(this.originalData);
  }

  render() {
    this.container.innerHTML = "";
    
    const svg = d3.select(this.container)
      .append("svg")
      .attr("width", this.options.width)
      .attr("height", this.options.height);

    const { width, height } = this.getChartDimensions();
    const g = svg.append("g")
      .attr("transform", `translate(${this.options.margin.left},${this.options.margin.top})`);

    // Create scales
    const x = this.createXScale(width);
    const y = this.createYScale(height);

    // Draw background histogram
    this.drawHistogramBars(g, this.originalData.bins, x, y, "#e0e0e0", this.baseOpacity);

    // Draw highlighted bars if we have a selection
    if (this.selected.size > 0) {
      let selectedBins;
      if (this.originalData.type === "continuous") {
        const selectedRange = Array.from(this.selected)[0];
        selectedBins = this.originalData.bins.filter(bin => 
          bin.x0 >= selectedRange.min && bin.x1 <= selectedRange.max
        );
      } else {
        selectedBins = this.originalData.bins.filter(bin => 
          this.selected.has(bin.key)
        );
      }
      
      // Draw selected bins in primary color
      this.drawHistogramBars(g, selectedBins, x, y, this.options.colors[0], 1);
    }

    // Add interaction handlers last
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
      const extent = [
        d3.min(this.originalData.bins, d => d.x0),
        d3.max(this.originalData.bins, d => d.x1)
      ];
      return d3.scaleLinear()
        .domain(extent)
        .range([0, width])
        .nice();  // Nice the scale for better brush interaction
    } else {
      return d3.scaleBand()
        .domain(this.originalData.bins.map(d => d.key))
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
        .attr("x", d => x(d.x0))
        .attr("width", d => Math.max(0, x(d.x1) - x(d.x0) - 1))
        .attr("y", d => y(d.length))
        .attr("height", d => height - y(d.length))
        .attr("fill", d => {
          if (this.selected.size > 0) {
            const selectedRange = Array.from(this.selected)[0];
            return selectedRange && d.x0 >= selectedRange.min && d.x1 <= selectedRange.max ? 
              this.options.colors[0] : color;
          }
          return color;
        })
        .attr("opacity", opacity)
        .attr("rx", 2);
    } else {
      g.selectAll(`.bar-${color}`)
        .data(bins)
        .join("rect")
        .attr("class", `bar-${color}`)
        .attr("x", d => x(d.key))
        .attr("width", x.bandwidth())
        .attr("y", d => y(d.length))
        .attr("height", d => height - y(d.length))
        .attr("fill", d => this.selected.has(d.key) ? this.options.colors[0] : color)
        .attr("opacity", opacity)
        .attr("rx", 2);
    }
  }

  setupBrush(svg, width, height) {
    // Clear any existing brush
    svg.selectAll(".brush").remove();
    
    const brush = d3.brushX()
      .extent([[0, 0], [width, height]])
      .on("end", (event) => {
        if (!event.selection) {
          this.resetSelection();
          return;
        }

        // Transform coordinates back to data space
        const scale = this.createXScale(width);
        const [x0, x1] = event.selection.map(x => scale.invert(x));

        console.log('Brush selection:', {
          domain: [x0, x1],
          columnName: this.columnName
        });

        // Store selected range for visualization
        this.selected = new Set([{min: x0, max: x1}]);

        // Get all values from bins that overlap with the brush range
        const selectedValues = new Set();
        this.originalData.bins.forEach(bin => {
          // Check if bin overlaps with brush selection
          if (bin.x0 <= x1 && bin.x1 >= x0) {
            // Only include values that actually fall within the brush range
            if (Array.isArray(bin.values)) {
              bin.values.forEach(v => {
                // Convert to number and check if in range
                const numValue = Number(v);
                if (!isNaN(numValue) && numValue >= x0 && numValue <= x1) {
                  selectedValues.add(numValue);
                }
              });
            }
          }
        });

        console.log('Selected values:', {
          count: selectedValues.size,
          sampleValues: Array.from(selectedValues).slice(0, 5)
        });

        // Update visualization
        this.render();

        // Notify table with actual numeric values
        if (this.table && selectedValues.size > 0) {
          this.table.handleHistogramSelection(selectedValues, this.columnName);
        }
      });

    // Add brush
    const brushG = svg.append("g")
      .attr("class", "brush")
      .attr("transform", `translate(${this.options.margin.left},${this.options.margin.top})`);

    // Initialize brush
    brushG.call(brush);

    // Add brush overlay to capture mouse events
    brushG.selectAll(".overlay")
      .style("pointer-events", "all")
      .style("cursor", "crosshair");
  }

  setupOrdinalInteraction(g, x, y) {
    g.selectAll(".bar-overlay")
      .data(this.originalData.bins)
      .join("rect")
      .attr("class", "bar-overlay")
      .attr("x", d => x(d.key))
      .attr("width", x.bandwidth())
      .attr("y", 0)
      .attr("height", this.getChartDimensions().height)
      .attr("fill", "transparent")
      .attr("cursor", "pointer")
      .on("click", (event, d) => {
        event.stopPropagation();

        // Use the exact value from the bin
        const selectedValue = d.key;
        
        // Update visualization state
        const newSelected = new Set(this.selected);
        if (event.ctrlKey || event.metaKey) {
          if (newSelected.has(selectedValue)) {
            newSelected.delete(selectedValue);
          } else {
            newSelected.add(selectedValue);
          }
        } else {
          newSelected.clear();
          newSelected.add(selectedValue);
        }
        
        this.selected = newSelected;

        // Update visualization first
        this.render();

        // Then notify table with exact value
        if (this.table && this.table.handleHistogramSelection) {
          const selectedValues = new Set([selectedValue]);
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
      this.originalData.bins.forEach(bin => {
        if (bin.values.some(v => selectedValues.has(v))) {
          this.selected.add(bin.x0);
        }
      });
    } else {
      // For ordinal data, find bins that match the selected values
      this.originalData.bins.forEach(bin => {
        if (bin.values.some(v => selectedValues.has(v))) {
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
    this.highlightedData = null;
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
