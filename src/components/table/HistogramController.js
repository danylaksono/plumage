import * as d3 from "npm:d3";

function HistogramController(data, binrules) {
  let controller = this;
  let div = document.createElement("div");

  this.bins = [];
  this.brush = null;
  this.isBrushing = false;

  this.updateData = (d) => this.setData(d);

  // Reset the selection state of the histogram
  this.resetSelection = () => {
    this.bins.forEach((bin) => {
      bin.selected = false;
    });

    this.svg.selectAll(".bar rect:nth-child(1)").attr("fill", "steelblue");
  };

  // console.log("------------------binrules outside setData: ", binrules);

  this.setData = function (newData) {
    try {
      // Validate input data and initialize with defaults if needed
      this.data = {
        type: newData?.type || "ordinal",
        bins: [],
      };

      // Clear the div and reset state
      div.innerHTML = "";
      this.bins = [];

      // Setup SVG dimensions
      const svgWidth = 120;
      const svgHeight = 60;
      const margin = { top: 5, right: 8, bottom: 10, left: 8 };
      const width = svgWidth - margin.left - margin.right;
      const height = svgHeight - margin.top - margin.bottom;

      // Create SVG with background
      this.svg = d3
        .select(div)
        .append("svg")
        .attr("width", svgWidth)
        .attr("height", svgHeight);

      this.svg
        .append("rect")
        .attr("width", svgWidth)
        .attr("height", svgHeight)
        .attr("fill", "#f8f9fa")
        .attr("rx", 4)
        .attr("ry", 4);

      // Process incoming bin data
      if (newData && Array.isArray(newData.bins) && newData.bins.length > 0) {
        this.data.bins = newData.bins.map((bin) => ({
          ...bin,
          length: this.safeNumber(bin.length),
          value: typeof bin.value === "bigint" ? Number(bin.value) : bin.value,
          x0: typeof bin.x0 === "bigint" ? Number(bin.x0) : bin.x0,
          x1: typeof bin.x1 === "bigint" ? Number(bin.x1) : bin.x1,
        }));
      } else if (newData && Array.isArray(newData) && newData.length > 0) {
        // If raw data is provided instead of bins, create bins
        const values = newData.filter((d) => d !== null && d !== undefined);

        if (this.data.type === "continuous") {
          // Create continuous bins
          const binGenerator = d3.bin();
          const bins = binGenerator(values);
          this.data.bins = bins.map((bin) => ({
            x0: bin.x0,
            x1: bin.x1,
            length: bin.length,
            values: bin,
          }));
        } else {
          // Create ordinal bins
          const valueCounts = d3.rollup(
            values,
            (v) => v.length,
            (d) => d
          );
          this.data.bins = Array.from(valueCounts, ([key, count]) => ({
            key,
            length: count,
            values: values.filter((v) => v === key),
          }));
        }
      }

      // Show "No data" message if still no valid bins
      if (!this.data.bins || this.data.bins.length === 0) {
        this.svg
          .append("text")
          .attr("x", svgWidth / 2)
          .attr("y", svgHeight / 2)
          .attr("text-anchor", "middle")
          .attr("dominant-baseline", "middle")
          .attr("font-size", "10px")
          .attr("fill", "#666")
          .text("No data");
        return;
      }

      // Calculate scale ranges
      const maxCount = Math.max(
        1,
        ...this.data.bins.map((b) => this.safeNumber(b.length))
      );
      const y = d3.scaleLinear().domain([0, maxCount]).range([height, 0]);

      // Create bar groups
      const barGroups = this.svg
        .selectAll(".bar")
        .data(this.data.bins)
        .join("g")
        .attr("class", "bar")
        .attr(
          "transform",
          (d, i) =>
            `translate(${(i * width) / this.data.bins.length + margin.left}, ${
              margin.top
            })`
        );

      // Add bars
      barGroups
        .append("rect")
        .attr("x", 1)
        .attr("width", Math.max(1, width / this.data.bins.length - 2))
        .attr("y", (d) => y(this.safeNumber(d.length)))
        .attr("height", (d) => height - y(this.safeNumber(d.length)))
        .attr("fill", "steelblue")
        .attr("rx", 2);

      // Add interaction for ordinal/nominal data
      if (this.data.type !== "continuous") {
        barGroups
          .append("rect")
          .attr("width", width / this.data.bins.length)
          .attr("height", height)
          .attr("fill", "transparent")
          .on("mouseover", (event, d) => {
            if (!d.selected) {
              d3.select(event.currentTarget.previousSibling).attr(
                "fill",
                "purple"
              );
            }
            this.showTooltip(d, width, height, margin);
          })
          .on("mouseout", (event, d) => {
            if (!d.selected) {
              d3.select(event.currentTarget.previousSibling).attr(
                "fill",
                "steelblue"
              );
            }
            this.hideTooltip();
          })
          .on("click", (event, d) => this.handleBarClick(d));
      }

      // Setup brush for continuous data
      if (this.data.type === "continuous") {
        this.setupBrush(width, height, margin);
      }
    } catch (error) {
      console.error("Error in setData:", error);
      // Show error state in visualization
      this.showErrorState();
    }
  };

  // Helper method to safely convert numbers
  this.safeNumber = function (val) {
    if (typeof val === "bigint") {
      try {
        return Number(val);
      } catch (e) {
        console.warn("BigInt conversion failed:", e);
        return 0;
      }
    }
    return typeof val === "number" ? val : 0;
  };

  // Helper method to show tooltip
  this.showTooltip = function (d, width, height, margin) {
    this.svg.selectAll(".histogram-label").remove();
    this.svg
      .append("text")
      .attr("class", "histogram-label")
      .attr("x", width / 2)
      .attr("y", height + margin.bottom)
      .attr("font-size", "10px")
      .attr("fill", "#444444")
      .attr("text-anchor", "middle")
      .text(d.key ? `${d.key}: ${d.length}` : `${d.x0} - ${d.x1}: ${d.length}`);
  };

  // Helper method to hide tooltip
  this.hideTooltip = function () {
    this.svg.selectAll(".histogram-label").remove();
  };

  // Helper method to show error state
  this.showErrorState = function () {
    div.innerHTML = "";
    const svgWidth = 120;
    const svgHeight = 60;

    this.svg = d3
      .select(div)
      .append("svg")
      .attr("width", svgWidth)
      .attr("height", svgHeight);

    this.svg
      .append("rect")
      .attr("width", svgWidth)
      .attr("height", svgHeight)
      .attr("fill", "#fff0f0")
      .attr("rx", 4)
      .attr("ry", 4);

    this.svg
      .append("text")
      .attr("x", svgWidth / 2)
      .attr("y", svgHeight / 2)
      .attr("text-anchor", "middle")
      .attr("dominant-baseline", "middle")
      .attr("font-size", "10px")
      .attr("fill", "#ff4444")
      .text("Error loading data");
  };

  // Setup brush for continuous data
  this.setupBrush = function (width, height, margin) {
    this.xScale = d3
      .scaleLinear()
      .domain([
        d3.min(this.data.bins, (d) => d.x0),
        d3.max(this.data.bins, (d) => d.x1),
      ])
      .range([0, width]);

    this.brush = d3
      .brushX()
      .extent([
        [0, 0],
        [width, height],
      ])
      .on("end", this.handleBrush);

    this.svg
      .append("g")
      .attr("class", "brush")
      .attr("transform", `translate(${margin.left}, ${margin.top})`)
      .call(this.brush);
  };

  // Add brushing for continuous data
  // Handle brush end event
  this.handleBrush = (event) => {
    // Remove any existing histogram label(s)
    this.svg.selectAll(".histogram-label").remove();

    if (!event.selection) {
      // If no selection from brushing, reset everything
      this.resetSelection();
      if (controller.table) {
        controller.table.clearSelection();
        controller.table.selectionUpdated();
      }
      return;
    }

    const [x0, x1] = event.selection;
    const [bound1, bound2] = event.selection.map(this.xScale.invert);
    const binWidth = width / this.bins.length;

    // Compute which bins are selected and update their color
    this.bins.forEach((bin, i) => {
      const binStart = i * binWidth;
      const binEnd = (i + 1) * binWidth;
      bin.selected = binStart <= x1 && binEnd >= x0;
      this.svg
        .select(`.bar:nth-child(${i + 1}) rect:nth-child(1)`)
        .attr("fill", bin.selected ? "orange" : "steelblue");
    });

    // Add histogram label
    this.svg
      // .data([d])
      .append("text")
      .attr("class", "histogram-label")
      .join("text")
      .attr("class", "histogram-label")
      .attr("x", width / 2)
      .attr("y", height + 10)
      .attr("font-size", "10px")
      .attr("fill", "#444444")
      .attr("text-anchor", "middle")
      // .text(`Selected: ${selectedLabels.join(", ")}`);
      .text(`Range: ${Math.round(bound1)} - ${Math.round(bound2)}`);

    // Update table selection if table exists
    if (controller.table) {
      controller.table.clearSelection();
      this.bins.forEach((bin) => {
        if (bin.selected) {
          bin.indeces.forEach((rowIndex) => {
            const tr = controller.table.tBody.querySelector(
              `tr:nth-child(${rowIndex + 1})`
            );
            if (tr) {
              controller.table.selectRow(tr);
            }
          });
        }
      });
      controller.table.selectionUpdated();
    }
  };

  this.updateVis = function () {
    if (!this.data || !this.data.bins || !this.data.bins.length) {
      console.warn("No valid bin data to visualize");
      return;
    }

    const bins = this.data.bins;
    const maxCount = Math.max(...bins.map((b) => b.length));

    // Update each bar in the histogram
    bins.forEach((bin, i) => {
      const height = (bin.length / maxCount) * 100;
      const bar = this.bars[i];

      if (bar) {
        // Update existing bar
        bar.style.height = `${height}%`;
        bar.setAttribute("data-count", bin.length);

        // Update tooltip content
        let tooltipContent = "";
        if (this.data.type === "continuous") {
          tooltipContent = `${bin.x0.toFixed(2)} - ${bin.x1.toFixed(2)}: ${
            bin.length
          }`;
        } else {
          tooltipContent = `${bin.key}: ${bin.length}`;
        }
        bar.setAttribute("title", tooltipContent);
      }
    });
  };

  this.table = null;
  this.setData(data);

  this.getNode = function () {
    // Create container if it doesn't exist
    if (!this.container) {
      this.container = document.createElement("div");
      Object.assign(this.container.style, {
        width: "100%",
        height: "40px",
        display: "flex",
        alignItems: "flex-end",
        gap: "1px",
        padding: "2px 0",
      });
    }

    // Clear existing bars
    this.container.innerHTML = "";
    this.bars = [];

    // Skip visualization if no data or bins
    if (!this.data?.bins || this.data.bins.length === 0) {
      return this.container;
    }

    const bins = this.data.bins;
    const maxCount = Math.max(...bins.map((b) => b.length || 0));

    // Create bars
    bins.forEach((bin) => {
      const bar = document.createElement("div");
      const height = maxCount > 0 ? (bin.length / maxCount) * 100 : 0;

      Object.assign(bar.style, {
        flex: "1",
        background: "#e0e0e0",
        height: `${height}%`,
        minHeight: "1px",
        transition: "height 0.2s ease-out",
        cursor: "pointer",
      });

      // Set tooltip content based on bin type
      let tooltipContent;
      if (this.data.type === "continuous") {
        tooltipContent = `${bin.x0?.toFixed?.(2) ?? bin.x0} - ${
          bin.x1?.toFixed?.(2) ?? bin.x1
        }: ${bin.length}`;
      } else if (this.data.type === "date") {
        tooltipContent = `${bin.x0?.toLocaleDateString()} - ${bin.x1?.toLocaleDateString()}: ${
          bin.length
        }`;
      } else {
        tooltipContent = `${bin.key || bin.x0}: ${bin.length}`;
      }
      bar.setAttribute("title", tooltipContent);
      bar.setAttribute("data-count", bin.length);

      // Add hover effect
      bar.addEventListener("mouseover", () => {
        bar.style.background = "#ccc";
      });
      bar.addEventListener("mouseout", () => {
        bar.style.background = "#e0e0e0";
      });

      this.bars.push(bar);
      this.container.appendChild(bar);
    });

    return this.container;
  };

  return this;
}

export { HistogramController };
