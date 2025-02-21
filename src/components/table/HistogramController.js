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

  this.setData = function (dd) {
    div.innerHTML = "";

    let data = dd.map((d, i) => ({ value: d, index: i }));

    const svgWidth = 120; // Slightly wider
    const svgHeight = 60; // Slightly taller
    const margin = { top: 5, right: 8, bottom: 10, left: 8 };
    const width = svgWidth - margin.left - margin.right;
    const height = svgHeight - margin.top - margin.bottom;

    this.svg = d3
      .select(div)
      .append("svg")
      .attr("width", svgWidth)
      .attr("height", svgHeight);

    // Add background and border to SVG
    this.svg
      .append("rect")
      .attr("width", svgWidth)
      .attr("height", svgHeight)
      .attr("fill", "#f8f9fa")
      .attr("rx", 4) // Rounded corners
      .attr("ry", 4);

    console.log("------------------binrules in setData: ", binrules);

    if (binrules.unique) {
      // Handle unique columns: create a single bin
      this.bins = [
        {
          category: "Unique Values",
          count: data.length,
          indeces: data.map((d) => d.index),
        },
      ];
    } else if ("thresholds" in binrules) {
      console.log("------------------Continuous data----------------------");
      // Continuous data
      // console.log("Domain: ", [
      //   d3.min(data, (d) => d.value),
      //   d3.max(data, (d) => d.value),
      // ]);

      // console.log("Thresholds: ", binrules.thresholds);
      let contBins = d3
        .bin()
        .domain([d3.min(data, (d) => d.value), d3.max(data, (d) => d.value)])
        .thresholds(binrules.thresholds)
        .value((d) => d.value)(data);

      this.bins = contBins.map((b) => ({
        category: b.x0 + "-" + b.x1,
        count: b.length,
        indeces: b.map((v) => v.index),
      }));

      // console.log("Brush Bins: ", this.bins);

      this.xScale = d3
        .scaleLinear()
        .domain([d3.min(data, (d) => d.value), d3.max(data, (d) => d.value)])
        .range([0, width]);

      // Initialize brush for continuous data
      this.brush = d3
        .brushX()
        .extent([
          [0, 0],
          [svgWidth, svgHeight],
        ])
        .on("end", this.handleBrush);

      // Add brush to svg
      this.svg
        .append("g")
        .attr("class", "brush")
        .style("position", "absolute")
        .style("z-index", 90999) // Attempt to force the brush on top
        .call(this.brush);
    } else if ("ordinals" in binrules || "nominals" in binrules) {
      // Handle ordinal or nominal data
      const frequency = d3.rollup(
        data,
        (values) => ({
          count: values.length,
          indeces: values.map((v) => v.index),
        }),
        (d) => d.value
      );

      const binType = "ordinals" in binrules ? "ordinals" : "nominals";

      // use predefined bin order if available
      if (binType in binrules && Array.isArray(binrules[binType])) {
        this.bins = binrules[binType].map((v) => ({
          category: v,
          count: frequency.get(v) != null ? frequency.get(v).count : 0,
          indeces: frequency.get(v) != null ? frequency.get(v).indeces : [],
        }));
      } else {
        this.bins = Array.from(frequency, ([key, value]) => ({
          category: key,
          count: value.count,
          indeces: value.indeces,
        }));
      }
    }

    this.bins.map((bin, i) => (bin.index = i));

    const y = d3
      .scaleLinear()
      .domain([0, d3.max(this.bins, (d) => d.count)])
      .range([height, 0]);

    const barGroups = this.svg
      .selectAll(".bar")
      .data(this.bins)
      .join("g")
      .attr("class", "bar")
      .attr(
        "transform",
        (d, i) => `translate(${(i * width) / this.bins.length}, 0)`
      );

    // Visible bars
    barGroups
      .append("rect")
      .attr("x", 1) // Small gap between bars
      .attr("width", (d) => width / this.bins.length - 2)
      .attr("y", (d) => y(d.count))
      .attr("height", (d) => height - y(d.count))
      .attr("fill", "steelblue")
      .attr("rx", 2) // Rounded corners for bars
      .style("transition", "fill 0.2s");

    // Improve tooltip styling
    const tooltip = d3
      .select(div)
      .append("div")
      .attr("class", "histogram-tooltip")
      .style("position", "absolute")
      .style("visibility", "hidden")
      .style("background", "white")
      .style("padding", "5px")
      .style("border-radius", "4px")
      .style("box-shadow", "0 2px 4px rgba(0,0,0,0.1)");

    // For continuous data, we don't need the invisible interaction bars
    // Only add them for ordinal/nominal data
    if (!("thresholds" in binrules)) {
      barGroups
        .append("rect")
        .attr("width", (d) => width / this.bins.length)
        .attr("height", height)
        .attr("fill", "transparent")
        .on("mouseover", (event, d) => {
          if (!d.selected) {
            d3.select(event.currentTarget.previousSibling).attr(
              "fill",
              "purple"
            );
          }

          this.svg
            .selectAll(".histogram-label")
            .data([d])
            .join("text")
            .attr("class", "histogram-label")
            .attr("x", width / 2)
            .attr("y", height + 10)
            .attr("font-size", "10px")
            .attr("fill", "#444444")
            .attr("text-anchor", "middle")
            .text(d.category + ": " + d.count);
        })
        .on("mouseout", (event, d) => {
          if (!d.selected) {
            d3.select(event.currentTarget.previousSibling).attr(
              "fill",
              "steelblue"
            );
          }

          this.svg.selectAll(".histogram-label").remove();
        })
        .on("click", (event, d) => {
          console.log("Histogram bar clicked:", d);
          if (
            controller.table &&
            controller.table.tableRenderer &&
            controller.table.tableRenderer.tBody
          ) {
            if (!d.selected) {
              controller.table.clearSelection();
            }

            this.bins[d.index].indeces.forEach((rowIndex) => {
              console.log("Row index:", rowIndex);

              //deselect first
              controller.table.data.forEach((d) => (d.selected = false));
              const tr = controller.table.tableRenderer.tBody.querySelector(
                `tr:nth-child(${rowIndex + 1})`
              );
              if (tr) {
                if (d.selected) {
                  controller.table.selectRow(tr);
                } else {
                  controller.table.unselectRow(tr);
                }
              }
            });
            controller.table.selectionUpdated();
          } else {
            console.warn(
              "controller.table or controller.table.tBody is undefined"
            );
          }
        });
    }
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
  };

  this.table = null;
  this.setData(data);

  this.getNode = () => div;
  return this;
}

export { HistogramController };
