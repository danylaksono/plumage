import * as d3 from "npm:d3";

// Modified HistogramController to leverage DuckDB for binning instead of client-side binning
export class HistogramController {
  constructor(initialData, config = {}) {
    // Removed internal binning logic based on raw data
    // Instead, we store configuration and expect bin data from DuckDB
    this.config = config;
    this.bins = config.binInfo || [];
    this.unique = config.unique || false;
    this.columnName = config.columnName || null;
    this.highlightedData = null;
    // Create a basic container for histogram
    this.node = document.createElement("div");
    this.node.classList.add("histogram-container");
    // Initial render
    this.render();
  }

  // New method to update histogram bin data fetched from DuckDB
  setData(visData) {
    // visData expected format: { type, bins, nominals? }
    this.config.type = visData.type;
    this.bins = visData.bins || [];
    if (visData.type === "ordinal") {
      this.nominals = visData.nominals || [];
    }
    this.render();
  }

  // Render histogram using the bin data from DuckDB
  render() {
    // Simple rendering logic: clear container and show bins as bars
    this.node.innerHTML = "";
    if (!this.bins || this.bins.length === 0) {
      this.node.innerText = "No bin data available";
      return;
    }

    // Create an SVG element for displaying histogram
    const svgWidth = 200;
    const svgHeight = 100;
    const svgNS = "http://www.w3.org/2000/svg";
    const svg = document.createElementNS(svgNS, "svg");
    svg.setAttribute("width", svgWidth);
    svg.setAttribute("height", svgHeight);

    // Calculate max count from bins for scaling
    const maxCount = Math.max(...this.bins.map((bin) => bin.count));

    const barWidth = svgWidth / this.bins.length;

    this.bins.forEach((bin, index) => {
      const barHeight = (bin.count / maxCount) * svgHeight;
      const rect = document.createElementNS(svgNS, "rect");
      rect.setAttribute("x", index * barWidth);
      rect.setAttribute("y", svgHeight - barHeight);
      rect.setAttribute("width", barWidth - 1);
      rect.setAttribute("height", barHeight);
      rect.setAttribute("fill", "#69b3a2");
      // Add simple click listener for selection (if needed)
      rect.addEventListener("click", () => {
        if (this.onBrush) {
          // Simulate a brush selection using bin boundaries
          const range = [bin.x0, bin.x1];
          this.onBrush(range);
        }
      });
      svg.appendChild(rect);
    });

    this.node.appendChild(svg);
  }

  getNode() {
    return this.node;
  }

  // Optional: method to reset selection highlights
  resetSelection() {
    this.highlightedData = null;
    this.render();
  }
}
