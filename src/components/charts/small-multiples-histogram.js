import * as d3 from "npm:d3";
import _ from "npm:lodash";
import { BaseSmallMultiples } from "../_base/small-multiples.js";
import { Histogram } from "./histogram.js";

export class SmallMultiplesHistogram extends BaseSmallMultiples {
  constructor(config) {
    // Calculate optimal chart dimensions based on container size and number of columns
    const numCharts = config.columns.length;
    const aspectRatio = 4 / 3; // Standard aspect ratio for histograms
    const numCols = Math.ceil(Math.sqrt(numCharts));
    const numRows = Math.ceil(numCharts / numCols);

    // Account for margins and gaps
    const margin = config.margin || {
      top: 40,
      right: 10,
      bottom: 30,
      left: 30,
    };
    const gap = config.gap || { horizontal: 20, vertical: 20 };

    // Calculate available space
    const availableWidth = config.width - margin.left - margin.right;
    const availableHeight = config.height - margin.top - margin.bottom;

    // Calculate optimal chart dimensions
    const optimalWidth = Math.floor(
      (availableWidth - (numCols - 1) * gap.horizontal) / numCols
    );
    const optimalHeight = Math.floor(
      (availableHeight - (numRows - 1) * gap.vertical) / numRows
    );

    super({
      ...config,
      ChartClass: Histogram,
      chartWidth: optimalWidth,
      chartHeight: optimalHeight,
      margin,
      gap,
    });

    // Add natural language query UI configuration
    this.queryUIConfig = {
      enabled: config.nlQueryEnabled !== false, // Default to true unless explicitly disabled
      position: config.nlQueryPosition || "top", // 'top', 'bottom', 'left', 'right'
      placeholder:
        config.nlQueryPlaceholder || "Ask a question about your data...",
      buttonLabel: config.nlQueryButtonLabel || "Search",
      height: config.nlQueryHeight || 40,
      width: config.nlQueryWidth || "100%",
      executeOnEnter: config.executeOnEnter !== false, // Default true
    };

    // Track global selection state
    this.globalSelectedData = [];
    this.isSynchronizing = false; // Flag to prevent selection loops
  }

  // Override initialize to set up selection synchronization
  async initialize() {
    await super.initialize();

    if (this.queryUIConfig.enabled) {
      this.createQueryInterface();
    }

    // Set up selection synchronization between charts
    this.setupSelectionSynchronization();

    return this;
  }

  // Add this new method for selection synchronization
  setupSelectionSynchronization() {
    // Add event listeners to each chart
    this.charts.forEach((chart) => {
      chart.on("selectionChanged", (selectedData) => {
        // Prevent infinite loops when synchronizing
        if (this.isSynchronizing) return;

        console.log(
          `Selection changed in chart for ${chart.config.column}, selected ${selectedData.length} items`
        );

        // Store the global selection
        this.globalSelectedData = selectedData;

        // Synchronize selection across all charts
        this.synchronizeSelections(chart, selectedData);
      });
    });
  }

  // Add this method to synchronize selections
  synchronizeSelections(sourceChart, selectedData) {
    // Set flag to prevent recursive selection updates
    this.isSynchronizing = true;

    try {
      if (selectedData.length === 0) {
        // Clear selections in all charts
        this.charts.forEach((chart) => {
          if (chart !== sourceChart) {
            chart.clearSelection();
          }
        });
      } else {
        // Extract row IDs for synchronization
        const rowIds = selectedData.map((row, index) => index);

        // Update all other charts with this selection
        this.charts.forEach((chart) => {
          if (chart !== sourceChart) {
            chart.highlightData(rowIds);
          }
        });
      }
    } finally {
      // Reset flag
      this.isSynchronizing = false;
    }
  }

  /**
   * Creates the natural language query interface
   */
  createQueryInterface() {
    // Create container div for the query interface
    const containerId = `nl-query-container-${Math.random()
      .toString(36)
      .substring(2, 9)}`;

    this.queryContainer = d3
      .create("div")
      .attr("id", containerId)
      .attr("class", "nl-query-container")
      .style(
        "width",
        typeof this.queryUIConfig.width === "number"
          ? `${this.queryUIConfig.width}px`
          : this.queryUIConfig.width
      )
      .style("margin", "10px 0")
      .style("display", "flex")
      .style("align-items", "center")
      .style("gap", "10px");

    // Create input field
    this.queryInput = this.queryContainer
      .append("input")
      .attr("type", "text")
      .attr("placeholder", this.queryUIConfig.placeholder)
      .attr("class", "nl-query-input")
      .style("flex", "1")
      .style("padding", "8px 12px")
      .style("border", "1px solid #ccc")
      .style("border-radius", "4px")
      .style("font-size", "14px")
      .style("height", `${this.queryUIConfig.height}px`);

    // Create search button
    this.queryButton = this.queryContainer
      .append("button")
      .attr("class", "nl-query-button")
      .style("padding", "8px 16px")
      .style("background-color", "#4CAF50")
      .style("color", "white")
      .style("border", "none")
      .style("border-radius", "4px")
      .style("cursor", "pointer")
      .style("height", `${this.queryUIConfig.height}px`)
      .style("white-space", "nowrap")
      .text(this.queryUIConfig.buttonLabel);

    // Create results area
    this.queryResults = this.queryContainer
      .append("div")
      .attr("class", "nl-query-results")
      .style("display", "none")
      .style("margin-top", "10px")
      .style("padding", "10px")
      .style("border", "1px solid #e1e1e1")
      .style("border-radius", "4px")
      .style("background-color", "#f9f9f9");

    // Add the container based on the configured position
    const svgNode = this.svg?.node();
    if (!svgNode || !svgNode.parentNode) {
      console.warn(
        "SVG element or parent node not found. Appending query interface to chart container instead."
      );

      // Fallback: Append to chart container directly
      if (this.container) {
        this.container.node().appendChild(this.queryContainer.node());
      } else {
        console.error("No container available to attach query interface");
        return; // Exit if we can't attach it anywhere
      }

      // Setup event listeners and return
      this.setupQueryEventListeners();
      return;
    }

    const parentNode = svgNode.parentNode;

    // Insert the query container before or after the SVG based on position
    if (this.queryUIConfig.position === "top") {
      parentNode.insertBefore(this.queryContainer.node(), svgNode);
    } else if (this.queryUIConfig.position === "bottom") {
      parentNode.insertBefore(this.queryContainer.node(), svgNode.nextSibling);
    } else {
      // For left/right positions, wrap both elements in a flex container
      const wrapperDiv = document.createElement("div");
      wrapperDiv.style.display = "flex";
      wrapperDiv.style.flexDirection =
        this.queryUIConfig.position === "left" ? "row" : "row-reverse";
      wrapperDiv.style.alignItems = "flex-start";
      wrapperDiv.style.gap = "20px";

      // Replace SVG with the wrapper and add both elements to it
      parentNode.replaceChild(wrapperDiv, svgNode);
      wrapperDiv.appendChild(this.queryContainer.node());
      wrapperDiv.appendChild(svgNode);

      // Adjust query container for vertical layout
      this.queryContainer
        .style("flex-direction", "column")
        .style(
          "width",
          typeof this.queryUIConfig.width === "number"
            ? `${this.queryUIConfig.width}px`
            : "250px"
        );
    }

    // Add event listeners
    this.setupQueryEventListeners();
  }

  /**
   * Set up event listeners for the query interface
   */
  setupQueryEventListeners() {
    // Button click event
    this.queryButton.on("click", () => this.executeQuery());

    // Enter key press event
    if (this.queryUIConfig.executeOnEnter) {
      this.queryInput.on("keypress", (event) => {
        if (event.key === "Enter") {
          event.preventDefault();
          this.executeQuery();
        }
      });
    }
  }

  /**
   * Execute the natural language query
   */
  async executeQuery() {
    const query = this.queryInput.property("value").trim();
    if (!query) return;

    // Show loading indicator
    this.showQueryLoading();

    try {
      // Execute query using the BaseVisualization method
      const results = await this.naturalLanguageQuery(query);

      // Update visualizations based on results
      this.updateVisualizationsWithResults(results, query);

      // Show success message
      this.showQuerySuccess(
        `Query executed successfully. Found ${results.length} results.`
      );
    } catch (error) {
      console.error("Natural language query error:", error);
      this.showQueryError(error.message);
    }
  }

  /**
   * Update visualizations with query results
   * @param {Array} results - Query results
   * @param {string} query - The original query string
   */
  updateVisualizationsWithResults(results, query) {
    // If no results, show message
    if (!results || results.length === 0) {
      this.showQueryMessage("No results found for your query.");
      return;
    }

    // Check if it's a distribution/statistical query result
    const isSummaryResult =
      results.length === 1 &&
      Object.keys(results[0]).some(
        (key) =>
          key.includes("avg") ||
          key.includes("percentile") ||
          key.includes("min") ||
          key.includes("max")
      );

    if (isSummaryResult) {
      // For summary results, display them in the query results area
      this.displaySummaryResults(results[0]);
    } else {
      // For data filtering results, update the visualizations

      // Filter the data for each chart
      this.charts.forEach((chart) => {
        try {
          // Update the chart with new filtered data
          chart.updateWithData(results);
        } catch (err) {
          console.error("Error updating chart with query results:", err);
        }
      });

      // Show how many records were found
      this.showQueryMessage(
        `Showing ${results.length} records that match your query.`
      );
    }
  }

  /**
   * Display summary statistical results
   * @param {Object} summaryData - Summary statistics object
   */
  displaySummaryResults(summaryData) {
    // Clear previous results
    const resultsContainer = this.queryResults
      .style("display", "block")
      .html("");

    // Create table for results
    const table = resultsContainer
      .append("table")
      .style("width", "100%")
      .style("border-collapse", "collapse");

    // Add header row
    const thead = table.append("thead");
    thead
      .append("tr")
      .selectAll("th")
      .data(["Statistic", "Value"])
      .enter()
      .append("th")
      .style("text-align", "left")
      .style("padding", "8px")
      .style("border-bottom", "1px solid #ddd")
      .text((d) => d);

    // Add data rows
    const tbody = table.append("tbody");
    Object.entries(summaryData).forEach(([key, value]) => {
      // Format key name to be more readable
      const formattedKey = key
        .replace(/_/g, " ")
        .replace(/\b\w/g, (char) => char.toUpperCase());

      // Format value based on type
      let formattedValue = value;
      if (typeof value === "number") {
        formattedValue = Number.isInteger(value) ? value : value.toFixed(4);
      } else if (value instanceof Date) {
        formattedValue = value.toLocaleString();
      }

      tbody
        .append("tr")
        .style("border-bottom", "1px solid #eee")
        .selectAll("td")
        .data([formattedKey, formattedValue])
        .enter()
        .append("td")
        .style("padding", "8px")
        .text((d) => d);
    });
  }

  /**
   * Show a loading indicator
   */
  showQueryLoading() {
    this.queryResults
      .style("display", "block")
      .html(
        '<div style="text-align: center; padding: 10px;">Processing your query...</div>'
      );
  }

  /**
   * Show an error message
   * @param {string} message - Error message
   */
  showQueryError(message) {
    this.queryResults.style("display", "block")
      .html(`<div style="color: #D8000C; background: #FFBABA; padding: 10px; border-radius: 4px;">
        <strong>Error:</strong> ${message}
      </div>`);
  }

  /**
   * Show a success message
   * @param {string} message - Success message
   */
  showQuerySuccess(message) {
    this.queryResults.style("display", "block")
      .html(`<div style="color: #4F8A10; background: #DFF2BF; padding: 10px; border-radius: 4px;">
        ${message}
      </div>`);
  }

  /**
   * Show a general message
   * @param {string} message - Message text
   */
  showQueryMessage(message) {
    this.queryResults.style("display", "block")
      .html(`<div style="padding: 10px; border-radius: 4px;">
        ${message}
      </div>`);
  }
}
