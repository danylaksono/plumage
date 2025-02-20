import * as d3 from "npm:d3";
import _ from "npm:lodash"; // Ensure this is imported.
import { BaseVisualization } from "./base.js";
import { Histogram } from "./histogram.js";

export class SmallMultiplesHistogram extends BaseVisualization {
  constructor(config) {
    const smallMultiplesDefaults = {
      columns: [], // Array of column names for histograms
      histogramWidth: 200, // Individual histogram width
      histogramHeight: 150, // Individual histogram height
      margin: { top: 10, right: 10, bottom: 30, left: 30 },
      // dataSource: null,     // Data source (if not already loaded in Base)
      // dataFormat: null,    // Data format
    };

    super({ ...smallMultiplesDefaults, ...config });

    this.histograms = []; // Array to hold individual Histogram instances
    this.selectedData = []; // Selected data shared across histograms
    this.update = this.update.bind(this); // Bind this to the update function

    if (!this.initialized) {
      this.createSvg();
    }
  }

  async initialize() {
    await super.initialize(); // Call BaseVisualization's initialize first
    await this.createHistograms();
    this.setupLinkedInteractivity();
    return this;
  }

  async createHistograms() {
    const { columns, histogramWidth, histogramHeight } = this.config;

    if (!columns || columns.length === 0) {
      console.warn("No columns specified for SmallMultiplesHistogram.");
      return;
    }

    // Determine the layout of the histograms (rows and columns)
    const containerWidth = this.config.width;
    const histogramsPerRow = Math.floor(containerWidth / histogramWidth);
    const numRows = Math.ceil(columns.length / histogramsPerRow);

    this.histograms = [];

    for (let i = 0; i < columns.length; i++) {
      const column = columns[i];

      const row = Math.floor(i / histogramsPerRow);
      const col = i % histogramsPerRow;

      const xOffset = col * histogramWidth;
      const yOffset = row * histogramHeight;

      const histogramConfig = {
        column: column,
        width:
          histogramWidth - this.config.margin.left - this.config.margin.right,
        height:
          histogramHeight - this.config.margin.top - this.config.margin.bottom,
        colors: this.config.colors,
        selectionMode: this.config.selectionMode,
        dataSource: this.config.dataSource, // Share the data source
        dataFormat: this.config.dataFormat,
        dataProcessor: this.dataProcessor,
        axis: true,
        tableName: this.tableName,
        margin: this.config.margin, // Share the margin
      };

      const histogram = new Histogram(histogramConfig);

      // Create a new group (g) for each histogram
      const histogramGroup = this.g
        .append("g")
        .attr("transform", `translate(${xOffset},${yOffset})`)
        .attr("class", `histogram-group histogram-group-${column}`); // Unique class for each histogram
      // Set the 'g' element to the histogram instance.
      histogram.g = histogramGroup;

      await histogram.initialize(); // Make sure you call the individual initialize
      await histogram.update();

      this.histograms.push(histogram);
    }
  }

  setupLinkedInteractivity() {
    const self = this;

    this.histograms.forEach((histogram) => {
      histogram.on("selectionChanged", async (selectedData) => {
        self.selectedData = selectedData;

        // Update highlighting in other histograms
        for (const otherHistogram of self.histograms) {
          if (otherHistogram !== histogram) {
            // Get the column values from selected data for the current histogram
            const selectedValues = selectedData.map(
              (row) => row[otherHistogram.config.column]
            );
            await otherHistogram.highlightDataByValue(selectedValues);
          }
        }

        // Dispatch selection event from SmallMultiples for external listeners
        self.dispatch.call("selectionChanged", self, selectedData);
      });
    });
  }

  async update() {
    await this.initialize();
  }

  async updateOtherHistograms(sourceHistogram) {
    const self = this;

    for (const histogram of this.histograms) {
      if (histogram !== sourceHistogram) {
        if (self.selectedData?.length > 0) {
          const column = histogram.config.column;

          const filterValues = this.selectedData.map(
            (item) => item[self.config.columns[0]]
          ); // get the first item from columns

          if (filterValues.length > 0) {
            const filterClause = `${
              self.config.columns[0]
            } IN ('${filterValues.join("','")}')`;
            const filteredData = await this.dataProcessor.getFilteredData(
              filterClause
            );
            const bins = await this.dataProcessor.binDataWithDuckDB(
              column,
              histogram.type,
              histogram.config.maxOrdinalBins
            );

            histogram.bins = bins;
            histogram.xScale = histogram.createXScale();
            histogram.yScale = histogram.createYScale();

            histogram.drawBars();
            histogram.drawAxes();
            histogram.drawLabels();
          }
        } else {
          // Reset the histogram to its original state (no filter)
          await histogram.update();
        }
      }
    }
  }

  async destroy() {
    // Destroy individual histograms
    for (const histogram of this.histograms) {
      await histogram.destroy();
    }
    this.histograms = [];

    await super.destroy(); // Call BaseVisualization's destroy
  }
}
