import { BaseVisualization, ChartConfig } from "./base.js";

export class BaseSmallMultiples extends BaseVisualization {
  constructor(config) {
    const smallMultiplesDefaults = {
      columns: [], // Array of column names for charts
      chartWidth: 200, // Individual chart width
      chartHeight: 150, // Individual chart height
      margin: { top: 40, right: 10, bottom: 30, left: 30 },
      showTitle: true,
      showAxis: false,
      gap: { horizontal: 10, vertical: 10 },
      ChartClass: null, // Class to use for individual charts
    };

    super({ ...smallMultiplesDefaults, ...config });

    if (!this.config.ChartClass) {
      throw new Error("ChartClass must be provided in config");
    }

    this.charts = [];
    this.selectedData = [];
    this.update = this.update.bind(this);

    console.log("[SmallMultiples] Initializing with config:", {
      columns: config.columns,
      chartWidth: config.chartWidth,
      chartHeight: config.chartHeight,
      ChartClass: config.ChartClass?.name,
    });
  }

  async initialize() {
    await super.initialize();
    await this.createCharts();
    this.setupLinkedInteractivity();
    return this;
  }

  async createCharts() {
    const { columns, chartWidth, chartHeight, showTitle, gap, ChartClass } =
      this.config;

    if (!columns || columns.length === 0) {
      console.warn(
        "[SmallMultiples] No columns specified for small multiples."
      );
      return;
    }

    console.log("[SmallMultiples] Creating charts layout:", {
      numColumns: columns.length,
      chartWidth,
      chartHeight,
      containerWidth: this.config.width,
    });

    const containerWidth = this.config.width;
    const effectiveWidth = chartWidth + gap.horizontal;
    const chartsPerRow = Math.floor(containerWidth / effectiveWidth);
    const numRows = Math.ceil(columns.length / chartsPerRow);

    console.log("[SmallMultiples] Layout calculated:", {
      chartsPerRow,
      numRows,
      effectiveWidth,
    });

    this.charts = [];

    for (let i = 0; i < columns.length; i++) {
      const column = columns[i];
      const row = Math.floor(i / chartsPerRow);
      const col = i % chartsPerRow;

      const xOffset = col * (chartWidth + gap.horizontal);
      const yOffset = row * (chartHeight + gap.vertical);

      console.log(
        `[SmallMultiples] Creating chart ${i + 1}/${columns.length}:`,
        {
          column,
          position: { row, col },
          offset: { x: xOffset, y: yOffset },
        }
      );

      const chartConfig = new ChartConfig({
        column: column,
        width: chartWidth - this.config.margin.left - this.config.margin.right,
        height:
          chartHeight - this.config.margin.top - this.config.margin.bottom,
        colors: this.config.colors,
        selectionMode: this.config.selectionMode,
        dataSource: this.config.dataSource,
        dataFormat: this.config.dataFormat,
        dataProcessor: this.dataProcessor,
        axis: this.config.showAxis,
        tableName: this.tableName,
        margin: {
          ...this.config.margin,
          top: showTitle ? 20 : this.config.margin.top,
        },
      });

      const chart = new ChartClass(chartConfig);

      const chartGroup = this.g
        .append("g")
        .attr("transform", `translate(${xOffset},${yOffset})`)
        .attr("class", `chart-group chart-group-${column}`);

      if (showTitle) {
        chartGroup
          .append("text")
          .attr("class", "chart-title")
          .attr("x", chartWidth / 2)
          .attr("y", this.config.margin.top / 2)
          .attr("text-anchor", "middle")
          .attr("dominant-baseline", "middle")
          .attr("font-size", "12px")
          .text(column);
      }

      const contentGroup = chartGroup
        .append("g")
        .attr(
          "transform",
          `translate(${this.config.margin.left},${this.config.margin.top})`
        );

      chart.g = contentGroup;
      await chart.initialize();
      await chart.update();

      this.charts.push(chart);

      console.log(
        `[SmallMultiples] Chart ${i + 1} initialized for column:`,
        column
      );
    }
  }

  setupLinkedInteractivity() {
    console.log(
      "[SmallMultiples] Setting up linked interactivity for",
      this.charts.length,
      "charts"
    );
    const self = this;

    this.charts.forEach((chart) => {
      chart.on("selectionChanged", async (selectedData) => {
        self.selectedData = selectedData;

        // Update highlighting in other charts
        for (const otherChart of self.charts) {
          if (otherChart !== chart) {
            const selectedValues = selectedData.map(
              (row) => row[otherChart.config.column]
            );
            await otherChart.highlightDataByValue(selectedValues);
          }
        }

        self.dispatch.call("selectionChanged", self, selectedData);
      });
    });
  }

  async update() {
    console.log("[SmallMultiples] Starting update");
    await this.initialize();
    console.log("[SmallMultiples] Update complete");
  }

  async updateOtherCharts(sourceChart) {
    const self = this;

    for (const chart of this.charts) {
      if (chart !== sourceChart) {
        if (self.selectedData?.length > 0) {
          const column = chart.config.column;
          const filterValues = this.selectedData.map(
            (item) => item[self.config.columns[0]]
          );

          if (filterValues.length > 0) {
            const filterClause = `${
              self.config.columns[0]
            } IN ('${filterValues.join("','")}')`;
            const filteredData = await this.dataProcessor.getFilteredData(
              filterClause
            );
            await chart.update(filteredData);
          }
        } else {
          await chart.update();
        }
      }
    }
  }

  async destroy() {
    for (const chart of this.charts) {
      await chart.destroy();
    }
    this.charts = [];
    await super.destroy();
  }
}
