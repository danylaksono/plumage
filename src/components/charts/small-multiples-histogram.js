import * as d3 from "npm:d3";
import _ from "npm:lodash"; // Ensure this is imported.
import { BaseSmallMultiples } from "../_base/small-multiples.js";
import { Histogram } from "./histogram.js";

export class SmallMultiplesHistogram extends BaseSmallMultiples {
  constructor(config) {
    super({
      ...config,
      ChartClass: Histogram, // Specify Histogram as the chart type
      chartWidth: config.histogramWidth || 200,
      chartHeight: config.histogramHeight || 150,
    });
  }
}
