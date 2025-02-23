import { BaseSmallMultiples } from "../_base/small-multiples.js";
import { ViolinPlot } from "./violin-plot.js";

export class SmallMultiplesViolin extends BaseSmallMultiples {
  constructor(config) {
    super({
      ...config,
      ChartClass: ViolinPlot,
      chartWidth: config.violinWidth || 150, // Default width optimized for violin plots
      chartHeight: config.violinHeight || 200, // Taller default height for better distribution visibility
      margin: {
        ...config.margin,
        left: 50, // Increased left margin for y-axis labels
      },
    });
  }
}
