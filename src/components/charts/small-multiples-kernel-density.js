import { BaseSmallMultiples } from "../_base/small-multiples.js";
import { KernelDensityPlot } from "./kernel-density.js";

export class SmallMultiplesKernelDensity extends BaseSmallMultiples {
  constructor(config) {
    super({
      ...config,
      ChartClass: KernelDensityPlot,
      chartWidth: config.plotWidth || 200,
      chartHeight: config.plotHeight || 150,
    });
  }
}
