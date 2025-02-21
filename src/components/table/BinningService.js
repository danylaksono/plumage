import * as d3 from "npm:d3";

export class BinningService {
  static DEFAULT_CONFIG = {
    maxOrdinalBins: 20,
    continuousBinMethod: "freedmanDiaconis", // Changed from inconsistent naming
    dateInterval: "day",
    minBinSize: 5,
    customThresholds: null,
  };

  constructor(config = {}) {
    this.config = {
      ...BinningService.DEFAULT_CONFIG,
      ...config,
    };
    this.validateConfig();
  }

  validateConfig() {
    const validMethods = ["freedmanDiaconis", "scott"];
    const validIntervals = ["hour", "day", "week", "month", "year"];

    if (!validMethods.includes(this.config.continuousBinMethod)) {
      throw new Error(
        `Invalid continuous bin method. Must be one of: ${validMethods.join(
          ", "
        )}`
      );
    }
    if (!validIntervals.includes(this.config.dateInterval)) {
      throw new Error(
        `Invalid date interval. Must be one of: ${validIntervals.join(", ")}`
      );
    }
    if (this.config.maxOrdinalBins < 2) {
      throw new Error("maxOrdinalBins must be at least 2");
    }
    if (this.config.minBinSize < 1) {
      throw new Error("minBinSize must be at least 1");
    }
  }

  getBins(data, column, type) {
    if (!data || !Array.isArray(data) || data.length === 0) {
      return [];
    }

    if (!column || typeof column !== "string") {
      throw new Error("Column must be a non-empty string");
    }

    const values = data
      .map((d) => d[column])
      .filter((v) => v !== null && v !== undefined);

    if (values.length === 0) {
      return [];
    }

    if (this.config.customThresholds?.[column]) {
      return this.getContinuousBins(
        data,
        column,
        this.config.customThresholds[column]
      );
    }

    switch (type?.toLowerCase()) {
      case "continuous":
        return this.getContinuousBins(data, column);
      case "ordinal":
        return this.getOrdinalBins(data, column);
      case "date":
        return this.getDateBins(data, column);
      default:
        return this.autoDetectAndBin(data, column);
    }
  }

  autoDetectAndBin(data, column) {
    const sample = data[0]?.[column];

    if (sample instanceof Date) {
      return this.getDateBins(data, column);
    }

    if (typeof sample === "number" || !isNaN(Number(sample))) {
      return this.getContinuousBins(data, column);
    }

    return this.getOrdinalBins(data, column);
  }

  getContinuousBins(data, column, customThresholds = null) {
    const values = data
      .map((d) => d[column])
      .filter((v) => v != null && !isNaN(Number(v)))
      .map((v) => Number(v))
      .sort((a, b) => a - b);

    if (values.length === 0) return [];

    const extent = d3.extent(values);
    if (extent[0] === extent[1]) {
      return [this.createSingleValueBin(values, extent[0])];
    }

    const useLogTransform = this.shouldUseLogTransform(values);
    const transformedValues = useLogTransform
      ? values.map((v) => Math.log(v))
      : values;
    const transformedExtent = d3.extent(transformedValues);

    const binCount =
      customThresholds ||
      this.calculateBinCount(transformedValues, transformedExtent);
    const bins = d3.bin().domain(transformedExtent).thresholds(binCount)(
      transformedValues
    );

    const formattedBins = this.formatBins(bins, useLogTransform);
    return this.consolidateBins(formattedBins);
  }

  shouldUseLogTransform(values) {
    const meanValue = d3.mean(values);
    const medianValue = d3.median(values);
    return values[0] > 0 && medianValue > 0 && meanValue / medianValue > 2;
  }

  calculateBinCount(values, extent) {
    return this.config.continuousBinMethod === "scott"
      ? this.calculateScottBinCount(values, extent)
      : this.calculateFreedmanDiaconisBinCount(values, extent);
  }

  createSingleValueBin(values, value) {
    return {
      x0: value,
      x1: value,
      length: values.length,
      values: values,
      count: values.length,
      mean: value,
      median: value,
      min: value,
      max: value,
    };
  }

  formatBins(bins, useLogTransform) {
    return bins.map((bin) => {
      const x0 = useLogTransform ? Math.exp(bin.x0) : bin.x0;
      const x1 = useLogTransform ? Math.exp(bin.x1) : bin.x1;
      const values = useLogTransform
        ? bin.map((v) => Math.exp(v))
        : Array.from(bin);

      return {
        x0,
        x1,
        length: values.length,
        values,
        count: values.length,
        mean: d3.mean(values),
        median: d3.median(values),
        min: d3.min(values),
        max: d3.max(values),
      };
    });
  }

  consolidateBins(bins) {
    const consolidated = [];
    let currentBin = bins[0];

    for (let i = 1; i < bins.length; i++) {
      if (bins[i].count < this.config.minBinSize) {
        currentBin = this.mergeBins(currentBin, bins[i]);
      } else {
        consolidated.push(currentBin);
        currentBin = bins[i];
      }
    }
    consolidated.push(currentBin);

    return consolidated.sort((a, b) => a.x0 - b.x0);
  }

  mergeBins(bin1, bin2) {
    const values = [...bin1.values, ...bin2.values];
    return {
      x0: bin1.x0,
      x1: bin2.x1,
      length: values.length,
      values,
      count: values.length,
      mean: d3.mean(values),
      median: d3.median(values),
      min: d3.min(values),
      max: d3.max(values),
    };
  }

  calculateFreedmanDiaconisBinCount(values, extent) {
    const n = values.length;
    const q1 = d3.quantile(values, 0.25);
    const q3 = d3.quantile(values, 0.75);
    const iqr = q3 - q1;

    if (iqr === 0) {
      return Math.ceil(Math.log2(n) + 1); // Sturges' formula fallback
    }

    const binWidth = (2 * iqr) / Math.cbrt(n);
    const range = extent[1] - extent[0];
    return Math.max(1, Math.ceil(range / binWidth));
  }

  calculateScottBinCount(values, extent) {
    const n = values.length;
    const stdev = d3.deviation(values);

    if (!stdev || stdev === 0) {
      return Math.ceil(Math.log2(n) + 1); // Sturges' formula fallback
    }

    const binWidth = (3.5 * stdev) / Math.cbrt(n);
    const range = extent[1] - extent[0];
    return Math.max(1, Math.ceil(range / binWidth));
  }

  getDateBins(data, column) {
    const dates = data
      .map((d) => d[column])
      .filter((d) => d instanceof Date && !isNaN(d));

    if (dates.length === 0) return [];

    const extent = d3.extent(dates);
    const timeInterval = this.getTimeInterval();
    const thresholds = timeInterval.range(...extent);

    const bins = d3
      .bin()
      .domain(extent)
      .thresholds(thresholds)
      .value((d) => d)(dates);

    return bins.map((bin) => ({
      date: bin.x0,
      x0: bin.x0,
      x1: bin.x1,
      length: bin.length,
      count: bin.length,
      values: Array.from(bin),
    }));
  }

  getTimeInterval() {
    const intervals = {
      hour: d3.timeHour,
      day: d3.timeDay,
      week: d3.timeWeek,
      month: d3.timeMonth,
      year: d3.timeYear,
    };
    return intervals[this.config.dateInterval] || d3.timeDay;
  }

  getOrdinalBins(data, column) {
    const grouped = d3.group(data, (d) => d[column]);

    let bins = Array.from(grouped, ([key, values]) => ({
      key,
      x0: key,
      x1: key,
      length: values.length,
      count: values.length,
      values,
      ...this.getBinStats(values.map((d) => d[column])),
    }));

    if (bins.length > this.config.maxOrdinalBins) {
      bins = this.consolidateOrdinalBins(bins);
    }

    return bins.sort((a, b) => b.length - a.length);
  }

  consolidateOrdinalBins(bins) {
    const sorted = bins.sort((a, b) => b.length - a.length);
    const topBins = sorted.slice(0, this.config.maxOrdinalBins - 1);
    const otherBins = sorted.slice(this.config.maxOrdinalBins - 1);

    if (otherBins.length === 0) return topBins;

    const otherBin = {
      key: "Other",
      x0: "Other",
      x1: "Other",
      length: otherBins.reduce((sum, bin) => sum + bin.length, 0),
      count: otherBins.reduce((sum, bin) => sum + bin.length, 0),
      values: otherBins.flatMap((bin) => bin.values),
      originalCategories: otherBins.map((bin) => bin.key),
      ...this.getBinStats(
        otherBins.flatMap((bin) => bin.values.map((d) => d[column]))
      ),
    };

    return [...topBins, otherBin];
  }

  getBinStats(values) {
    if (!values?.length) return {};

    const numericValues = values.filter((v) => !isNaN(Number(v)));
    if (numericValues.length === 0) return { count: values.length };

    return {
      count: values.length,
      mean: d3.mean(numericValues),
      median: d3.median(numericValues),
      min: d3.min(numericValues),
      max: d3.max(numericValues),
    };
  }
}
