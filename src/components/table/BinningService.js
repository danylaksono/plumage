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
    this.cacheBins = new Map();
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

  binColumn(
    data,
    columnDef,
    maxOrdinalBins = 20,
    continuousBinMethod = "sturges"
  ) {
    const columnName =
      typeof columnDef === "string" ? columnDef : columnDef.column;
    const type = columnDef.type || "ordinal";

    // Handle unique columns
    if (columnDef.unique) {
      return {
        type: "unique",
        bins: [
          {
            x0: null,
            x1: null,
            length: 0,
            key: "unique",
          },
        ],
      };
    }

    // For all other columns, return type and empty bins array
    // DuckDB will handle the actual binning
    return {
      type: type,
      bins: [],
    };
  }

  // Remove complex binning logic since DuckDB handles it
  detectType(data, columnName) {
    // This is now just a fallback, as DuckDB handles type detection
    const sampleValue = data.find((d) => d[columnName] != null)?.[columnName];

    if (!sampleValue) return "ordinal";
    if (sampleValue instanceof Date) return "date";
    if (typeof sampleValue === "number") return "continuous";
    if (typeof sampleValue === "string" && !isNaN(parseFloat(sampleValue)))
      return "continuous";

    return "ordinal";
  }

  createBins(data, binrules) {
    if (!data || !Array.isArray(data) || data.length === 0) {
      console.warn("No valid data provided to createBins");
      return [];
    }

    // Filter out null/undefined values
    const validData = data.filter((d) => d !== null && d !== undefined);
    if (validData.length === 0) {
      return [];
    }

    if (binrules.unique) {
      // Handle unique values case
      return [{ length: validData.length, values: validData }];
    }

    // Handle continuous data
    if (binrules.thresholds || typeof validData[0] === "number") {
      const thresholds =
        binrules.thresholds || this.generateThresholds(validData);
      const histogram = d3
        .bin()
        .domain([d3.min(validData), d3.max(validData)])
        .thresholds(thresholds);

      const bins = histogram(validData);
      return bins.map((bin) => ({
        x0: bin.x0,
        x1: bin.x1,
        length: bin.length,
        values: bin,
        indeces: bin.map((v) => data.indexOf(v)).filter((i) => i !== -1),
      }));
    }

    // Handle ordinal/nominal data
    const valueGroups = d3.group(validData);
    const bins = Array.from(valueGroups, ([key, values]) => ({
      key: key,
      length: values.length,
      values: values,
      indeces: values.map((v) => data.indexOf(v)).filter((i) => i !== -1),
    }));

    // Sort bins by frequency if not continuous
    return bins.sort((a, b) => b.length - a.length);
  }

  generateThresholds(data) {
    const min = d3.min(data);
    const max = d3.max(data);
    const range = max - min;
    const binCount = Math.min(20, Math.ceil(Math.sqrt(data.length)));
    return d3.range(min, max + range / binCount, range / binCount);
  }

  async binData(data, column, type, maxBins = 20) {
    const values = data.map(d => d[column]).filter(d => d != null);
    
    console.log('Binning data:', {
      column,
      type,
      sampleValues: values.slice(0, 5),
      totalValues: values.length
    });
    
    if (type === 'continuous') {
      return this.binContinuousData(values, maxBins);
    } else {
      return this.binOrdinalData(values, maxBins);
    }
  }

  binContinuousData(values, maxBins) {
    const extent = d3.extent(values);
    const generator = d3.bin()
      .domain(extent)
      .thresholds(maxBins);

    const rawBins = generator(values);
    
    // Transform bins to store original values
    const bins = rawBins.map(bin => ({
      x0: bin.x0,
      x1: bin.x1,
      count: bin.length,
      values: Array.from(bin),  // Store original values for selection
      originalValues: Array.from(bin)  // Keep a separate copy
    }));

    console.log('Created continuous bins:', {
      binCount: bins.length,
      sampleBin: {
        range: [bins[0]?.x0, bins[0]?.x1],
        count: bins[0]?.count,
        sampleValues: bins[0]?.values.slice(0, 3)
      }
    });

    return bins;
  }

  binOrdinalData(values, maxBins) {
    // Create a map to preserve original values
    const valueGroups = new Map();
    values.forEach(value => {
      const key = String(value);
      if (!valueGroups.has(key)) {
        valueGroups.set(key, {
          key: value,  // Keep original value
          count: 0,
          values: []
        });
      }
      valueGroups.get(key).count++;
      valueGroups.get(key).values.push(value);
    });

    // Convert to array and sort by count
    let bins = Array.from(valueGroups.values())
      .sort((a, b) => b.count - a.count);

    // Handle maxBins limit
    if (bins.length > maxBins) {
      const otherBin = bins.slice(maxBins - 1).reduce(
        (acc, bin) => ({
          key: 'Other',
          count: acc.count + bin.count,
          values: [...acc.values, ...bin.values]
        }),
        { key: 'Other', count: 0, values: [] }
      );
      bins = [...bins.slice(0, maxBins - 1), otherBin];
    }

    console.log('Created ordinal bins:', {
      binCount: bins.length,
      sampleBin: {
        key: bins[0]?.key,
        count: bins[0]?.count,
        sampleValues: bins[0]?.values.slice(0, 3)
      }
    });

    return bins;
  }

  async binDataWithDuckDB(data, column, type, maxBins = 20) {
    // First try to get from cache
    const cacheKey = `${column}-${type}-${maxBins}`;
    if (this.cacheBins.has(cacheKey)) {
      return this.cacheBins.get(cacheKey);
    }

    const bins = await this.binData(data, column, type, maxBins);
    this.cacheBins.set(cacheKey, bins);
    return bins;
  }
}
