export class ColumnManager {
  constructor(
    columnNames,
    data,
    binningService,
    maxOrdinalBins,
    continuousBinMethod
  ) {
    this.columnNames = columnNames;
    this.columnTypes = {};
    this.columns = columnNames.map((col) => {
      if (typeof col === "string") {
        return { column: col, unique: false };
      } else {
        return {
          column: col.column,
          alias: col.alias,
          unique: col.unique || false,
          type: col.type || null, // Add support for manual type definition
        };
      }
    });
    this.binningService = binningService;
    this.maxOrdinalBins = maxOrdinalBins;
    this.continuousBinMethod = continuousBinMethod;

    // Pre-populate column types if manually specified
    this.columns.forEach((col) => {
      if (col.type) {
        this.columnTypes[col.column] = col.type;
      }
    });
  }

  setColumnType(columnName, type) {
    if (!columnName) {
      console.error("Invalid columnName:", columnName);
      return;
    }
    this.columnTypes[columnName] = type;
  }

  getColumnType(data, column) {
    // If already cached, return the cached type
    if (this.columnTypes[column]) {
      return this.columnTypes[column];
    }

    // Infer type from data
    for (const d of data) {
      const value = d[column];
      if (value === undefined || value === null) continue;

      // Check for date objects
      if (value instanceof Date) return "date";

      // Check for numbers
      if (typeof value === "number" || !isNaN(Number(value))) {
        // Check if it's really continuous or just a few discrete values
        const uniqueValues = new Set(data.map((d) => d[column])).size;
        if (uniqueValues > 10) {
          // Threshold for considering it continuous
          return "continuous";
        }
      }

      // Default to ordinal for strings and small number sets
      return "ordinal";
    }

    // Default to ordinal if no clear type is found
    return "ordinal";
  }

  inferColumnTypesAndThresholds(data) {
    if (!this.binningService) {
      console.error("BinningService not initialized");
      return;
    }

    this.columns.forEach((colDef) => {
      const colName = colDef.column;
      const type = this.getColumnType(data, colName);
      colDef.type = type;
      this.setColumnType(colName, type);

      // threshold and binning for each type
      if (!colDef.unique) {
        try {
          // If the user has predefined thresholds, use them.
          if (
            colDef.thresholds &&
            Array.isArray(colDef.thresholds) &&
            colDef.thresholds.length
          ) {
            console.log(`Using predefined thresholds for ${colName}`);
          } else {
            // Otherwise, calculate them via binning service
            const bins = this.binningService.getBins(data, colName, type);

            if (!bins || bins.length === 0) {
              console.warn(`No bins generated for column: ${colName}`);
              return;
            }

            // For continuous data, use computed bin boundaries
            if (type === "continuous") {
              colDef.thresholds = bins.map((bin) =>
                bin.x0 !== undefined && bin.x0 !== null ? bin.x0 : null
              );
              colDef.bins = bins;
              console.log(
                "Setting thresholds for continuous column:",
                colName,
                colDef
              );
            } else if (type === "ordinal") {
              colDef.bins = bins;
              colDef.nominals = bins
                .map((bin) => bin.key)
                .filter((key) => key !== undefined && key !== null);
            } else if (type === "date") {
              colDef.bins = bins;
              colDef.dateRange = d3.extent(bins, (bin) => bin.date);
            }
          }
        } catch (error) {
          console.error(`Error binning column ${colName}:`, error);
        }
      }
    });
  }
}
