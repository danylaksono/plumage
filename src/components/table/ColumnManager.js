export class ColumnManager {
  constructor(
    columnTypes,
    data,
    binningService,
    maxOrdinalBins,
    continuousBinMethod
  ) {
    this.columns = Array.isArray(columnTypes)
      ? columnTypes.map((col) =>
          this.initializeColumn(
            col,
            data,
            binningService,
            maxOrdinalBins,
            continuousBinMethod
          )
        )
      : [];
  }

  initializeColumn(
    col,
    data,
    binningService,
    maxOrdinalBins,
    continuousBinMethod
  ) {
    // Ensure we have a proper column object
    const columnDef = typeof col === "string" ? { column: col } : { ...col };

    // Extract properties with defaults
    const colName = columnDef.column;
    const isUnique = columnDef.unique || false;
    const alias = columnDef.alias || null;
    let type = columnDef.type || null;

    if (!colName) {
      console.error("Invalid column definition:", columnDef);
      throw new Error("Column name is required");
    }

    // For unique columns, always use unique type
    if (isUnique) {
      type = "unique";
    }

    // Create base column definition
    const result = {
      column: colName,
      alias,
      unique: isUnique,
      type,
    };

    // Skip binning for unique columns
    if (!isUnique && binningService) {
      try {
        const binningResult = binningService.binColumn(
          data,
          result,
          maxOrdinalBins,
          continuousBinMethod
        );
        Object.assign(result, binningResult);
      } catch (error) {
        console.warn(`Failed to bin column ${colName}:`, error);
        // Continue without binning data
      }
    }

    return result;
  }

  setColumnType(columnName, type) {
    if (!columnName) {
      console.error("Invalid columnName:", columnName);
      return;
    }
    this.columnTypes[columnName] = type;
  }

  getColumnType(data, column) {
    // If column is marked as unique in configuration, return 'unique'
    const colConfig = this.columns.find((c) => c.column === column);
    if (colConfig && colConfig.unique) {
      return "unique";
    }

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
      const numValue = Number(value);
      if (!isNaN(numValue) && typeof value !== "string") {
        // Only treat as number if it's not a string representation
        const uniqueValues = new Set(data.map((d) => d[column])).size;
        if (uniqueValues > 10) {
          return "continuous";
        }
      }

      // Default to ordinal for strings and small number sets
      return "ordinal";
    }

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
