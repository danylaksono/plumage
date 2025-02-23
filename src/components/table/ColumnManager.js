export class ColumnManager {
  constructor(columnTypes) {
    this.columns = Array.isArray(columnTypes)
      ? columnTypes.map((col) => this.initializeColumn(col))
      : [];
  }

  initializeColumn(col) {
    // Ensure we have a proper column object
    const columnDef = typeof col === "string" ? { column: col } : { ...col };

    // Extract properties with defaults
    const colName = columnDef.column;
    const isUnique = columnDef.unique || false;
    const alias = columnDef.alias || null;
    const type = columnDef.type || "ordinal"; // Default type if not specified

    if (!colName) {
      console.error("Invalid column definition:", columnDef);
      throw new Error("Column name is required");
    }

    // Create column definition
    return {
      column: colName,
      alias,
      unique: isUnique,
      type: isUnique ? "unique" : type,
    };
  }

  setColumnType(columnName, type) {
    const column = this.columns.find((c) => c.column === columnName);
    if (column) {
      column.type = type;
    }
  }

  getColumnType(columnName) {
    const column = this.columns.find((c) => c.column === columnName);
    return column ? column.type : "ordinal";
  }

  // Convert DuckDB types to our visualization types
  mapDuckDBType(duckdbType) {
    const type = duckdbType.toLowerCase();
    if (type.includes("varchar") || type.includes("text")) {
      return "ordinal";
    }
    if (type.includes("timestamp") || type.includes("date")) {
      return "date";
    }
    if (
      type.includes("int") ||
      type.includes("decimal") ||
      type.includes("float") ||
      type.includes("double")
    ) {
      return "continuous";
    }
    return "ordinal";
  }

  // Get column configuration
  getColumnConfig(columnName) {
    return this.columns.find((c) => c.column === columnName) || null;
  }

  // Check if a column exists
  hasColumn(columnName) {
    return this.columns.some((c) => c.column === columnName);
  }

  // Get all columns
  getAllColumns() {
    return [...this.columns];
  }

  // Get columns of a specific type
  getColumnsByType(type) {
    return this.columns.filter((c) => c.type === type);
  }
}
