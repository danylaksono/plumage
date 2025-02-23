export class FilterService {
  constructor(duckDBProcessor) {
    this.duckDBProcessor = duckDBProcessor;
  }

  async applyFilter(selectedData, sortColumn = null) {
    try {
      if (!selectedData || selectedData.length === 0) {
        return null;
      }

      // If we have a sort column, filter based on its unique values
      if (sortColumn) {
        const uniqueValues = [
          ...new Set(selectedData.map((row) => row[sortColumn])),
        ];
        if (uniqueValues.length > 0) {
          const escapedColumn = this.duckDBProcessor.safeColumnName(sortColumn);
          const valueList = uniqueValues
            .map((val) => {
              if (val === null || val === undefined) return "NULL";
              return `'${String(val).replace(/'/g, "''")}'`;
            })
            .join(", ");

          return `${escapedColumn} IN (${valueList})`;
        }
      }

      // If no sort column, use ROWIDs
      const rowIds = selectedData
        .map((row) => row.ROWID)
        .filter((id) => id != null);
      if (rowIds.length > 0) {
        return `ROWID IN (${rowIds.join(", ")})`;
      }

      return null;
    } catch (error) {
      console.error("Error building filter clause:", error);
      return null;
    }
  }

  async applyNumericFilter(column, range) {
    if (!range || !column) return null;

    try {
      const escapedColumn = this.duckDBProcessor.safeColumnName(column);
      return `${escapedColumn} BETWEEN ${range[0]} AND ${range[1]}`;
    } catch (error) {
      console.error("Error building numeric filter:", error);
      return null;
    }
  }

  async applyDateFilter(column, range) {
    if (!range || !column) return null;

    try {
      const escapedColumn = this.duckDBProcessor.safeColumnName(column);
      const start = range[0].toISOString();
      const end = range[1].toISOString();
      return `${escapedColumn} BETWEEN '${start}' AND '${end}'`;
    } catch (error) {
      console.error("Error building date filter:", error);
      return null;
    }
  }

  async applyOrdinalFilter(column, categories) {
    if (!categories || !column) return null;

    try {
      const escapedColumn = this.duckDBProcessor.safeColumnName(column);
      const valueList = categories
        .map((val) => `'${String(val).replace(/'/g, "''")}'`)
        .join(", ");
      return `${escapedColumn} IN (${valueList})`;
    } catch (error) {
      console.error("Error building ordinal filter:", error);
      return null;
    }
  }

  async combineFilters(filters) {
    // Remove any null filters
    const validFilters = filters.filter((f) => f !== null);

    if (validFilters.length === 0) {
      return "1=1"; // No filters, return true condition
    }

    return validFilters.join(" AND ");
  }
}
