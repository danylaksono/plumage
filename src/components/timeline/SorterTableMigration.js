export class SorterTableMigration {
  static async migrateHistogram(oldHistogram, duckDBProcessor, tableName) {
    // Create new histogram configuration
    const config = {
      column: oldHistogram.columnName,
      height: 60,
      width: 150,
      colors: ["steelblue", "orange"],
      dataProcessor: duckDBProcessor,
      tableName: tableName,
      selectionMode: oldHistogram.type === "continuous" ? "drag" : "click",
      showLabelsBelow: true,
      axis: false,
    };

    // Create new histogram instance
    const newHistogram = new Histogram(config);
    await newHistogram.initialize();

    // Copy over any existing selections
    if (oldHistogram.selectedBins && oldHistogram.selectedBins.size > 0) {
      newHistogram.selectedBins = new Set(oldHistogram.selectedBins);
      await newHistogram.drawBars();
    }

    return newHistogram;
  }

  static async migrateSelections(sorterTable) {
    const selectedRows = Array.from(sorterTable.selectedRows);
    if (selectedRows.length === 0) return;

    try {
      // Update histogram selections
      await Promise.all(
        sorterTable.visControllers.map(async (histogram, idx) => {
          if (!histogram) return;

          const columnName = sorterTable.columnManager.columns[idx].column;
          await histogram.highlightData(selectedRows);
        })
      );
    } catch (error) {
      console.error("Error migrating selections:", error);
    }
  }
}
