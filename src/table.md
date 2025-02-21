---
title: Testing sortableTable
toc: false
sidebar: false
footer: false
sql:
  oxford: ./../data/oxford_decarbonisation_data.parquet
---

# SorterTable

<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.2.0/css/all.min.css" />

```js
import { SorterTable } from "./components/table/index.js";
```

```sql id=oxford
  SELECT DISTINCT
    "UPRN" AS id,
    "LSOA code" AS lsoa,
    "MSOA code" AS msoa,
    "Air Source Heat Pump Potential_Building Size (m^2)" AS building_area,
    "Air Source Heat Pump Potential_Garden Area (m^2)" AS garden_area,
    "Air Source Heat Pump Potential_Overall Suitability Rating" AS ashp_suitability,
    "Air Source Heat Pump Potential_Recommended Heat Pump Size [kW]" AS ashp_size,
    "Low Carbon Technology Costs_Air Source Heat Pump - Labour" AS ashp_labour,
    "Low Carbon Technology Costs_Air Source Heat Pump - Material" AS ashp_material,
    "Low Carbon Technology Costs_Air Source Heat Pump - Total" AS ashp_total,
    "Domestic Ground Source Heat Pump Potential_Overall Suitability Rating" AS gshp_suitability,
    "Domestic Ground Source Heat Pump Potential_Recommended Heat Pump Size [kW]" AS gshp_size,
    "Low Carbon Technology Costs_Ground Source Heat Pump - Labour" AS gshp_labour,
    "Low Carbon Technology Costs_Ground Source Heat Pump - Materials" AS gshp_material,
    "Low Carbon Technology Costs_Ground Source Heat Pump - Total" AS gshp_total,
    "Domestic Heat Demand_Annual Heat Demand (kWh)" AS heat_demand,
    "Substation - Demand_rag" AS substation_demand
FROM oxford b;
```

```js
const oxBuildings = [...oxford];

// Example configuration for columns with proper data types
const columns2 = [
  { column: "id", unique: true, type: "ordinal" }, // ID column as ordinal
  { column: "lsoa", type: "ordinal" }, // Area codes as ordinal
  { column: "msoa", type: "ordinal" },
  { column: "building_area", type: "continuous", alias: "Building Size (m²)" },
  { column: "garden_area", type: "continuous", alias: "Garden Area (m²)" },
  { column: "ashp_suitability", type: "ordinal", alias: "ASHP Suitability" },
  { column: "ashp_size", type: "continuous", alias: "ASHP Size (kW)" },
  { column: "ashp_total", type: "continuous", alias: "ASHP Total Cost" },
  { column: "gshp_suitability", type: "ordinal", alias: "GSHP Suitability" },
  { column: "heat_demand", type: "continuous", alias: "Heat Demand (kWh)" },
  { column: "substation_demand", type: "ordinal", alias: "Substation Demand" },
];

// Example callback function
const myChangeCallback = (event) => {
  console.log("Table changed:", event);
};

// Initialize and display table
const initializeTable = async () => {
  try {
    // Create new table instance with explicit options
    const myTable = await new SorterTable(
      oxBuildings,
      columns2,
      myChangeCallback,
      {
        height: "600px",
        width: "100%",
        rowsPerPage: 50,
        maxOrdinalBins: 10, // Limit ordinal categories
        continuousBinMethod: "sturges", // Use Sturges' formula for continuous bins
      }
    );

    // Return the table node for display
    return myTable.getNode();
  } catch (error) {
    console.error("Failed to initialize table:", error);
    throw error; // Propagate error for better debugging
  }
};

// Call the initialization function
const tableElement = await initializeTable();
display(tableElement);
```
