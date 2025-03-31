---
title: Testing Small MultiplesHistogram
toc: false
sidebar: false
footer: false
sql:
  oxford: ./../data/oxford_decarbonisation_data.parquet
---

# SmallMultiplesHistogram

```js
import { SmallMultiplesHistogram } from "./components/charts/small-multiples-histogram.js";
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
```

```js
// Example usage
const smallMultiples = new SmallMultiplesHistogram({
  width: 1200,
  height: 800,
  columns: [
    "lsoa",
    "msoa",
    "building_area",
    "garden_area",
    "ashp_suitability",
    "ashp_size",
    "ashp_total",
    "gshp_suitability",
    "gshp_size",
    "gshp_total",
  ],
  colors: ["steelblue", "orange"],
  selectionMode: "drag",
  showAxis: false,
  dataSource: oxBuildings,
  dataFormat: "json",
  showLabelsBelow: true,
  margin: { top: 40, right: 20, bottom: 40, left: 40 },
  gap: { horizontal: 30, vertical: 30 },
});

// Initialize and draw
await smallMultiples.initialize();
await smallMultiples.update();

display(smallMultiples.svg.node());
```

```js
// Listen for selection changes
smallMultiples.on("selectionChanged", (selectedData) => {
  console.log("Selected points:", selectedData);
  display(selectedData);
});
```

```js
// "Get all buildings where the building area is in the first percentile"
// "Show me the average floor count"
// "Find buildings with year built greater than 2000"
// "Show outliers in building area"
```
