---
title: Testing sortableTable
toc: false
sidebar: false
footer: false
sql:
  oxford: ./../data/oxford_decarbonisation_data.parquet
---

```js
// import { createTable } from "./../components/sorterTable.js";
import { sorterTable } from "./components/sorttable/sorterTableClass.js";
```

# Table Experiments 2

```sql id=oxford_data
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
    "Domestic Insulation Potential_EPC Rating" AS epc_rating,
    "Domestic Insulation Potential_Insulation - Cavity Wall" AS insulation_cwall,
    "Low Carbon Technology Costs_Insulation - Cavity Wall - Labour" AS insulation_cwall_labour,
    "Low Carbon Technology Costs_Insulation - Cavity Wall  - Materials" AS insulation_cwall_materials,
    "Low Carbon Technology Costs_Insulation - Cavity Wall - Total" AS insulation_cwall_total,
    "Domestic Insulation Potential_Insulation - External Wall" AS insulation_ewall,
    "Low Carbon Technology Costs_Insulation - External Wall - Labour" AS insulation_ewall_labour,
    "Low Carbon Technology Costs_Insulation - External Wall - Material" AS insulation_ewall_materials,
    "Low Carbon Technology Costs_Insulation - External Wall - Total" AS insulation_ewall_total,
    "Domestic Insulation Potential_Insulation - Roof" AS insulation_roof,
    "Low Carbon Technology Costs_Insulation - Loft - Labour" AS insulation_roof_labour,
    "Low Carbon Technology Costs_Insulation - Loft - Material" AS insulation_roof_materials,
    "Low Carbon Technology Costs_Insulation - Loft - Total" AS insulation_roof_total,
    "Domestic Insulation Potential_Insulation - Under Floor" AS insulation_floor,
    "Low Carbon Technology Costs_Insulation - Under Floor - Labour" AS insulation_floor_labour,
    "Low Carbon Technology Costs_Insulation - Under Floor - Material" AS insulation_floor_materials,
    "Low Carbon Technology Costs_Insulation - Under Floor- Total" AS insulation_floor_total,
    "Domestic PV Potential_Overall Suitability" AS pv_suitability,
    "Domestic PV Potential_Recommended Array Size [kW]" AS pv_size,
    "Domestic PV Potential_Annual Generation [kWh]" AS pv_generation,
    "Low Carbon Technology Costs_Rooftop PV - Labour" AS pv_labour,
    "Low Carbon Technology Costs_Rooftop PV - Materials" AS pv_material,
    "Low Carbon Technology Costs_Rooftop PV - Total" AS pv_total,
    "Substation Name" AS substation_name,
    "Substation - CapacityRating" AS substation_capacity_rating,
    "Substation - Peakload" AS substation_peakload,
    "Substation - Headroom" AS substation_headroom,
    "Substation - % headroom" AS substation_headroom_pct,
    "Substation - Demand_rag" AS substation_demand
FROM oxford b;
-- WHERE substation_peakload >= 500 AND pv_generation is not null;
```

```js
const oxBuildings = [...oxford_data];
```

```js
function useState(value) {
  const state = Mutable(value);
  const setState = (value) => (state.value = value);
  return [state, setState];
}
const [selected, setSelected] = useState({});
```

```js
const columns2 = [
  { column: "id", unique: true },
  "lsoa",
  "epc_rating",
  "insulation_ewall",
  { column: "pv_generation", alias: "PV Generation" },
  "ashp_size",
  "substation_demand",
  "gshp_total",
  "gshp_suitability",
  { column: "heat_demand", alias: "Heat Demand" },
];
```

```js
function createDynamicFilter(attribute, operator, threshold) {
  // Validate attribute
  if (typeof attribute !== "string" || attribute.trim() === "") {
    throw new Error("Invalid attribute: Attribute must be a non-empty string.");
  }

  // Validate operator
  const validOperators = [">", ">=", "<", "<=", "==", "!="];
  if (!validOperators.includes(operator)) {
    throw new Error(
      `Invalid operator: Supported operators are ${validOperators.join(", ")}.`
    );
  }

  // Validate threshold
  if (typeof threshold !== "number" && typeof threshold !== "string") {
    throw new Error(
      "Invalid threshold: Threshold must be a number or a string."
    );
  }

  // Return the filter function
  return (dataObj) => {
    // Use the passed data object directly
    const value = dataObj[attribute];

    if (value === undefined) {
      console.warn(`Attribute "${attribute}" not found in data object.`);
      return false; // Exclude data objects missing the attribute
    }

    // Perform comparison
    try {
      switch (operator) {
        case ">":
          return value > threshold;
        case ">=":
          return value >= threshold;
        case "<":
          return value < threshold;
        case "<=":
          return value <= threshold;
        case "==":
          return value == threshold; // Consider using === for strict equality
        case "!=":
          return value != threshold; // Consider using !== for strict inequality
        default:
          throw new Error(`Unexpected operator: ${operator}`);
      }
    } catch (error) {
      console.error(
        `Error evaluating filter: ${attribute} ${operator} ${threshold} - ${error.message}`
      );
      return false;
    }
  };
}
```

```js
const userAttribute = "insulation_ewall";
const userOperator = "==";
const userThreshold = "Uninsulated";

const customFilter = createDynamicFilter(
  userAttribute,
  userOperator,
  userThreshold
);

table2.applyCustomFilter(customFilter);
```

```js
const table2 = new sorterTable(oxBuildings, columns2, tableChanged, {
  containerWidth: 300,
});
```

```js
display(table2.getNode());
```

```js
html`Selected`;
display(selected);
```

<!--
```js
// const sortButton = document.createElement("button");
// sortButton.textContent = "Sort by LSOA (Descending)";
// // document.body.appendChild(sortButton);

// sortButton.addEventListener("click", () => {
//   const ageSortCtrl = table.sortControllers.find(
//     (ctrl) => ctrl.getColumn() === "lsoa"
//   );
//   ageSortCtrl.setDirection("down");
//   table.sortChanged(ageSortCtrl);
// });
```

```js
// Create a color scale (example)
const colorScale = d3
  .scaleSequential(d3.interpolateViridis)
  .domain([0, d3.max(buildings, (d) => d.pv_generation)]);
// display(colorScale(1000));
```

```js
// Define cell renderers
const cellRenderers = {
  pv_generation: (value, rowData) => {
    const max = d3.max(buildings, (d) => d.pv_generation); // Calculate max dynamically
    return sparkbar(max, colorScale)(value, rowData); // Call sparkbar with calculated max
  },
  ashp_size: (data) => {
    const span = document.createElement("span");
    span.innerText = data >= 180 ? "More" : "Less";
    return span;
  },
};
```

```js
function sparkbar(max, colorScale, alpha = 0.6) {
  return (x, rowData) => {
    // console.log("Rendering sparkbar for:", x, rowData);
    // Now takes 'x' (cell value) and 'rowData' (entire row)
    const color = d3.color(colorScale(x));
    color.opacity = alpha;
    const div = document.createElement("div");
    div.style.background = color;
    div.style.width = `${(100 * x) / max}%`;
    div.style.float = "right";
    div.style.paddingRight = "3px";
    div.style.boxSizing = "border-box";
    div.style.overflow = "visible";
    div.style.display = "flex";
    div.style.justifyContent = "end";
    div.innerText = x.toLocaleString("en");
    return div;
  };
}
``` -->

<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.2.0/css/all.min.css" />

<style>
table {
  font-family: Roboto, Helvetica, sans-serif;
  font-size: 10px;
  border-collapse: collapse;
  width: 100%;
}

.sorter-table tbody tr:nth-child(even) {
  background-color: #f8f8f8; /* Zebra striping */
}

.sorter-table tbody tr:hover {
  background-color: #f0f0f0; /* Highlight on hover */
}

.sorter-table thead {
  background-color: #e0e0e0; /* Header background */
}

.sorter-table th {
  text-align: left;
  padding: 8px;
  font-size: 10px;
  border-bottom: 2px solid #ddd;
}

.sorter-table td {
  padding: 8px;
  border: 1px solid #ddd;
  text-align: left;
  vertical-align: middle;
}


.sorter-table .sidebar { /* Target the sidebar using the table's class */
  display: flex;
  flex-direction: column;
  align-items: center;
  width: 50px; /* Adjust as needed */
  padding: 10px;
  border-right: 1px solid #ccc;
}

.sorter-table .sidebar i { /* Target icons within the sidebar */
  margin-bottom: 15px;
  cursor: pointer;
  color: gray;
}

.sorter-table .sidebar i:hover {
  color: black; /* Highlight on hover */
} 

/* .unique-values-text {
  font-size: 12px; /* Adjust font size to match histogram labels */
  color: #666; /* Adjust color to match histogram text */
  text-align: center;
  width: 100%; /* Ensure the span takes up the full width of the cell */
  display: block; /* Make the span a block element */
} */
</style>

```js
function tableChanged(event) {
  console.log("Table changed:", event);

  if (event.type === "filter") {
    console.log("Filtered indices:", event);
    console.log("Filter rule:", event.rule);
  }
  if (event.type === "columnSelection") {
    console.log("Selected column:", event.selectedColumn);
    // ... do something with the selected column information ...
  }

  if (event.type === "sort") {
    console.log("Sorted indices:", event);
    console.log("Sort criteria:", event.sort);
  }

  if (event.type === "selection") {
    console.log("Selected rows:", event.selection);
    setSelected(event.selection);
    console.log("Selection rule:", event.rule);
  }
}
```

## Renderers Demo

<!--
```js
// --- Dummy Data ---
const buildings = [
  {
    id: "A1",
    lsoa: "E01000001",
    insulation_rating: "C",
    insulation_ewall: "Good",
    pv_generation: 3500,
    ashp_size: 120,
    substation_demand: 45.5,
    history: [
      { date: new Date("2023-01-01"), value: 10 },
      { date: new Date("2023-02-01"), value: 15 },
      { date: new Date("2023-03-01"), value: 8 },
      { date: new Date("2023-04-01"), value: 12 },
      { date: new Date("2023-05-01"), value: 18 },
    ],
  },
  {
    id: "A2",
    lsoa: "E01000002",
    insulation_rating: "B",
    insulation_ewall: "Very Good",
    pv_generation: 8100,
    ashp_size: 200,
    substation_demand: 60.2,
    history: [
      { date: new Date("2023-01-01"), value: 22 },
      { date: new Date("2023-02-01"), value: 18 },
      { date: new Date("2023-03-01"), value: 25 },
      { date: new Date("2023-04-01"), value: 20 },
      { date: new Date("2023-05-01"), value: 15 },
    ],
  },
  {
    id: "A3",
    lsoa: "E01000003",
    insulation_rating: "D",
    insulation_ewall: "Average",
    pv_generation: 1500,
    ashp_size: 90,
    substation_demand: 38.0,
    history: [
      { date: new Date("2023-01-01"), value: 5 },
      { date: new Date("2023-02-01"), value: 8 },
      { date: new Date("2023-03-01"), value: 6 },
      { date: new Date("2023-04-01"), value: 9 },
      { date: new Date("2023-05-01"), value: 7 },
    ],
  },
  {
    id: "A4",
    lsoa: "E01000004",
    insulation_rating: "A",
    insulation_ewall: "Excellent",
    pv_generation: 12500,
    ashp_size: 350,
    substation_demand: 75.8,
    history: [
      { date: new Date("2023-01-01"), value: 30 },
      { date: new Date("2023-02-01"), value: 33 },
      { date: new Date("2023-03-01"), value: 28 },
      { date: new Date("2023-04-01"), value: 35 },
      { date: new Date("2023-05-01"), value: 29 },
    ],
  },
  {
    id: "A5",
    lsoa: "E01000005",
    insulation_rating: "E",
    insulation_ewall: "Poor",
    pv_generation: 500,
    ashp_size: 50,
    substation_demand: 25.3,
    history: [
      { date: new Date("2023-01-01"), value: 12 },
      { date: new Date("2023-02-01"), value: 10 },
      { date: new Date("2023-03-01"), value: 14 },
      { date: new Date("2023-04-01"), value: 9 },
      { date: new Date("2023-05-01"), value: 11 },
    ],
  },
];

// --- Column Definitions ---
const columnNames = [
  { column: "id", unique: true },
  "lsoa",
  { column: "insulation_rating", alias: "Insulation Rating" },
  { column: "insulation_ewall", alias: "Wall Insulation" },
  {
    column: "pv_generation",
    alias: "PV Generation",
    thresholds: [0, 5000, 10000, 20000, 50000],
  },
  { column: "ashp_size", alias: "ASHP Size" },
  { column: "substation_demand", alias: "Substation Demand" },
  { column: "history", alias: "History" }, // For sparkline
];
```

```js
// --- Color Scale ---
const colorScale = d3
  .scaleSequential(d3.interpolateGreens)
  .domain([0, d3.max(buildings, (d) => d.pv_generation)]);
```

```js
// --- Custom Renderer Functions ---
function sparkbar(max, colorScale, alpha = 0.6) {
  return (x, rowData) => {
    const color = "blue"; // d3.color(colorScale(x));
    // color.opacity = alpha;
    const div = document.createElement("div");
    div.style.background = color;
    div.style.width = `${(100 * x) / max}%`;
    div.style.height = "10px"; // Ensure visibility
    // div.style.float = "right";
    div.style.boxSizing = "border-box";
    div.style.overflow = "hidden";
    div.style.display = "block";
    div.title = x.toLocaleString("en"); // Tooltip text
    return div;
  };
}

function sparkarea(data, rowData, options = {}) {
  const {
    width = 240,
    height = 20,
    fillColor = "#faa",
    strokeColor = "red",
  } = options;

  // Extract x and y values from the data
  const X = data.map((d) => d.date); // Assuming 'date' property for x-axis
  const Y = data.map((d) => d.value); // Assuming 'value' property for y-axis

  // Handle cases where there is not enough data for an area chart
  if (X.length < 2 || Y.length < 2) {
    const message = document.createElement("div");
    message.innerText = "Not enough data";
    return message;
  }

  const x = d3
    .scaleTime() // Use scaleTime for dates
    .domain(d3.extent(X))
    .range([0, width]);
  const y = d3.scaleLinear().domain(d3.extent(Y)).range([height, 0]);

  const area = d3
    .area()
    .x((d, i) => x(X[i]))
    .y1((d, i) => y(Y[i]))
    .y0(height)
    .defined((d, i) => !isNaN(X[i]) && !isNaN(Y[i]));

  // Create the SVG element
  const svg = d3
    .create("svg")
    .attr("width", width)
    .attr("height", height)
    .style("vertical-align", "middle")
    .style("margin", "-3px 0")
    .node(); // Get the actual SVG element

  // Add the area path
  d3.select(svg).append("path").attr("fill", fillColor).attr("d", area(data));

  // Add the outline path
  d3.select(svg)
    .append("path")
    .attr("fill", "none")
    .attr("stroke", strokeColor)
    .attr("d", area.lineY1()(data));

  // Create a container div
  const container = document.createElement("div");
  container.appendChild(svg);

  return container;
}

function ratingRenderer(rating, rowData, maxRating = 5) {
  const fullStar = "★";
  const emptyStar = "☆";
  const container = document.createElement("div");

  for (let i = 0; i < maxRating; i++) {
    const star = document.createElement("span");
    star.innerText = i < rating ? fullStar : emptyStar;
    star.style.color = "gold";
    star.style.fontSize = "16px";
    container.appendChild(star);
  }

  return container;
}
```

```js
// --- Cell Renderers ---
const cellRenderers = {
  pv_generation: (value, rowData) => {
    const max = d3.max(buildings, (d) => d.pv_generation);
    return sparkbar(max, colorScale)(value, rowData);
  },
  ashp_size: (data) => {
    const span = document.createElement("span");
    span.innerText = data >= 180 ? "Large" : "Small";
    return span;
  },
  history: (data, rowData) => sparkarea(data, rowData),
  insulation_rating: (rating) => {
    const ratingNumber = { A: 5, B: 4, C: 3, D: 2, E: 1, Unknown: 0 }; // Example mapping
    return ratingRenderer(ratingNumber[rating], {}); // Use {} if rowData is not needed
  },
};
```


```js
// --- Create sorterTable Instance ---
const table = new sorterTable(buildings, columnNames, tableChanged, {
  cellRenderers,
  defaultLines: 5, // Show 5 rows initially
  additionalLines: 5, // Load 5 more rows on scroll
});
display(table.getNode());
``` -->
