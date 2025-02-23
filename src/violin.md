# Violin Plot

```js
import { ViolinPlot } from "./components/charts/violin-plot.js";
```

<style>
  .violin-tooltip {
  background: rgba(255, 255, 255, 0.95);
  border-radius: 4px;
  box-shadow: 0 2px 4px rgba(0,0,0,0.1);
  font-family: -apple-system, system-ui, sans-serif;
  font-size: 12px;
  pointer-events: none;
  transition: opacity 0.2s ease;
}

.violin {
  transition: fill 0.2s ease, opacity 0.2s ease;
}

.brush .selection {
  stroke: #2c5282;
  stroke-width: 1.5px;
  fill-opacity: 0.3;
}
</style>

```js
// Example usage
const violinPlot = new ViolinPlot({
  width: 400,
  height: 400,
  column: "income",
  colors: ["#00c194"], // improved color scheme
  selectionMode: "drag", // added drag interactivity
  showAxis: true, // enable axis
  showTooltip: true, // (optional) enable tooltip
  dataSource: data,
  showLabelsBelow: false,
  showMedian: true,
  showQuartiles: true,
});

// Initialize and draw
await violinPlot.initialize();
await violinPlot.update();

await violinPlot.highlightData([1, 2, 3, 5, 7]);
// await violinPlot.highlightDataByValue([30000, 40000, 78000]);

display(violinPlot.svg.node());
```

```js
// Listen for selection changes
violinPlot.on("selectionChanged", (selectedData) => {
  console.log("Selected points:", selectedData);
  display(selectedData);
});
```

```js
const data = [
  {
    age: 25,
    date: new Date(2023, 0, 1),
    category: "A",
    income: 30000,
    score: 78,
    city: "London",
    gender: "M",
  },
  {
    age: 30,
    date: new Date(2023, 1, 15),
    category: "B",
    income: 40000,
    score: 85,
    city: "Manchester",
    gender: "F",
  },
  {
    age: 35,
    date: new Date(2023, 2, 10),
    category: "A",
    income: 50000,
    score: 92,
    city: "Birmingham",
    gender: "M",
  },
  {
    age: 40,
    date: new Date(2023, 3, 5),
    category: "C",
    income: 60000,
    score: 88,
    city: "Liverpool",
    gender: "F",
  },
  {
    age: 45,
    date: new Date(2023, 4, 20),
    category: "B",
    income: 55000,
    score: 82,
    city: "Leeds",
    gender: "M",
  },
  {
    age: 50,
    date: new Date(2023, 5, 25),
    category: "A",
    income: 65000,
    score: 90,
    city: "Sheffield",
    gender: "F",
  },
  {
    age: 55,
    date: new Date(2023, 6, 30),
    category: "C",
    income: 70000,
    score: 75,
    city: "Bristol",
    gender: "M",
  },
  {
    age: 60,
    date: new Date(2023, 7, 15),
    category: "B",
    income: 80000,
    score: 95,
    city: "Nottingham",
    gender: "F",
  },
  {
    age: 65,
    date: new Date(2023, 8, 10),
    category: "A",
    income: 75000,
    score: 89,
    city: "Leicester",
    gender: "M",
  },
  {
    age: 70,
    date: new Date(2023, 9, 5),
    category: "C",
    income: 85000,
    score: 80,
    city: "Glasgow",
    gender: "F",
  },
  {
    age: 28,
    date: new Date(2023, 10, 10),
    category: "B",
    income: 42000,
    score: 83,
    city: "Edinburgh",
    gender: "M",
  },
  {
    age: 33,
    date: new Date(2023, 11, 15),
    category: "A",
    income: 47000,
    score: 86,
    city: "Cardiff",
    gender: "F",
  },
  {
    age: 38,
    date: new Date(2024, 0, 5),
    category: "C",
    income: 52000,
    score: 79,
    city: "Belfast",
    gender: "M",
  },
  {
    age: 43,
    date: new Date(2024, 1, 20),
    category: "B",
    income: 58000,
    score: 88,
    city: "Aberdeen",
    gender: "F",
  },
  {
    age: 48,
    date: new Date(2024, 2, 25),
    category: "A",
    income: 62000,
    score: 91,
    city: "Swansea",
    gender: "M",
  },
  {
    age: 53,
    date: new Date(2024, 3, 30),
    category: "C",
    income: 68000,
    score: 77,
    city: "Oxford",
    gender: "F",
  },
  {
    age: 58,
    date: new Date(2024, 4, 15),
    category: "B",
    income: 73000,
    score: 94,
    city: "Cambridge",
    gender: "M",
  },
  {
    age: 63,
    date: new Date(2024, 5, 10),
    category: "A",
    income: 79000,
    score: 87,
    city: "Bath",
    gender: "F",
  },
  {
    age: 68,
    date: new Date(2024, 6, 5),
    category: "C",
    income: 83000,
    score: 81,
    city: "York",
    gender: "M",
  },
];
```
