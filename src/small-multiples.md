# SmallMultiplesHistogram

```js
import { SmallMultiplesHistogram } from "./components/small-multiples-histogram.js";
```

```js
// Example usage
const smallMultiples = new SmallMultiplesHistogram({
  width: 600,
  height: 400,
  columns: ["age", "income", "score"],
  colors: ["steelblue", "orange"],
  selectionMode: "drag",
  dataSource: data,
  // showLabelsBelow: true,
  dataFormat: "json", // or "csv" or "parquet"
});

// Initialize and draw
// await smallMultiples.initialize();
await smallMultiples.update();

display(smallMultiples.svg.node());
```

```js
// // Listen for selection changes
// histogram.on("selectionChanged", (selectedData) => {
//   console.log("Selected points:", selectedData);
//   display(selectedData);
// });
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
