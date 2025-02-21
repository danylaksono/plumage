# SorterTable

```js
import { SorterTable } from "./components/table/index.js";
```

```js
// Example usage
const myColumns = ["income", "age", "city"];
const myChangeCallback = (event) => {
  console.log("Table changed:", event);
};

// Initialize and display table
const initializeTable = async () => {
  try {
    // Create new table instance - note the await
    const myTable = await new SorterTable(data, myColumns, myChangeCallback);

    // Now we can call getNode() on the initialized instance
    const tableNode = myTable.getNode();

    // Add to the DOM - use your preferred method
    // For Observable notebooks:
    return tableNode;

    // For regular web pages:
    // document.getElementById('table-container').appendChild(tableNode);
  } catch (error) {
    console.error("Failed to initialize table:", error);
  }
};

// Call the initialization function
const tableElement = await initializeTable();
display(tableElement);
```

## API Reference

### Initialization

The table must be initialized asynchronously:

```js
const myTable = await new SorterTable(
  data,          // Array<any> | string | File - your data source
  columnNames,   // string[] - array of column names
  changeCallback // (event: TableEvent) => void - callback for table changes
  options        // Optional configuration object
);
```

### Displaying the Table

After initialization, get the table's DOM node and add it to your page:

```js
const tableNode = myTable.getNode();
// Add to your page using your preferred method:
// container.appendChild(tableNode);
// Or for Observable notebooks:
display(tableNode);
```

### Options

```ts
interface TableOptions {
  height?: string; // Container height (default: "400px")
  width?: string; // Container width (default: "100%")
  rowsPerPage?: number; // Number of rows per page (default: 50)
  loadMoreThreshold?: number; // Scroll threshold for loading more rows (default: 100)
  maxOrdinalBins?: number; // Maximum number of bins for ordinal data
  continuousBinMethod?: string; // Binning method for continuous data
  cellRenderers?: { [key: string]: (value: any, row: any) => HTMLElement }; // Custom cell renderers
}
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
