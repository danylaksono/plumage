# PlumageVis

This is an [Observable Framework](https://observablehq.com/framework/) app. To install the required dependencies, run:

```
npm install
```

Then, to start the local preview server, run:

```
npm run dev
```

Then visit <http://localhost:3000> to preview your app.

For more, see <https://observablehq.com/framework/getting-started>.

## Project structure

A typical Framework project looks like this:

```ini
.
├─ src
│  ├─ components
│  │  └─ timeline.js           # an importable module
│  ├─ data
│  │  ├─ launches.csv.js       # a data loader
│  │  └─ events.json           # a static data file
│  ├─ example-dashboard.md     # a page
│  ├─ example-report.md        # another page
│  └─ index.md                 # the home page
├─ .gitignore
├─ observablehq.config.js      # the app config file
├─ package.json
└─ README.md
```

**`src`** - This is the “source root” — where your source files live. Pages go here. Each page is a Markdown file. Observable Framework uses [file-based routing](https://observablehq.com/framework/project-structure#routing), which means that the name of the file controls where the page is served. You can create as many pages as you like. Use folders to organize your pages.

**`src/index.md`** - This is the home page for your app. You can have as many additional pages as you’d like, but you should always have a home page, too.

**`src/data`** - You can put [data loaders](https://observablehq.com/framework/data-loaders) or static data files anywhere in your source root, but we recommend putting them here.

**`src/components`** - You can put shared [JavaScript modules](https://observablehq.com/framework/imports) anywhere in your source root, but we recommend putting them here. This helps you pull code out of Markdown files and into JavaScript modules, making it easier to reuse code across pages, write tests and run linters, and even share code with vanilla web applications.

**`observablehq.config.js`** - This is the [app configuration](https://observablehq.com/framework/config) file, such as the pages and sections in the sidebar navigation, and the app’s title.

## Command reference

| Command              | Description                                 |
| -------------------- | ------------------------------------------- |
| `npm install`        | Install or reinstall dependencies           |
| `npm run dev`        | Start local preview server                  |
| `npm run build`      | Build your static site, generating `./dist` |
| `npm run deploy`     | Deploy your app to Observable               |
| `npm run clean`      | Clear the local data loader cache           |
| `npm run observable` | Run commands like `observable help`         |

## API Reference

### Initialization

The table must be initialized asynchronously:

```js
// const myTable = await new SorterTable(
//   data,          // Array<any> | string | File - your data source
//   columnNames,   // string[] - array of column names
//   changeCallback // (event: TableEvent) => void - callback for table changes
//   options        // Optional configuration object
// );
```

### Displaying the Table

After initialization, get the table's DOM node and add it to your page:

```js
// const tableNode = myTable.getNode();
// Add to your page using your preferred method:
// container.appendChild(tableNode);
// Or for Observable notebooks:
// display(tableNode);
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
