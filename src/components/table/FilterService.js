export class FilterService {
  constructor(data) {
    this.data = data;
    this.filters = [];
  }

  addFilter(filterFunction) {
    this.filters.push(filterFunction);
  }

  clearFilters() {
    this.filters = [];
  }

  applyFilters() {
    if (this.filters.length === 0) {
      return [...this.data]; // Return a copy of the original data
    }

    return this.data.filter((item) => {
      return this.filters.every((filter) => filter(item));
    });
  }
}
