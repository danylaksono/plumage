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

export { createDynamicFilter };
