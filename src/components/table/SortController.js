function SortController(colName, update) {
  let active = false;

  let controller = this;
  let div = document.createElement("div");
  div.style.width = "24px";
  div.style.height = "24px";
  div.style.margin = "0 auto";
  div.style.cursor = "pointer";

  // Use Font Awesome icon
  const icon = document.createElement("i");
  icon.classList.add("fas", "fa-sort"); // Initial sort icon
  icon.style.color = "gray";
  icon.style.fontSize = "12px"; // Adjust icon size if needed
  div.appendChild(icon);

  let sorting = "none";

  // Toggle function
  this.toggleDirection = () => {
    if (sorting === "none" || sorting === "down") {
      sorting = "up";
      icon.classList.remove("fa-sort-down", "fa-sort");
      icon.classList.add("fa-sort-up");
    } else {
      sorting = "down";
      icon.classList.remove("fa-sort-up", "fa-sort");
      icon.classList.add("fa-sort-down");
    }
  };

  this.getDirection = () => sorting;

  this.getColumn = () => colName;

  this.getNode = () => div;

  // Prevent click propagation from the icon
  div.addEventListener("click", (event) => {
    event.stopPropagation();
    active = !active;
    controller.toggleDirection();
    update(controller);

    // Visual feedback
    icon.style.color = active ? "#2196F3" : "gray";
  });

  return this;
}

export { SortController };
