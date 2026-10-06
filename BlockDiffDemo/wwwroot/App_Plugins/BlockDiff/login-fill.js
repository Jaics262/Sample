(function fillDemoLogin() {
  const email = "admin@example.com";
  const password = "BlockDiff-demo-1";

  function inputs(node, found) {
    if (!node || !node.querySelectorAll) {
      return;
    }

    node.querySelectorAll("input").forEach((input) => found.push(input));
    node.querySelectorAll("*").forEach((element) => {
      if (element.shadowRoot) {
        inputs(element.shadowRoot, found);
      }
    });
  }

  function setValue(input, value) {
    const descriptor = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value");
    descriptor.set.call(input, value);
    input.dispatchEvent(new Event("input", { bubbles: true }));
    input.dispatchEvent(new Event("change", { bubbles: true }));
  }

  const found = [];
  inputs(document, found);
  const username = found.find((input) => input.name === "username" || input.type === "email" || input.autocomplete === "username");
  const secret = found.find((input) => input.type === "password" || input.name === "password");
  if (!username || !secret) {
    window.setTimeout(fillDemoLogin, 200);
    return;
  }

  username.setAttribute("autocomplete", "username");
  secret.setAttribute("autocomplete", "current-password");
  if (!username.value) {
    setValue(username, email);
  }

  if (!secret.value) {
    setValue(secret, password);
  }
})();
