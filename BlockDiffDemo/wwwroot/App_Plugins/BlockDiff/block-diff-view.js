import { UmbElementMixin } from "@umbraco-cms/backoffice/element-api";
import { UMB_AUTH_CONTEXT } from "@umbraco-cms/backoffice/auth";

class BlockDiffWorkspaceView extends UmbElementMixin(HTMLElement) {
  #contentId = "";
  #timer = 0;
  #auth;

  constructor() {
    super();
    this.consumeContext(UMB_AUTH_CONTEXT, (auth) => {
      this.#auth = auth;
    });
  }

  connectedCallback() {
    super.connectedCallback();
    this.style.display = "block";
    this.innerHTML = `
      <style>
        .block-diff { padding: 24px 28px 48px; max-width: 1080px; color: var(--uui-color-text, #1b264f); font-family: var(--uui-font-family, Lato, sans-serif); }
        .block-diff h1 { margin: 0 0 6px; font-size: 24px; font-weight: 700; }
        .block-diff .lead { margin: 0 0 20px; color: var(--uui-color-text-alt, #515160); }
        .pickers { display: flex; gap: 16px; align-items: end; flex-wrap: wrap; margin-bottom: 18px; }
        .pickers label { display: flex; flex-direction: column; gap: 6px; font-size: 13px; font-weight: 700; min-width: 280px; }
        .pickers select { font: inherit; padding: 8px 10px; border: 1px solid var(--uui-color-border, #d8d7e9); border-radius: 3px; background: #fff; }
        .summary { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 22px; }
        .chip { border-radius: 99px; padding: 4px 10px; font-size: 13px; background: #f3f3f5; }
        .chip.count { background: #1b264f; color: #fff; }
        .property, .block { border: 1px solid var(--uui-color-border, #e3e3e8); border-radius: 6px; background: #fff; margin-bottom: 14px; overflow: hidden; }
        .property header, .block header { display: flex; justify-content: space-between; gap: 12px; align-items: center; padding: 12px 14px; background: #fafafa; }
        .property h2, .block h3 { margin: 0; font-size: 16px; }
        .block h3 { font-size: 14px; }
        .fields { padding: 4px 14px 12px; }
        .field { display: grid; grid-template-columns: 140px 1fr 1fr; gap: 12px; padding: 10px 0; border-top: 1px solid #f0f0f3; }
        .field .name { font-weight: 700; font-size: 13px; }
        .value { white-space: pre-wrap; font-size: 14px; }
        .value small { display: block; color: #76748f; font-size: 11px; text-transform: uppercase; letter-spacing: .04em; margin-bottom: 3px; }
        .diff-del { background: #fde2e2; color: #8d1f1f; text-decoration: line-through; border-radius: 2px; }
        .diff-ins { background: #d9f5e3; color: #0b7a32; border-radius: 2px; }
        .status { font-size: 12px; font-weight: 700; text-transform: uppercase; letter-spacing: .04em; }
        .changed { box-shadow: inset 3px 0 0 #c47b00; }
        .added { box-shadow: inset 3px 0 0 #0b7a32; }
        .removed { box-shadow: inset 3px 0 0 #a12626; }
        .changed .status { color: #8a5a00; }
        .added .status { color: #0b7a32; }
        .removed .status { color: #a12626; }
        .removed .value { text-decoration: line-through; color: #76748f; }
        .unchanged { opacity: .72; }
        .nested { margin: 8px 0 8px 16px; }
        .empty, .error { padding: 16px; background: #fff; border: 1px solid #e3e3e8; border-radius: 6px; }
        .error { border-color: #f1c4c4; background: #fff6f6; }
      </style>
      <div class="block-diff">
        <h1>Block diff</h1>
        <p class="lead">Text fields and Block List items, including blocks nested inside a block. Added, removed, and changed values are marked. Changed words are highlighted in each column. Unchanged values stay on the page so the structure is visible.</p>
        <div class="pickers">
          <label>From<select id="from"></select></label>
          <label>To<select id="to"></select></label>
        </div>
        <div id="result"><p class="empty">Loading versions…</p></div>
      </div>`;

    this.querySelector("#from").addEventListener("change", () => this.#compare());
    this.querySelector("#to").addEventListener("change", () => this.#compare());
    this.#syncContent();
    this.#timer = window.setInterval(() => this.#syncContent(), 600);
  }

  disconnectedCallback() {
    window.clearInterval(this.#timer);
    super.disconnectedCallback();
  }

  #syncContent() {
    const match = window.location.pathname.match(/document\/edit\/([0-9a-f-]{36})/i);
    const id = match?.[1] ?? "";
    if (!id || id === this.#contentId) {
      return;
    }

    this.#contentId = id;
    this.#loadVersions();
  }

  async #loadVersions() {
    const result = this.querySelector("#result");
    result.innerHTML = `<p class="empty">Loading versions…</p>`;
    try {
      const data = await this.#api(`/umbraco/management/api/v1/block-diff/content/${this.#contentId}/versions`);
      const versions = data.versions ?? data.Versions ?? [];
      this.#fillSelect("#from", versions);
      this.#fillSelect("#to", versions);
      if (versions.length > 1) {
        this.querySelector("#from").value = String(versions[versions.length - 1].id ?? versions[versions.length - 1].Id);
        this.querySelector("#to").value = String(versions[0].id ?? versions[0].Id);
      }

      await this.#compare();
    } catch (error) {
      result.innerHTML = `<p class="error">${this.#text(error.message)}</p>`;
    }
  }

  #fillSelect(selector, versions) {
    const select = this.querySelector(selector);
    select.innerHTML = "";
    for (const version of versions) {
      const option = document.createElement("option");
      option.value = String(version.id ?? version.Id);
      option.textContent = version.label ?? version.Label;
      select.append(option);
    }
  }

  async #compare() {
    const from = this.querySelector("#from").value;
    const to = this.querySelector("#to").value;
    const result = this.querySelector("#result");
    if (!from || !to) {
      result.innerHTML = `<p class="empty">This item has no saved versions yet.</p>`;
      return;
    }

    result.innerHTML = `<p class="empty">Comparing…</p>`;
    try {
      this.#render(await this.#api(`/umbraco/management/api/v1/block-diff/content/${this.#contentId}/compare?from=${from}&to=${to}`));
    } catch (error) {
      result.innerHTML = `<p class="error">${this.#text(error.message)}</p>`;
    }
  }

  #render(diff) {
    const result = this.querySelector("#result");
    result.innerHTML = "";
    const summary = document.createElement("div");
    summary.className = "summary";
    const count = diff.changeCount ?? diff.ChangeCount ?? 0;
    const lines = diff.summary ?? diff.Summary ?? [];
    const countChip = document.createElement("span");
    countChip.className = "chip count";
    countChip.textContent = count === 0 ? "No changes" : `${count} change${count === 1 ? "" : "s"}`;
    summary.append(countChip);
    for (const line of lines) {
      const chip = document.createElement("span");
      chip.className = "chip";
      chip.textContent = line;
      summary.append(chip);
    }
    result.append(summary);

    for (const property of diff.properties ?? diff.Properties ?? []) {
      result.append(this.#property(property));
    }
  }

  #property(property) {
    const kind = property.kind ?? property.Kind;
    const box = document.createElement("section");
    box.className = `property ${property.status ?? property.Status}`;
    box.append(this.#header(property.name ?? property.Name, property.status ?? property.Status, "h2"));
    if (kind === "blocks") {
      const body = document.createElement("div");
      body.className = "fields";
      const blocks = property.blocks ?? property.Blocks ?? [];
      if (blocks.length === 0) {
        body.append(this.#note("No blocks in either version."));
      }
      for (const block of blocks) {
        body.append(this.#block(block));
      }
      box.append(body);
      return box;
    }

    box.append(this.#values(property.from ?? property.From, property.to ?? property.To));
    return box;
  }

  #block(block) {
    const box = document.createElement("article");
    box.className = `block ${block.status ?? block.Status}`;
    box.append(this.#header(block.label ?? block.Label ?? block.name ?? block.Name, block.status ?? block.Status, "h3"));
    const fields = document.createElement("div");
    fields.className = "fields";
    for (const field of block.fields ?? block.Fields ?? []) {
      fields.append(this.#field(field));
    }
    box.append(fields);
    return box;
  }

  #field(field) {
    const nested = field.blocks ?? field.Blocks ?? [];
    if (nested.length > 0 || (field.status ?? field.Status) === "changed" && nested.length === 0 && (field.from ?? field.From) == null && (field.to ?? field.To) == null && (field.blocks || field.Blocks)) {
      const wrap = document.createElement("div");
      wrap.className = "nested";
      const title = document.createElement("strong");
      title.textContent = field.name ?? field.Name;
      wrap.append(title);
      for (const block of nested) {
        wrap.append(this.#block(block));
      }
      return wrap;
    }

    const row = document.createElement("div");
    row.className = `field ${field.status ?? field.Status}`;
    const name = document.createElement("div");
    name.className = "name";
    name.textContent = field.name ?? field.Name;
    const from = field.from ?? field.From;
    const to = field.to ?? field.To;
    row.append(name, this.#side("From", from, to, "from"), this.#side("To", to, from, "to"));
    return row;
  }

  #values(from, to) {
    const row = document.createElement("div");
    row.className = "fields";
    const field = document.createElement("div");
    field.className = "field";
    field.append(document.createElement("div"), this.#side("From", from, to, "from"), this.#side("To", to, from, "to"));
    row.append(field);
    return row;
  }

  #side(caption, value, other, side) {
    const cell = document.createElement("div");
    cell.className = "value";
    const label = document.createElement("small");
    label.textContent = caption;
    cell.append(label);
    if (!value) {
      cell.append(document.createTextNode("—"));
      return cell;
    }

    if (!other || value === other) {
      cell.append(document.createTextNode(value));
      return cell;
    }

    const fromText = side === "from" ? value : other;
    const toText = side === "to" ? value : other;
    for (const part of wordDiff(fromText, toText)) {
      const show = part.op === "equal" || (side === "from" && part.op === "delete") || (side === "to" && part.op === "insert");
      if (!show) {
        continue;
      }

      if (part.op === "equal") {
        cell.append(document.createTextNode(part.text));
        continue;
      }

      const mark = document.createElement("span");
      mark.className = part.op === "delete" ? "diff-del" : "diff-ins";
      mark.textContent = part.text;
      cell.append(mark);
    }

    return cell;
  }

  #header(title, status, tag) {
    const header = document.createElement("header");
    const heading = document.createElement(tag);
    heading.textContent = title;
    const pill = document.createElement("span");
    pill.className = "status";
    pill.textContent = status;
    header.append(heading, pill);
    return header;
  }

  #note(text) {
    const paragraph = document.createElement("p");
    paragraph.textContent = text;
    return paragraph;
  }

  async #api(path) {
    const started = Date.now();
    while (!this.#auth && Date.now() - started < 5000) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }

    if (!this.#auth) {
      throw new Error("The backoffice sign-in token is not available.");
    }

    const token = await this.#auth.getLatestToken();
    const response = await fetch(path, {
      headers: {
        Accept: "application/json",
        Authorization: `Bearer ${token}`,
      },
    });
    if (!response.ok) {
      throw new Error(`Request failed (${response.status}).`);
    }

    return response.json();
  }

  #text(value) {
    return value ?? "Something went wrong.";
  }
}

function wordDiff(from, other) {
  const source = tokenize(from);
  const target = tokenize(other);
  const rows = source.length + 1;
  const columns = target.length + 1;
  const lengths = Array.from({ length: rows }, () => new Uint16Array(columns));
  for (let i = source.length - 1; i >= 0; i--) {
    for (let j = target.length - 1; j >= 0; j--) {
      lengths[i][j] = source[i] === target[j]
        ? lengths[i + 1][j + 1] + 1
        : Math.max(lengths[i + 1][j], lengths[i][j + 1]);
    }
  }

  const parts = [];
  let i = 0;
  let j = 0;
  while (i < source.length && j < target.length) {
    if (source[i] === target[j]) {
      parts.push({ op: "equal", text: source[i] });
      i++;
      j++;
    } else if (lengths[i + 1][j] >= lengths[i][j + 1]) {
      parts.push({ op: "delete", text: source[i] });
      i++;
    } else {
      parts.push({ op: "insert", text: target[j] });
      j++;
    }
  }

  while (i < source.length) {
    parts.push({ op: "delete", text: source[i] });
    i++;
  }

  while (j < target.length) {
    parts.push({ op: "insert", text: target[j] });
    j++;
  }

  return parts;
}

function tokenize(value) {
  return String(value).split(/(\s+)/).filter((token) => token.length > 0);
}

if (!customElements.get("block-diff-workspace-view")) {
  customElements.define("block-diff-workspace-view", BlockDiffWorkspaceView);
}

export const onInit = (_host, extensionRegistry) => {
  extensionRegistry.register({
    type: "workspaceView",
    alias: "BlockDiff.WorkspaceView",
    name: "Block diff",
    element: BlockDiffWorkspaceView,
    weight: 850,
    meta: {
      label: "Block diff",
      pathname: "block-diff",
      icon: "icon-list",
    },
    conditions: [
      {
        alias: "Umb.Condition.WorkspaceAlias",
        match: "Umb.Workspace.Document",
      },
    ],
  });
};

export default BlockDiffWorkspaceView;
