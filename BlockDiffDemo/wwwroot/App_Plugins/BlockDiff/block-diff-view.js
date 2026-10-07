import { UmbElementMixin } from "@umbraco-cms/backoffice/element-api";
import { UMB_AUTH_CONTEXT } from "@umbraco-cms/backoffice/auth";
import { UMB_DOCUMENT_ENTITY_TYPE } from "@umbraco-cms/backoffice/document";
import { UMB_NOTIFICATION_CONTEXT } from "@umbraco-cms/backoffice/notification";
import { UMB_ACTION_EVENT_CONTEXT } from "@umbraco-cms/backoffice/action";
import { UmbRequestReloadStructureForEntityEvent, UmbEntityUpdatedEvent } from "@umbraco-cms/backoffice/entity-action";

class BlockDiffWorkspaceView extends UmbElementMixin(HTMLElement) {
  #contentId = "";
  #timer = 0;
  #auth;
  #notifications;
  #actions;
  #diff;
  #versions = [];
  #linksOpen = false;
  #pendingRollback = null;

  constructor() {
    super();
    this.consumeContext(UMB_AUTH_CONTEXT, (auth) => {
      this.#auth = auth;
    });
    this.consumeContext(UMB_NOTIFICATION_CONTEXT, (notifications) => {
      this.#notifications = notifications;
    });
    this.consumeContext(UMB_ACTION_EVENT_CONTEXT, (actions) => {
      this.#actions = actions;
    });
  }

  connectedCallback() {
    super.connectedCallback();
    this.style.display = "block";
    this.innerHTML = `
      <style>
        .block-diff { padding: 24px 28px 48px; max-width: 1080px; color: var(--uui-color-text, #1b264f); background: transparent; font-family: var(--uui-font-family, Lato, sans-serif); }
        .block-diff h1, .block-diff h2, .block-diff h3, .block-diff .name { color: var(--uui-color-text, #1b264f); }
        .block-diff h1 { margin: 0 0 6px; font-size: 24px; font-weight: 700; }
        .block-diff .lead { margin: 0 0 20px; color: var(--uui-color-text-alt, #515160); }
        .pickers { display: flex; gap: 16px; align-items: end; flex-wrap: wrap; margin-bottom: 18px; }
        .pickers label { display: flex; flex-direction: column; gap: 6px; font-size: 13px; font-weight: 700; min-width: 340px; color: var(--uui-color-text, #1b264f); }
        .pickers label.show { min-width: 180px; }
        .version-links { display: flex; gap: 16px; align-items: center; font-weight: 400; }
        .preview-link, .rollback-link { font-size: 13px; font-weight: 700; color: var(--uui-color-selected, #1b264f); }
        .rollback-link { border: 0; background: transparent; padding: 0; cursor: pointer; font-family: inherit; }
        button.chip { font: inherit; border: 0; cursor: pointer; color: var(--uui-color-text, #1b264f); }
        button.chip:hover, .preview-link:hover, .rollback-link:hover { text-decoration: underline; }
        .property, .block, .field, .nested { scroll-margin-top: 16px; }
        .pickers select { font: inherit; color: var(--uui-color-text, #1b264f); padding: 8px 10px; border: 1px solid var(--uui-color-border, #d8d7e9); border-radius: 3px; background: var(--uui-color-surface, #fff); }
        .summary-bar { display: flex; align-items: center; gap: 12px; margin-bottom: 12px; }
        .summary { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 22px; }
        .summary[hidden] { display: none; }
        button.toggle-links { font: inherit; font-size: 13px; font-weight: 700; border: 0; background: transparent; color: var(--uui-color-selected, #1b264f); cursor: pointer; padding: 4px 0; }
        .chip { border-radius: 99px; padding: 4px 10px; font-size: 13px; background: var(--uui-color-surface-alt, #f3f3f5); color: var(--uui-color-text, #1b264f); }
        .chip.count { background: var(--uui-color-selected, #1b264f); color: var(--uui-color-selected-contrast, #fff); }
        .property, .block { border: 1px solid var(--uui-color-border, #e3e3e8); border-radius: 6px; background: var(--uui-color-surface, #fff); color: var(--uui-color-text, #1b264f); margin-bottom: 14px; overflow: hidden; }
        .property header, .block header { display: flex; justify-content: space-between; gap: 12px; align-items: center; padding: 12px 14px; background: var(--uui-color-surface-alt, #fafafa); }
        .property h2, .block h3 { margin: 0; font-size: 16px; }
        .block h3 { font-size: 14px; }
        .fields { padding: 4px 14px 12px; }
        .field { display: grid; grid-template-columns: 180px 1fr 1fr; gap: 12px; padding: 10px 0; border-top: 1px solid var(--uui-color-border, #f0f0f3); }
        .field .name { font-weight: 700; font-size: 13px; }
        .value { white-space: pre-wrap; font-size: 14px; color: var(--uui-color-text, #1b264f); }
        .value small { display: block; color: var(--uui-color-text-alt, #76748f); font-size: 11px; text-transform: uppercase; letter-spacing: .04em; margin-bottom: 3px; }
        .diff-del { background: #fde2e2; color: #8d1f1f; text-decoration: line-through; border-radius: 2px; }
        .diff-ins { background: #d9f5e3; color: #0b7a32; border-radius: 2px; }
        .status { font-size: 12px; font-weight: 700; text-transform: uppercase; letter-spacing: .04em; }
        .changed { box-shadow: inset 3px 0 0 var(--uui-color-warning, #c47b00); }
        .added { box-shadow: inset 3px 0 0 var(--uui-color-positive, #0b7a32); }
        .removed { box-shadow: inset 3px 0 0 var(--uui-color-danger, #a12626); }
        .changed .status { color: var(--uui-color-warning, #8a5a00); }
        .added .status { color: var(--uui-color-positive, #0b7a32); }
        .removed .status { color: var(--uui-color-danger, #a12626); }
        .removed .value { text-decoration: line-through; color: var(--uui-color-text-alt, #76748f); }
        .unchanged { opacity: .72; }
        .nested { margin: 8px 0 8px 16px; }
        .empty, .error { padding: 16px; background: var(--uui-color-surface, #fff); color: var(--uui-color-text, #1b264f); border: 1px solid var(--uui-color-border, #e3e3e8); border-radius: 6px; }
        .error { border-color: var(--uui-color-danger, #f1c4c4); background: var(--uui-color-danger-emphasis, #fff6f6); color: var(--uui-color-danger-standalone, #a12626); }
        .rollback-dialog { position: fixed; inset: 0; z-index: 10000; display: flex; align-items: center; justify-content: center; background: rgba(15, 18, 28, .45); padding: 24px; }
        .rollback-dialog[hidden] { display: none; }
        .rollback-panel { width: min(480px, 100%); background: var(--uui-color-surface, #fff); color: var(--uui-color-text, #1b264f); border: 1px solid var(--uui-color-border, #e3e3e8); border-radius: 8px; box-shadow: 0 12px 40px rgba(0,0,0,.18); padding: 20px 22px; }
        .rollback-panel h2 { margin: 0 0 8px; font-size: 18px; }
        .rollback-panel p { margin: 0 0 14px; color: var(--uui-color-text-alt, #515160); font-size: 14px; line-height: 1.45; font-weight: 400; }
        .rollback-panel label { display: flex; flex-direction: column; gap: 6px; font-size: 13px; font-weight: 700; margin-bottom: 16px; }
        .rollback-panel textarea { font: inherit; font-weight: 400; min-height: 88px; resize: vertical; padding: 8px 10px; border: 1px solid var(--uui-color-border, #d8d7e9); border-radius: 3px; background: var(--uui-color-surface, #fff); color: var(--uui-color-text, #1b264f); }
        .rollback-actions { display: flex; flex-wrap: wrap; gap: 8px; justify-content: flex-end; }
        .rollback-actions button { font: inherit; font-size: 13px; font-weight: 700; border-radius: 3px; padding: 8px 12px; cursor: pointer; border: 1px solid var(--uui-color-border, #d8d7e9); background: var(--uui-color-surface, #fff); color: var(--uui-color-text, #1b264f); }
        .rollback-actions button.primary { background: var(--uui-color-selected, #1b264f); color: var(--uui-color-selected-contrast, #fff); border-color: var(--uui-color-selected, #1b264f); }
        .rollback-actions button.danger { background: var(--uui-color-danger, #a12626); color: #fff; border-color: var(--uui-color-danger, #a12626); }
      </style>
      <div class="block-diff">
        <h1>Compare versions</h1>
        <p class="lead">Text fields and Block List items, including blocks nested inside a block. Each field is labelled with its property group. Added, removed, and changed values are marked, and changed words are highlighted. Colors follow the backoffice theme.</p>
        <div class="pickers">
          <label>From
            <select id="from"></select>
            <span class="version-links">
              <a id="preview-from" class="preview-link" target="_blank" rel="noopener" hidden>Preview this version</a>
              <button type="button" id="rollback-from" class="rollback-link" hidden>Rollback</button>
            </span>
          </label>
          <label>To
            <select id="to"></select>
            <span class="version-links">
              <a id="preview-to" class="preview-link" target="_blank" rel="noopener" hidden>Preview this version</a>
              <button type="button" id="rollback-to" class="rollback-link" hidden>Rollback</button>
            </span>
          </label>
          <label class="show">Show
            <select id="filter">
              <option value="all">Show all</option>
              <option value="changed">Show only differences</option>
              <option value="unchanged">Show unchanged</option>
            </select>
          </label>
        </div>
        <div id="result"><p class="empty">Loading versions…</p></div>
      </div>
      <div id="rollback-dialog" class="rollback-dialog" hidden>
        <div class="rollback-panel" role="dialog" aria-modal="true" aria-labelledby="rollback-title">
          <h2 id="rollback-title">Roll back to this version?</h2>
          <p id="rollback-summary"></p>
          <label>Remark <span style="font-weight:400">(optional)</span>
            <textarea id="rollback-remark" placeholder="Why are you rolling back to this version?"></textarea>
          </label>
          <div class="rollback-actions">
            <button type="button" id="rollback-cancel">Cancel</button>
            <button type="button" id="rollback-save" class="primary">Save</button>
            <button type="button" id="rollback-publish" class="danger">Save and publish</button>
          </div>
        </div>
      </div>`;

    this.querySelector("#from").addEventListener("change", () => {
      this.#updatePreviewLinks();
      this.#compare();
    });
    this.querySelector("#to").addEventListener("change", () => {
      this.#updatePreviewLinks();
      this.#compare();
    });
    this.querySelector("#filter").addEventListener("change", () => {
      if (this.#diff) {
        this.#render(this.#diff);
      }
    });
    this.querySelector("#rollback-from").addEventListener("click", (event) => this.#openRollback(event));
    this.querySelector("#rollback-to").addEventListener("click", (event) => this.#openRollback(event));
    this.querySelector("#rollback-cancel").addEventListener("click", () => this.#closeRollbackDialog());
    this.querySelector("#rollback-dialog").addEventListener("click", (event) => {
      if (event.target === event.currentTarget) {
        this.#closeRollbackDialog();
      }
    });
    this.querySelector("#rollback-save").addEventListener("click", () => this.#confirmRollback(false));
    this.querySelector("#rollback-publish").addEventListener("click", () => this.#confirmRollback(true));
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
      this.#versions = versions;
      this.#fillSelect("#from", versions);
      this.#fillSelect("#to", versions);
      if (versions.length > 1) {
        this.querySelector("#from").value = String(versions[versions.length - 1].id ?? versions[versions.length - 1].Id);
        this.querySelector("#to").value = String(versions[0].id ?? versions[0].Id);
      }

      this.#updatePreviewLinks();
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
      this.#diff = await this.#api(`/umbraco/management/api/v1/block-diff/content/${this.#contentId}/compare?from=${from}&to=${to}`);
      this.#render(this.#diff);
    } catch (error) {
      result.innerHTML = `<p class="error">${this.#text(error.message)}</p>`;
    }
  }

  #render(diff) {
    const result = this.querySelector("#result");
    result.innerHTML = "";
    const count = diff.changeCount ?? diff.ChangeCount ?? 0;
    const lines = diff.summary ?? diff.Summary ?? [];
    const bar = document.createElement("div");
    bar.className = "summary-bar";
    const countChip = document.createElement("span");
    countChip.className = "chip count";
    countChip.textContent = count === 0 ? "No changes" : `${count} change${count === 1 ? "" : "s"}`;
    bar.append(countChip);
    const summary = document.createElement("div");
    summary.className = "summary";
    summary.hidden = !this.#linksOpen;
    if (lines.length > 0) {
      const toggle = document.createElement("button");
      toggle.type = "button";
      toggle.className = "toggle-links";
      toggle.textContent = this.#linksOpen ? "Hide links" : "Show links";
      toggle.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        this.#linksOpen = !this.#linksOpen;
        summary.hidden = !this.#linksOpen;
        toggle.textContent = this.#linksOpen ? "Hide links" : "Show links";
      });
      bar.append(toggle);
    }
    result.append(bar);
    for (const line of lines) {
      const text = typeof line === "string" ? line : line.text ?? line.Text ?? "";
      const target = typeof line === "string" ? "" : line.target ?? line.Target ?? "";
      if (!target) {
        const chip = document.createElement("span");
        chip.className = "chip";
        chip.textContent = text;
        summary.append(chip);
        continue;
      }

      const link = document.createElement("button");
      link.type = "button";
      link.className = "chip";
      link.textContent = text;
      link.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        this.#openSection(target);
      });
      summary.append(link);
    }
    result.append(summary);

    const mode = this.querySelector("#filter")?.value ?? "all";
    const cards = [];
    for (const property of diff.properties ?? diff.Properties ?? []) {
      const card = this.#property(property, mode);
      if (card) {
        cards.push(card);
      }
    }

    if (cards.length === 0) {
      const empty = document.createElement("p");
      empty.className = "empty";
      empty.textContent = mode === "unchanged"
        ? "Every field changed between these versions."
        : mode === "changed"
          ? "No differences between these versions."
          : "This item has no properties.";
      result.append(empty);
      return;
    }

    for (const card of cards) {
      result.append(card);
    }
  }

  #modeMatches(status, mode) {
    if (mode === "unchanged") {
      return status === "unchanged";
    }

    if (mode === "changed") {
      return status !== "unchanged";
    }

    return true;
  }

  #property(property, mode) {
    const kind = property.kind ?? property.Kind;
    const status = property.status ?? property.Status;
    if (kind !== "blocks" && !this.#modeMatches(status, mode)) {
      return null;
    }

    const box = document.createElement("section");
    box.className = `property ${status}`;
    this.#mark(box, property.anchor ?? property.Anchor);
    box.append(this.#header(property.name ?? property.Name, status, "h2"));
    if (kind === "blocks") {
      const body = document.createElement("div");
      body.className = "fields";
      const blocks = property.blocks ?? property.Blocks ?? [];
      const visible = [];
      for (const block of blocks) {
        const card = this.#block(block, mode);
        if (card) {
          visible.push(card);
        }
      }

      if (mode !== "all" && visible.length === 0) {
        return null;
      }

      if (visible.length === 0) {
        body.append(this.#note("No blocks in either version."));
      }

      for (const card of visible) {
        body.append(card);
      }

      box.append(body);
      return box;
    }

    box.append(this.#values(property.from ?? property.From, property.to ?? property.To));
    return box;
  }

  #block(block, mode) {
    const status = block.status ?? block.Status;
    const sourceFields = block.fields ?? block.Fields ?? [];
    const fieldNodes = [];
    for (const field of sourceFields) {
      const node = this.#field(field, mode);
      if (node) {
        fieldNodes.push(node);
      }
    }

    if (mode !== "all" && fieldNodes.length === 0 && (sourceFields.length > 0 || !this.#modeMatches(status, mode))) {
      return null;
    }

    const box = document.createElement("article");
    box.className = `block ${status}`;
    this.#mark(box, block.anchor ?? block.Anchor);
    box.append(this.#header(block.label ?? block.Label ?? block.name ?? block.Name, status, "h3"));
    const fields = document.createElement("div");
    fields.className = "fields";
    for (const node of fieldNodes) {
      fields.append(node);
    }
    box.append(fields);
    return box;
  }

  #field(field, mode) {
    const nested = field.blocks ?? field.Blocks ?? [];
    const status = field.status ?? field.Status;
    const nestedList = nested.length > 0 || ((field.blocks || field.Blocks) && (field.from ?? field.From) == null && (field.to ?? field.To) == null);
    if (nestedList) {
      const children = [];
      for (const block of nested) {
        const card = this.#block(block, mode);
        if (card) {
          children.push(card);
        }
      }

      if (mode !== "all" && children.length === 0) {
        return null;
      }

      const wrap = document.createElement("div");
      wrap.className = "nested";
      this.#mark(wrap, field.anchor ?? field.Anchor);
      const title = document.createElement("strong");
      title.textContent = field.name ?? field.Name;
      wrap.append(title);
      for (const card of children) {
        wrap.append(card);
      }
      return wrap;
    }

    if (!this.#modeMatches(status, mode)) {
      return null;
    }

    const row = document.createElement("div");
    row.className = `field ${status}`;
    this.#mark(row, field.anchor ?? field.Anchor);
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

  #updatePreviewLinks() {
    this.#setPreview("#preview-from", "#rollback-from", this.querySelector("#from").value);
    this.#setPreview("#preview-to", "#rollback-to", this.querySelector("#to").value);
  }

  #setPreview(previewSelector, rollbackSelector, versionId) {
    const link = this.querySelector(previewSelector);
    const rollback = this.querySelector(rollbackSelector);
    const version = this.#versions.find((item) => String(item.id ?? item.Id) === String(versionId));
    const url = version?.previewUrl ?? version?.PreviewUrl ?? "";
    link.href = url || "#";
    link.hidden = url.length === 0;
    rollback.hidden = !version;
    rollback.dataset.versionId = String(version?.id ?? version?.Id ?? "");
    rollback.dataset.versionKey = versionKeyFromPreview(url);
    rollback.dataset.versionLabel = version?.label ?? version?.Label ?? "";
  }

  #openRollback(event) {
    event.preventDefault();
    const button = event.currentTarget;
    const versionKey = button.dataset.versionKey ?? "";
    const versionId = Number(button.dataset.versionId || 0);
    const versionLabel = button.dataset.versionLabel || "the selected version";
    if (!versionKey) {
      this.#notifications?.peek("danger", {
        data: { message: "That version cannot be rolled back from here." },
      });
      return;
    }

    this.#pendingRollback = { versionKey, versionId, versionLabel };
    this.querySelector("#rollback-summary").textContent =
      `Restore ${versionLabel}. Save keeps it as draft; Save and publish updates the live site.`;
    this.querySelector("#rollback-remark").value = "";
    this.querySelector("#rollback-dialog").hidden = false;
    this.querySelector("#rollback-remark").focus();
  }

  #closeRollbackDialog() {
    this.#pendingRollback = null;
    this.querySelector("#rollback-dialog").hidden = true;
  }

  async #confirmRollback(publish) {
    const pending = this.#pendingRollback;
    if (!pending) {
      return;
    }

    const remark = this.querySelector("#rollback-remark").value.trim();
    this.#closeRollbackDialog();

    try {
      await this.#post(`/umbraco/management/api/v1/document-version/${pending.versionKey}/rollback`);
      if (publish) {
        await this.#put(`/umbraco/management/api/v1/document/${this.#contentId}/publish`, {
          publishSchedules: [{ culture: null }],
        });
      }

      // Remarks go to Compare versions only — do not write a second History Rollback entry.
      if (remark) {
        await this.#post(`/umbraco/management/api/v1/block-diff/content/${this.#contentId}/rollback-remark`, {
          remark,
        });
      }

      this.#notifications?.peek("positive", {
        data: {
          message: publish
            ? "The document was rolled back and published."
            : "The document was rolled back and saved as draft.",
        },
      });
      this.#actions?.dispatchEvent(new UmbRequestReloadStructureForEntityEvent({
        unique: this.#contentId,
        entityType: UMB_DOCUMENT_ENTITY_TYPE,
      }));
      this.#actions?.dispatchEvent(new UmbEntityUpdatedEvent({
        unique: this.#contentId,
        entityType: UMB_DOCUMENT_ENTITY_TYPE,
      }));
      await this.#loadVersions();
    } catch (error) {
      this.#notifications?.peek("danger", {
        data: { message: this.#text(error.message) },
      });
    }
  }

  #openSection(target) {
    const found = () => this.querySelector(`#${CSS.escape(target)}`);
    if (!found() && this.#diff && this.querySelector("#filter").value !== "all") {
      this.querySelector("#filter").value = "all";
      this.#render(this.#diff);
    }

    found()?.scrollIntoView({ behavior: "smooth", block: "start" });
  }

  #mark(element, anchor) {
    if (anchor) {
      element.id = anchor;
    }
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
    const response = await this.#request(path);
    return response.json();
  }

  async #post(path, body) {
    return this.#send(path, "POST", body);
  }

  async #put(path, body) {
    return this.#send(path, "PUT", body);
  }

  async #send(path, method, body) {
    const response = await this.#request(path, {
      method,
      headers: body === undefined ? {} : { "Content-Type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (response.status === 204) {
      return;
    }

    const text = await response.text();
    return text ? JSON.parse(text) : undefined;
  }

  async #request(path, options = {}) {
    const started = Date.now();
    while (!this.#auth && Date.now() - started < 5000) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }

    if (!this.#auth) {
      throw new Error("The backoffice sign-in token is not available.");
    }

    const token = await this.#auth.getLatestToken();
    const response = await fetch(path, {
      ...options,
      headers: {
        Accept: "application/json",
        Authorization: `Bearer ${token}`,
        ...(options.headers ?? {}),
      },
    });
    if (!response.ok) {
      throw new Error(`Request failed (${response.status}).`);
    }

    return response;
  }

  #text(value) {
    return value ?? "Something went wrong.";
  }
}

function versionKeyFromPreview(url) {
  if (!url) {
    return "";
  }

  try {
    return new URL(url, window.location.origin).searchParams.get("vid") ?? "";
  } catch {
    return "";
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
    name: "Compare versions",
    element: BlockDiffWorkspaceView,
    weight: 150,
    meta: {
      label: "Compare versions",
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
