import assert from "node:assert/strict";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { basename, join, posix } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const topics = [
  "README.md", "DEMO.md", "PROJECT_STATUS.md", "docs/ARCHITECTURE.md",
  "docs/SCREENSHOTS.md", "backend/README.md", "frontend/README.md",
  "backend/services/payment/FINANCIAL_RECOVERY.md", "frontend/app/fonts/README.md",
];
const english = path => path.replace(/\.md$/, ".en.md");
const read = path => readFileSync(join(root, path), "utf8").replace(/\r\n/g, "\n");
const switchLine = path => `[简体中文](${basename(path)}) | [English](${basename(english(path))})`;
const gallery = "docs/SCREENSHOTS.md";

const args = process.argv.slice(2);
assert(args.length === 0 || (args.length === 1 && args[0] === "--fix-gallery-nav"),
  "Usage: node docs/check-docs.mjs [--fix-gallery-nav]");
if (args.length) {
  const text = read(gallery);
  const header = "# RushGo 页面图册\n\n";
  assert(text.startsWith(header), "Unexpected generated gallery title");
  if (!text.includes(switchLine(gallery))) {
    writeFileSync(join(root, gallery), header + switchLine(gallery) + "\n\n" + text.slice(header.length));
  }
}

const documents = new Map();
for (const topic of topics) {
  for (const path of [topic, english(topic)]) {
    const source = read(path);
    assert.equal(source.split("\n")[2], switchLine(topic), path + ": language switch");
    const blocks = [...source.matchAll(/^(`{3,}|~{3,})([\w-]*)\s*\n([\s\S]*?)^\1[ \t]*$/gm)];
    const prose = source.replace(/^(`{3,}|~{3,})[^\n]*\n[\s\S]*?^\1[ \t]*$/gm, "");
    const headings = [...prose.matchAll(/^(#{1,6}) +(.+)$/gm)];
    const anchors = new Set([...prose.matchAll(/<a\s+id="([^"]+)"/g)].map(m => m[1]));
    for (const [, , label] of headings) {
      const slug = label.toLowerCase().replace(/[^\p{L}\p{M}\p{N}_ -]/gu, "").replaceAll(" ", "-");
      let id = slug;
      for (let n = 1; anchors.has(id); n++) id = slug + "-" + n;
      anchors.add(id);
    }
    documents.set(path, { prose, blocks, headings, anchors });
  }
}

let referenceCount = 0;
for (const [path, doc] of documents) {
  const en = path.endsWith(".en.md");
  for (const match of doc.prose.matchAll(/\]\(([^()\s]+)\)|\b(?:src|href)="([^"]+)"/g)) {
    const href = match[1] ?? match[2];
    if (/^[\w+.-]+:/.test(href)) continue;
    const [file, fragment] = href.split("#");
    const target = file ? posix.normalize(posix.join(posix.dirname(path), decodeURIComponent(file))) : path;
    assert(existsSync(join(root, target)), path + ": missing " + href);
    if (fragment && documents.has(target)) {
      assert(documents.get(target).anchors.has(decodeURIComponent(fragment)), path + ": missing anchor " + href);
    }
    if (documents.has(target) && target.endsWith(".en.md") !== en) {
      const counterpart = en ? path.replace(/\.en\.md$/, ".md") : english(path);
      assert.equal(target, counterpart, path + ": link crosses languages " + href);
    }
    referenceCount++;
  }
}

const pictures = prose => [...prose.matchAll(/!\[[^\]]*\]\(([^)]+)\)|<img\b[^>]*\bsrc="([^"]+)"/g)]
  .map(m => m[1] ?? m[2]).sort();
for (const topic of topics) {
  const zh = documents.get(topic);
  const en = documents.get(english(topic));
  assert.deepEqual(en.headings.map(m => m[1]), zh.headings.map(m => m[1]), topic + ": heading levels");
  assert.deepEqual(en.blocks.map(m => m[2]), zh.blocks.map(m => m[2]), topic + ": code block types");
  for (const [i, block] of zh.blocks.entries()) {
    if (block[2] !== "mermaid") {
      const code = body => body.split("\n").filter(line => !line.trimStart().startsWith("#")).join("\n");
      assert.equal(code(en.blocks[i][3]), code(block[3]), topic + ": command or configuration mismatch");
    }
  }
  assert.deepEqual(pictures(en.prose), pictures(zh.prose), topic + ": image references");
}

const index = JSON.parse(read("docs/demo/screenshot-index.json"));
for (const path of [gallery, english(gallery)]) {
  const doc = documents.get(path);
  const expected = index.flatMap(row => row.screenshots.map(shot => posix.relative("docs", shot.file))).sort();
  assert.deepEqual(pictures(doc.prose), expected, path + ": screenshots differ from the capture index");
  const rows = [...doc.prose.matchAll(/^\| \[[^\n]+?\]\(#([^)]+)\) \| `([^`]+)` \|/gm)];
  assert.deepEqual(rows.map(m => m[2]).sort(), index.map(row => row.route).sort(), path + ": page routes");
}

const diagrams = topics.reduce((n, path) => n + documents.get(path).blocks.filter(b => b[2] === "mermaid").length, 0);
console.log(`Verified ${topics.length} bilingual pairs and ${referenceCount} relative references.`);
console.log(`Each language covers ${diagrams} diagrams and ${index.length} gallery routes.`);
