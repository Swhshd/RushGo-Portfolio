import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import ts from "typescript";
import React from "react";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const require = createRequire(import.meta.url);

// Compile the real components without introducing a test runner dependency.
// Overrides isolate browser/payment I/O; React and the application code stay real.
export function loadTypeScript(file, overrides = {}) {
  const cache = new Map();
  function load(filename) {
    if (cache.has(filename)) return cache.get(filename).exports;
    const source = fs.readFileSync(filename, "utf8");
    const compiled = ts.transpileModule(source, {
      compilerOptions: { target: ts.ScriptTarget.ES2020, module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX, esModuleInterop: true },
      fileName: filename,
    }).outputText;
    const compiledModule = { exports: {} };
    cache.set(filename, compiledModule);
    function localRequire(specifier) {
      if (Object.hasOwn(overrides, specifier)) return overrides[specifier];
      if (specifier.startsWith(".") || specifier.startsWith("@/")) {
        const target = specifier.startsWith("@/")
          ? path.join(root, specifier.slice(2))
          : path.resolve(path.dirname(filename), specifier);
        for (const extension of ["", ".ts", ".tsx", "/index.ts", "/index.tsx"]) {
          if (fs.existsSync(target + extension) && fs.statSync(target + extension).isFile()) {
            return /\.tsx?$/.test(target + extension) ? load(target + extension) : require(target + extension);
          }
        }
      }
      return require(specifier);
    }
    const testProcess = { env: { NEXT_PUBLIC_STRIPE_PUBLISHABLE_KEY: "pk_test_fixture" } };
    new Function("require", "module", "exports", "process", compiled)(localRequire, compiledModule, compiledModule.exports, testProcess);
    return compiledModule.exports;
  }
  return load(path.join(root, file));
}

export const browserOverrides = {
  "next/navigation": {
    useRouter: () => ({ push() {}, replace() {} }),
    useSearchParams: () => new URLSearchParams(),
  },
  "next/link": { __esModule: true, default: ({ children, ...props }) => React.createElement("a", props, children) },
  "@stripe/stripe-js": { loadStripe: () => null },
  "@stripe/react-stripe-js": {
    Elements: ({ children }) => children,
    CardElement: () => React.createElement("div", null, "Card input"),
    useStripe: () => ({ confirmCardPayment() { throw new Error("No payments in render regression"); } }),
    useElements: () => ({}),
  },
};
