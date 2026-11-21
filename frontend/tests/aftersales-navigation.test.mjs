import assert from "node:assert/strict";
import { test } from "node:test";
import { loadTypeScript } from "./render-typescript.mjs";

const { approvalEventId } = loadTypeScript("lib/aftersales-navigation.ts");
const origin = "http://127.0.0.1:3106";
const id = "8cc8f7ed-ed3b-4855-92a5-fa2dd70eb162";
const route = `/admin/events/${id}/refunds`;

test("approval entry accepts shared local links and IDs", () => {
  for (const value of [id, `  ${id}  `, route, origin + route, origin + route + "/"])
    assert.equal(approvalEventId(value, origin), id);
});

test("approval entry cannot navigate to another origin, route, or untrusted scheme", () => {
  for (const value of ["", "00000000-0000-0000-0000-000000000000",
    "not-an-id", "https://other.example" + route, "//other.example" + route,
    "javascript:alert(1)", origin + route + "?redirect=https://other.example",
    origin + route + "#code", origin + route + "/approve",
    origin.replace("127.0.0.1", "user@127.0.0.1") + route,
    route.replace(id, encodeURIComponent(id + "/")), "/admin/events/" + id,
    origin + "/admin/events/\n" + id + "/refunds", "x".repeat(2049)])
    assert.equal(approvalEventId(value, origin), null, value);
});
