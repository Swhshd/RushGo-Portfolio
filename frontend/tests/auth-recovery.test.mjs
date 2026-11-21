import assert from "node:assert/strict";
import {test} from "node:test";
import {loadTypeScript} from "./render-typescript.mjs";
import {componentDriver,find} from "./recovery-driver.mjs";
// Catch missing match validation and any confirmation leaking into real API JSON.
// Only browser storage and HTTP transport are controlled; page/policy/client stay real.
function registrationFixture(t, { cooldownMs = 60000 } = {}) {
  const previous = { window: globalThis.window, localStorage: globalThis.localStorage, fetch: globalThis.fetch };
  const calls = [], pushes = [], tokens = new Map();
  globalThis.window = {};
  globalThis.localStorage = { getItem: key => tokens.get(key) || null, setItem: (key, value) => tokens.set(key, value), removeItem: key => tokens.delete(key) };
  t.after(() => Object.assign(globalThis, previous));
  const { apiClient } = loadTypeScript("lib/api/client.ts");
  globalThis.fetch = async (url, options) => {
    calls.push({ url, options });
    if (url === "/api/auth/registration-code") {
      const generation = calls.filter(call => call.url === url).length;
      return new Response(JSON.stringify({ verification_id: generation === 1 ? "synthetic-proof" : "synthetic-proof-2", expires_at: new Date(Date.now() + 600000).toISOString(), next_resend_at: new Date(Date.now() + cooldownMs).toISOString() }));
    }
    assert.equal(url, "/api/auth/register", "unexpected auth operation");
    return new Response(JSON.stringify({ account_created: true, message: "Account created. Please sign in." }), { status: 201 });
  };
  const driver = componentDriver("app/(customer)/register/page.tsx", {
    "next/navigation": { useRouter: () => ({ push: href => pushes.push(href) }) },
    "@/lib/auth-context": { useAuth: () => ({ register: input => apiClient.register(input) }) },
    "@/lib/api/client": { apiClient },
  });
  const field = id => find(driver.render(), node => node.props?.id === id);
  const change = (id, value) => {
    // Permit the pre-fix page to submit so RED proves the missing boundary,
    // rather than failing merely because the new input selector is absent.
    const input = field(id);
    if (!input && id === "register-confirm") return;
    assert(input, `missing input ${id}`);
    input.props.onChange({ target: { value } });
  };
  const send = () => find(driver.render(), node => node.type === "button" && node.props.type === "button");
  const submit = () => find(driver.render(), node => node.type === "form").props.onSubmit({ preventDefault() {} });
  async function prepare(password, confirmation = password) {
    change("register-name", "Synthetic M6.1");
    change("register-email", "m61@example.test");
    change("register-password", password);
    change("register-confirm", confirmation);
    await send().props.onClick();
    change("register-code", "000001");
  }
  return { driver, field, change, send, submit, prepare, calls, pushes, tokens, registrations: () => calls.filter(call => call.url === "/api/auth/register") };
}

test("registration mismatch reports a local error without POST, proof loss or session creation", async t => {
  const f = registrationFixture(t);
  await f.prepare("synthetic long input", "synthetic typo input");
  const cooldown = f.send().props.children;
  await f.submit();
  assert.equal(f.registrations().length, 0, "mismatch must stop before registration HTTP");
  assert.equal(find(f.driver.render(), node => node.props?.role === "alert")?.props.children, "Passwords do not match.");
  assert.equal(f.field("register-code").props.value, "000001");
  assert.equal(f.send().props.disabled, true);
  assert.equal(f.send().props.children, cooldown);
  assert(find(f.driver.render(), node => node.props?.role === "status"), "valid proof must remain visible");
  assert.deepEqual(f.pushes, []);
  assert.equal(f.tokens.size, 0);

  f.change("register-confirm", "synthetic long input");
  await f.submit();
  assert.equal(f.registrations().length, 1, "correcting confirmation must reuse the still-valid proof");
  assert.equal(JSON.parse(f.registrations()[0].options.body).verification_id, "synthetic-proof");
  assert.equal(f.calls.filter(call => call.url === "/api/auth/registration-code").length, 1);
  assert.deepEqual(f.pushes, ["/login?success=registered"]);
  assert.equal(f.tokens.size, 0);
});

test("matching registration sends exactly the approved JSON fields and one password", async t => {
  const f = registrationFixture(t);
  await f.prepare("synthetic long input");
  await f.submit();
  assert.equal(f.registrations().length, 1);
  const request = f.registrations()[0];
  assert.equal(request.options.method, "POST");
  assert.deepEqual(JSON.parse(request.options.body), { email: "m61@example.test", password: "synthetic long input", name: "Synthetic M6.1", verification_id: "synthetic-proof", verification_code: "000001" });
  assert.deepEqual(f.pushes, ["/login?success=registered"]);
  assert.equal(f.tokens.size, 0);
  const password = f.field("register-password"), confirm = f.field("register-confirm");
  assert(confirm, "confirmation must be rendered");
  assert.equal(confirm.props.type, "password");
  assert.equal(confirm.props.autoComplete, "new-password");
  assert.equal(confirm.props.required, true);
  assert.equal(confirm.props.className, password.props.className);
});

for (const [label, password, expectedError] of [
  ["15 ASCII scalars", "x".repeat(15), null],
  ["14 ASCII scalars", "x".repeat(14), "Use at least 15 characters."],
  ["15 supplementary scalars", "😀".repeat(15), null],
  ["72 UTF-8 bytes", "😀".repeat(18), null],
  ["76 UTF-8 bytes", "😀".repeat(19), "This password is too long. Use fewer characters."],
  ["73 ASCII bytes", "x".repeat(73), "This password is too long. Use fewer characters."],
  ["15 untrimmed spaces without composition rules", " ".repeat(15), null],
]) {
  test(`matching registration preserves policy for ${label}`, async t => {
    const f = registrationFixture(t);
    await f.prepare(password);
    await f.submit();
    assert.equal(f.registrations().length, expectedError ? 0 : 1);
    if (expectedError) assert(find(f.driver.render(), node => node.props?.children === expectedError), "existing policy error must remain visible");
    else assert.equal(JSON.parse(f.registrations()[0].options.body).password, password);
    assert.equal(f.tokens.size, 0);
  });
}

test("confirmation mismatch does not replace the existing password policy error", async t => {
  const f = registrationFixture(t);
  await f.prepare("x".repeat(14), "synthetic long input");
  await f.submit();
  assert.equal(f.registrations().length, 0);
  assert(find(f.driver.render(), node => node.props?.children === "Use at least 15 characters."), "policy must be checked before confirmation");
});

test("resend after a local mismatch replaces proof and clears code without changing passwords", async t => {
  const f = registrationFixture(t, { cooldownMs: -1000 });
  await f.prepare("synthetic long input", "synthetic typo input");
  await f.submit();
  assert.equal(f.registrations().length, 0);
  assert.equal(f.send().props.disabled, false);
  await f.send().props.onClick();
  assert.equal(f.field("register-code").props.value, "");
  assert.equal(f.field("register-password").props.value, "synthetic long input");
  assert.equal(f.field("register-confirm").props.value, "synthetic typo input");
  f.change("register-confirm", "synthetic long input");
  f.change("register-code", "000002");
  await f.submit();
  assert.equal(f.registrations().length, 1);
  const body = JSON.parse(f.registrations()[0].options.body);
  assert.equal(body.verification_id, "synthetic-proof-2");
  assert.equal(body.verification_code, "000002");
});

test("new password uses scalar count and UTF8 byte bounds",()=>{const {validateNewPassword}=loadTypeScript("lib/auth-password.ts");for(const value of ["x".repeat(15),"😀".repeat(15),"😀".repeat(18)," ".repeat(15)])assert.equal(validateNewPassword(value),null);for(const value of ["x".repeat(14),"x".repeat(73),"😀".repeat(19),"x".repeat(15)+"\ud800"])assert.notEqual(validateNewPassword(value),null);});
test("register keeps leading-zero proof, invalidates changed email and redirects to Sign In",async t=>{const f=registrationFixture(t);await f.prepare("synthetic long input");await f.submit();assert.equal(JSON.parse(f.registrations()[0].options.body).verification_code,"000001");assert.equal(new URL(f.pushes[0],"https://example.com").pathname,"/login");f.change("register-email","b@example.test");assert.equal(find(f.driver.render(),n=>n.type==="button"&&n.props.type==="submit").props.disabled,true);assert.equal(f.field("register-code").props.value,"");assert.equal(find(f.driver.render(),n=>n.props?.role==="status"),null);});
test("reset reads fragment once, removes it without GET consumption and only POST consumes",async()=>{const calls=[],pushes=[],writes=[];let hash="#token=synthetic-token";globalThis.window={location:{get hash(){return hash;},pathname:"/reset-password",search:""},history:{replaceState(_a,_b,url){hash="";assert.equal(url,"/reset-password");}}};globalThis.localStorage={setItem:(...args)=>writes.push(args)};globalThis.sessionStorage={setItem:(...args)=>writes.push(args)};const d=componentDriver("app/(customer)/reset-password/reset-form.tsx",{"next/navigation":{useRouter:()=>({push:href=>pushes.push(href)})},"@/lib/auth-context":{useAuth:()=>({resetPassword:async(...args)=>calls.push(args)})}});d.render();d.runEffects();d.render();d.runEffects();assert.equal(hash,"");assert.equal(calls.length,0);assert.equal(writes.length,0);find(d.render(),n=>n.props?.id==="reset-password").props.onChange({target:{value:"synthetic long input"}});find(d.render(),n=>n.props?.id==="reset-confirm").props.onChange({target:{value:"synthetic long input"}});await find(d.render(),n=>n.type==="form").props.onSubmit({preventDefault(){}});assert.equal(calls.length,1);assert.equal(calls[0][0],"synthetic-token");assert.deepEqual(pushes,["/login?success=reset"]);});

for (const [label, password, confirmation, expectedError] of [
  ["mismatched confirmation", "synthetic long input", "synthetic typo input", "Passwords do not match."],
  ["matching short password", "x".repeat(14), "x".repeat(14), "Use at least 15 characters."],
  ["matching >72 byte password", "😀".repeat(19), "😀".repeat(19), "This password is too long. Use fewer characters."],
]) {
  test(`existing reset blocks POST for ${label}`, async t => {
    const previous = { window: globalThis.window, localStorage: globalThis.localStorage, fetch: globalThis.fetch };
    t.after(() => Object.assign(globalThis, previous));
    let hash = "#token=synthetic-token";
    globalThis.window = { location: { get hash() { return hash; }, pathname: "/reset-password" }, history: { replaceState() { hash = ""; } } };
    globalThis.localStorage = { getItem: () => null };
    const { apiClient } = loadTypeScript("lib/api/client.ts");
    const calls = [], pushes = [];
    globalThis.fetch = async (...args) => { calls.push(args); return new Response("{}"); };
    const d = componentDriver("app/(customer)/reset-password/reset-form.tsx", {
      "next/navigation": { useRouter: () => ({ push: href => pushes.push(href) }) },
      "@/lib/auth-context": { useAuth: () => ({ resetPassword: (token, value) => apiClient.resetPassword(token, value) }) },
    });
    d.render(); d.runEffects();
    for (const [id, value] of [["reset-password", password], ["reset-confirm", confirmation]]) find(d.render(), node => node.props?.id === id).props.onChange({ target: { value } });
    await find(d.render(), node => node.type === "form").props.onSubmit({ preventDefault() {} });
    assert.equal(calls.length, 0, "invalid reset must stop before HTTP");
    assert.equal(find(d.render(), node => node.props?.role === "alert")?.props.children, expectedError);
    assert.deepEqual(pushes, []);
  });
}
