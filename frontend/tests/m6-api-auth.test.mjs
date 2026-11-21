import assert from "node:assert/strict";
import { test } from "node:test";
import { loadTypeScript } from "./render-typescript.mjs";
function fixture(initial={}) {const tokens=new Map(Object.entries(initial)),calls=[];globalThis.window={};globalThis.localStorage={getItem:k=>tokens.get(k)||null,setItem:(k,v)=>tokens.set(k,v),removeItem:k=>tokens.delete(k)};const {apiClient}=loadTypeScript("lib/api/client.ts");globalThis.fetch=async(url,options)=>{calls.push({url,options});return new Response(JSON.stringify({account_created:true,message:"synthetic"}),{status:201})};return {apiClient,tokens,calls};}
test("registration writes no session credentials",async()=>{const f=fixture();await f.apiClient.register({email:"new@example.com",password:"synthetic long input",name:"M6",verification_id:"fixture",verification_code:"000001"});assert.equal(f.tokens.size,0);assert.equal(JSON.parse(f.calls[0].options.body).verification_code,"000001");});
test("public recovery never authorizes or refreshes a valid session",async()=>{const f=fixture({access_token:"synthetic",refresh_token:"synthetic"});globalThis.fetch=async(url,options)=>{f.calls.push({url,options});return new Response(JSON.stringify({code:"INVALID_RESET_TOKEN",error:"synthetic provider secret"}),{status:401})};await assert.rejects(f.apiClient.resetPassword("synthetic","synthetic long input"),e=>e.code==="INVALID_RESET_TOKEN"&&!e.message.includes("synthetic provider secret"));assert.equal(f.calls.length,1);assert.equal(f.calls[0].options.headers.Authorization,undefined);assert.equal(f.tokens.size,2);});
test("logout carries current refresh once and clears despite server failure",async()=>{const f=fixture({access_token:"synthetic-access",refresh_token:"synthetic-refresh"});globalThis.fetch=async(url,options)=>{f.calls.push({url,options});return new Response('{}',{status:503})};await assert.rejects(f.apiClient.logout());assert.equal(f.calls.length,1);assert.equal(JSON.parse(f.calls[0].options.body).refresh_token,"synthetic-refresh");assert.equal(f.tokens.size,0);});
test("logout awaits active rotating refresh before capturing credential",async()=>{const f=fixture({access_token:"old",refresh_token:"old-refresh"});let entered,release;const started=new Promise(resolve=>entered=resolve);globalThis.fetch=async(url,options)=>{f.calls.push({url,options});if(url==="/api/auth/refresh"){entered();return new Promise(resolve=>release=()=>resolve(new Response(JSON.stringify({access_token:"rotated",refresh_token:"rotated-refresh"}))));}if(url==="/api/auth/logout")return new Response('{}');return new Response('{}',{status:401});};const read=f.apiClient.getProfile().catch(()=>{});await started;const logout=f.apiClient.logout();release();await Promise.all([read,logout]);const calls=f.calls.filter(c=>c.url==="/api/auth/logout");assert.equal(calls.length,1);assert.equal(JSON.parse(calls[0].options.body).refresh_token,"rotated-refresh");assert.equal(f.tokens.size,0);});
test("a protected 401 during logout cannot erase the credential before server revocation",async()=>{const f=fixture({access_token:"old",refresh_token:"old-refresh"});let entered,release;const started=new Promise(resolve=>entered=resolve);globalThis.fetch=async(url,options)=>{f.calls.push({url,options});if(url==="/api/auth/refresh"){entered();return new Promise(resolve=>release=()=>resolve(new Response(JSON.stringify({access_token:"rotated",refresh_token:"rotated-refresh"}))));}if(url==="/api/auth/logout")return new Response('{}');return new Response('{}',{status:401});};const oldRead=f.apiClient.getProfile().catch(()=>{});await started;const logout=f.apiClient.logout();await f.apiClient.listBookings().catch(()=>{});release();await Promise.all([oldRead,logout]);const calls=f.calls.filter(c=>c.url==="/api/auth/logout");assert.equal(calls.length,1);assert.equal(JSON.parse(calls[0].options.body).refresh_token,"rotated-refresh");assert.equal(f.tokens.size,0);});
for (const failure of [401, 503, "network"]) {
  test(`logout reports server failure after an active refresh fails with ${failure}`, async () => {
    const f = fixture({ access_token: "old", refresh_token: "old-refresh" });
    let entered, release;
    const started = new Promise(resolve => entered = resolve);
    globalThis.fetch = async (url, options) => {
      f.calls.push({ url, options });
      if (url === "/api/auth/refresh") {
        entered();
        return new Promise((resolve, reject) => release = () => failure === "network" ? reject(new Error("synthetic transport failure")) : resolve(new Response("{}", { status: failure })));
      }
      return new Response("{}", { status: url === "/api/auth/logout" ? 503 : 401 });
    };
    const read = f.apiClient.getProfile().catch(() => {});
    await started;
    const logout = f.apiClient.logout();
    const rejected = assert.rejects(logout, error => error.code === "TEMPORARY_UNAVAILABLE" && error.status === 503);
    release();
    await Promise.all([read, rejected]);
    const termination = f.calls.filter(call => call.url === "/api/auth/logout");
    assert.equal(termination.length, 1);
    assert.equal(JSON.parse(termination[0].options.body).refresh_token, "old-refresh");
    assert.equal(f.tokens.size, 0);
  });
}
test("logout waiting on an old refresh preserves a genuinely newer login", async () => {
  const f = fixture({ access_token: "old", refresh_token: "old-refresh" });
  let entered, release;
  const started = new Promise(resolve => entered = resolve);
  globalThis.fetch = async (url, options) => {
    f.calls.push({ url, options });
    if (url === "/api/auth/refresh") { entered(); return new Promise(resolve => release = () => resolve(new Response("{}", { status: 401 }))); }
    if (url === "/api/auth/login") return new Response(JSON.stringify({ access_token: "new-login", refresh_token: "new-login-refresh" }));
    return new Response("{}", { status: 401 });
  };
  const read = f.apiClient.getProfile().catch(() => {});
  await started;
  const logout = f.apiClient.logout();
  await f.apiClient.login("synthetic@example.com", "synthetic long input");
  release();
  await Promise.all([read, logout]);
  assert.equal(f.tokens.get("refresh_token"), "new-login-refresh");
  assert.equal(f.calls.filter(call => call.url === "/api/auth/logout").length, 0);
});
