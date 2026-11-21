import assert from "node:assert/strict";
import test from "node:test";
import fs from "node:fs/promises";
import ts from "typescript";

test("scheduler update serializes the displayed version across retries", async () => {
  const source = await fs.readFile(new URL("../lib/api/client.ts", import.meta.url), "utf8");
  const code = ts.transpileModule(source, {compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText;
  let request;
  const fetch = async (_url, options) => {request=JSON.parse(options.body);return {ok:true,status:200,json:async()=>({})};};
  const compiledModule={exports:{}};
  new Function("require","module","exports","fetch","localStorage",code)((name)=>{
    if(name==="./errors")return {ApiError:class extends Error{},decodeApiError:()=>new Error("fixture")};
    return {};
  },compiledModule,compiledModule.exports,fetch,{getItem:()=>null,setItem(){},removeItem(){}});
  await compiledModule.exports.apiClient.updateSchedulerJob({id:"fixture",is_enable:false,cron_expression:"*/5 * * * * *",timeout:10,expected_version:7});
  assert.equal(request.expected_version,7);
});
