import { readdir } from "node:fs/promises";
import { join, posix } from "node:path";

const roleNames = { guest: "访客", buyer: "购票人", staff: "活动工作人员", admin: "管理员" };
const sourceNames = { fixture: "本地合成数据", form: "静态表单" };
export const desktop = { width: 1440, height: 1000 };
export const mobile = { width: 390, height: 844 };
export const mobileRoutes = ["/", "/events/[id]", "/my-tickets"];

export async function findPages(root) {
  const pages = new Map();
  async function visit(parts) {
    const dir = join(root, "frontend", "app", ...parts);
    for (const item of await readdir(dir, { withFileTypes: true })) {
      if (item.isDirectory()) await visit([...parts, item.name]);
      if (item.name === "page.tsx") {
        const route = "/" + parts.filter(s => !s.startsWith("(")).join("/");
        pages.set(route, posix.join("frontend/app", ...parts, item.name));
      }
    }
  }
  await visit([]);
  return pages;
}

const code = value => String.fromCharCode(96) + value + String.fromCharCode(96);
const fileLink = path => path.replaceAll("(", "%28").replaceAll(")", "%29").replaceAll("[", "%5B").replaceAll("]", "%5D");
const imageLink = path => path.slice("docs/".length);

export function makeIndex(specs, pages, revision) {
  return specs.map(spec => {
    const group = spec.role === "admin" ? "admin" : "customer";
    const target = spec.aliasOf ? specs.find(s => s.route === spec.aliasOf) : spec;
    const screenshots = [{ file: "docs/demo/screenshots/" + group + "/" + target.slug + ".png", viewport: desktop }];
    if (mobileRoutes.includes(spec.route)) screenshots.push({ file: "docs/demo/screenshots/mobile/" + spec.slug + ".png", viewport: mobile });
    return {
      route: spec.route, exampleUrl: spec.url, ...(spec.entryUrl ? { entryUrl: spec.entryUrl } : {}),
      pageSource: pages.get(spec.route), title: spec.title,
      role: spec.role, locale: "zh-CN", timezone: "Asia/Shanghai", deviceScaleFactor: 1, dataSource: spec.source,
      frontendRevision: revision, generator: "docs/demo/capture-gallery.mjs",
      status: spec.aliasOf ? "alias" : "captured", ...(spec.aliasOf ? { aliasOf: spec.aliasOf } : {}), screenshots,
    };
  });
}

export function makeGallery(specs, index) {
  const lines = [
    "# RushGo 页面图册", "",
    "从活动发现、预订和持票入场，到退款审批和运维查询，下表列出 Next.js 路由树中的全部页面。点击索引里的页面名可阅读功能说明，点击图片链接可查看原图。", "",
    "截图由当前页面组件在本地 Chromium 浏览器渲染。动态页面共用海港爵士之夜的合成样例，购票人、活动工作人员和管理员分别使用独立的预览身份。索引中的“静态表单”指未提交的表单界面；受保护表单使用相应的合成身份。图中的金额、订单、票据和运营记录均为样例，付款、邮件、核销和审批没有实际执行。", "",
    "桌面视口 1440 × 1000，移动视口 390 × 844，像素比为 1。界面使用简体中文、深色主题和上海时区。样例时钟固定于 2027 年 5 月 20 日 11:00。", "",
    "[复现与交互预览](../DEMO.md) · [逐图来源清单](demo/screenshot-index.json) · [架构与业务流程](ARCHITECTURE.md)", "",
    "## 页面索引", "", "| 页面 | 路由 | 预览角色 | 截图 | 数据来源 | 覆盖 |", "| --- | --- | --- | --- | --- | --- |",
  ];
  for (const [i, s] of specs.entries()) {
    const row = index[i];
    lines.push("| [" + s.title + "](#page-" + s.slug + ") | " + code(s.route) + " | " + roleNames[s.role] + " | [桌面](" + imageLink(row.screenshots[0].file) + ") | " + sourceNames[s.source] + " | " + (s.aliasOf ? "共用目标页" : "已截图") + " |");
  }
  let lastGroup = "";
  for (const [i, s] of specs.entries()) {
    const row = index[i];
    const group = s.role === "admin" ? "管理端" : s.role === "staff" ? "工作人员" : "观众与购票人";
    if (group !== lastGroup) lines.push("", "## " + group, "");
    lastGroup = group;
    lines.push("", '<a id="page-' + s.slug + '"></a>', "", "### " + s.title, "",
      code(s.route) + " · " + roleNames[s.role] + " · [页面源码](../" + fileLink(row.pageSource) + ")", "", s.purpose, "",
      ...s.features.map(text => "- " + text), "", "**截图说明**　" + sourceNames[s.source] + "。" + s.state, "");
    if (s.aliasOf) lines.push("此入口转到 " + code(s.aliasOf) + "，见目标页截图。");
    else lines.push("![RushGo " + s.title + "](" + imageLink(row.screenshots[0].file) + ")");
  }
  lines.push("", "## 移动端", "", "三个窄屏样例使用相同组件和数据，可以比较导航、活动信息和票据操作在手机上的排列。");
  for (const row of index.filter(row => row.screenshots.length > 1)) {
    lines.push("", "### " + row.title, "", '<img src="' + imageLink(row.screenshots[1].file) + '" alt="RushGo 移动端' + row.title + '" width="320">');
  }
  return lines.join("\n") + "\n";
}
