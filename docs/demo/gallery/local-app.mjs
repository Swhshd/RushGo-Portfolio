import { createServer } from "node:http";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { ids } from "./sample.mjs";

export const root = fileURLToPath(new URL("../../../", import.meta.url));
const frontend = fileURLToPath(new URL("../../../frontend/", import.meta.url));
export const frontendRequire = createRequire(new URL("../../../frontend/package.json", import.meta.url));
export const streamPath = "/api/events/" + ids.event + "/seats/stream";

// Serve only this checkout's built pages and static assets. The API catch-all
// sits before Next so no browser request can reach its configured rewrites.
export async function startGalleryApp(paths) {
  process.env.NEXT_TELEMETRY_DISABLED = "1";
  const next = frontendRequire("next");
  const app = next({ dev: false, dir: frontend, hostname: "127.0.0.1" });
  await app.prepare();
  const handle = app.getRequestHandler();
  const server = createServer((req, res) => {
    const path = new URL(req.url, "http://127.0.0.1").pathname;
    if (req.method !== "GET" && req.method !== "HEAD") {
      res.writeHead(405).end();
    } else if (path === streamPath) {
      res.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-store" });
      res.write("event: ready\ndata: {}\n\n");
    } else if (paths.has(path) || path.startsWith("/_next/static/")) {
      handle(req, res).catch(() => res.writeHead(500).end());
    } else {
      res.writeHead(404).end();
    }
  });
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  return {
    origin: "http://127.0.0.1:" + server.address().port,
    async close() {
      server.closeAllConnections();
      await new Promise(resolve => server.close(resolve));
      await app.close();
    },
  };
}
