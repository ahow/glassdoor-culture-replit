import type { Express } from "express";
import { createServer, type Server } from "http";
import { createProxyMiddleware } from "http-proxy-middleware";
import { spawn } from "child_process";

let flaskProcess: ReturnType<typeof spawn> | null = null;

function startFlask() {
  if (flaskProcess) return;
  
  console.log("Starting Flask backend on port 8080...");
  flaskProcess = spawn("python", ["app.py"], {
    env: { ...process.env, FLASK_PORT: "8080" },
    stdio: ["ignore", "pipe", "pipe"],
    cwd: process.cwd(),
  });
  
  flaskProcess.stdout?.on("data", (data) => {
    console.log(`[Flask] ${data.toString().trim()}`);
  });
  
  flaskProcess.stderr?.on("data", (data) => {
    console.log(`[Flask] ${data.toString().trim()}`);
  });
  
  flaskProcess.on("close", (code) => {
    console.log(`Flask process exited with code ${code}`);
    flaskProcess = null;
  });
}

export async function registerRoutes(
  httpServer: Server,
  app: Express
): Promise<Server> {
  startFlask();
  
  await new Promise((resolve) => setTimeout(resolve, 3000));
  
  const flaskProxy = createProxyMiddleware({
    target: "http://localhost:8080",
    changeOrigin: true,
    on: {
      proxyReq: (proxyReq, req: any) => {
        if (req.body !== undefined && req.body !== null) {
          const bodyData = JSON.stringify(req.body);
          proxyReq.setHeader('Content-Type', 'application/json');
          proxyReq.setHeader('Content-Length', Buffer.byteLength(bodyData));
          proxyReq.write(bodyData);
        }
      },
    },
  });

  app.use(flaskProxy);

  return httpServer;
}
