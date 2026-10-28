import * as http from "node:http"
import * as os from "node:os"
import * as fs from "node:fs/promises"
import * as path from "node:path"
import type { AddressInfo } from "node:net"

export type WakeServerOptions = {
  sessionID: string
  port?: number | "auto" | "off"
  basePort?: number
  portRange?: number
  directory?: string
  signal?: AbortSignal
}

export type WakeServerResult = {
  port: number
  host: string
  token: string
  signalFile?: string
  wakePromise: Promise<{ payload?: Record<string, unknown> | string; reason?: string }>
  close: () => Promise<void>
}

export function computeSessionPort(sessionID: string, basePort = 40000, portRange = 4096): number {
  if (!sessionID || typeof sessionID !== "string") return basePort
  let hash = 0
  for (let i = 0; i < sessionID.length; i++) {
    hash = (hash * 31 + sessionID.charCodeAt(i)) >>> 0
  }
  return basePort + (hash % portRange)
}

function parseBody(req: http.IncomingMessage): Promise<Record<string, unknown> | string | undefined> {
  return new Promise((resolve) => {
    let body = ""
    req.setEncoding("utf8")
    req.on("data", (chunk) => {
      if (body.length < 65536) body += chunk
    })
    req.on("end", () => {
      const trimmed = body.trim()
      if (!trimmed) return resolve(undefined)
      try {
        const json = JSON.parse(trimmed)
        if (typeof json === "object" && json !== null) {
          return resolve(json as Record<string, unknown>)
        }
      } catch {}
      resolve(trimmed)
    })
    req.on("error", () => resolve(undefined))
  })
}

function extractToken(req: http.IncomingMessage, parsedUrl: URL, body: unknown): string | undefined {
  const queryToken = parsedUrl.searchParams.get("token")
  if (queryToken) return queryToken

  const authHeader = req.headers["authorization"]
  if (authHeader && authHeader.toLowerCase().startsWith("bearer ")) {
    return authHeader.slice(7).trim()
  }

  const customHeader = req.headers["x-wait-token"]
  if (typeof customHeader === "string") return customHeader.trim()

  if (typeof body === "object" && body !== null && "token" in body) {
    const val = (body as Record<string, unknown>).token
    if (typeof val === "string") return val
  }

  return undefined
}

export async function startWakeServer(options: WakeServerOptions): Promise<WakeServerResult | undefined> {
  if (options.port === "off") return undefined

  const sessionID = options.sessionID || "default"
  const basePort = options.basePort ?? 40000
  const portRange = options.portRange ?? 4096
  const targetPort = typeof options.port === "number"
    ? options.port
    : computeSessionPort(sessionID, basePort, portRange)

  let wakeResolve: (value: { payload?: Record<string, unknown> | string; reason?: string }) => void
  const wakePromise = new Promise<{ payload?: Record<string, unknown> | string; reason?: string }>((resolve) => {
    wakeResolve = resolve
  })

  let server: http.Server | undefined
  let boundPort = targetPort
  let isClosed = false

  const serverHandler: http.RequestListener = async (req, res) => {
    const parsedUrl = new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`)
    const pathname = parsedUrl.pathname.replace(/\/+$/, "") || "/"

    if (pathname === "/health" || pathname === "/status") {
      res.writeHead(200, { "Content-Type": "application/json" })
      res.end(JSON.stringify({ status: "waiting", sessionID, port: boundPort }))
      return
    }

    if (pathname === "/wake" || pathname === "") {
      const body = await parseBody(req)
      const token = extractToken(req, parsedUrl, body)

      if (token !== sessionID) {
        res.writeHead(401, { "Content-Type": "application/json" })
        res.end(JSON.stringify({ status: "error", message: "Unauthorized: invalid or missing token" }))
        return
      }

      const queryPayload: Record<string, unknown> = {}
      for (const [key, val] of parsedUrl.searchParams.entries()) {
        if (key !== "token") queryPayload[key] = val
      }

      let payload: Record<string, unknown> | string | undefined
      if (typeof body === "object" && body !== null) {
        const bodyCopy = { ...body }
        delete (bodyCopy as Record<string, unknown>).token
        payload = { ...queryPayload, ...bodyCopy }
      } else if (typeof body === "string") {
        payload = Object.keys(queryPayload).length > 0 ? { ...queryPayload, message: body } : body
      } else {
        payload = Object.keys(queryPayload).length > 0 ? queryPayload : undefined
      }

      res.writeHead(200, { "Content-Type": "application/json" })
      res.end(JSON.stringify({ status: "ok", message: "Wake signal acknowledged", sessionID }))

      wakeResolve({ payload, reason: "port_signal" })
      return
    }

    res.writeHead(404, { "Content-Type": "application/json" })
    res.end(JSON.stringify({ status: "error", message: "Not found" }))
  }

  server = http.createServer(serverHandler)

  // Try targetPort, then a few increments, then port 0 (ephemeral)
  const portsToTry: number[] = []
  if (typeof options.port === "number") {
    portsToTry.push(options.port)
  } else {
    for (let i = 0; i < 5; i++) {
      portsToTry.push(basePort + ((targetPort - basePort + i) % portRange))
    }
    portsToTry.push(0) // Ephemeral fallback
  }

  let lastError: Error | undefined
  let listenSuccess = false

  for (const tryPort of portsToTry) {
    if (options.signal?.aborted) break
    try {
      await new Promise<void>((resolve, reject) => {
        const s = server!
        function onError(err: Error) {
          s.removeListener("listening", onListen)
          reject(err)
        }
        function onListen() {
          s.removeListener("error", onError)
          resolve()
        }
        s.once("error", onError)
        s.once("listening", onListen)
        s.listen(tryPort, "0.0.0.0")
      })
      listenSuccess = true
      break
    } catch (err: any) {
      lastError = err
      if (err.code !== "EADDRINUSE") {
        break
      }
    }
  }

  if (!listenSuccess || !server) {
    if (server) {
      server.close()
    }
    return undefined
  }

  const addr = server.address() as AddressInfo
  boundPort = addr.port
  const host = os.hostname()

  // Write rendezvous signal file in project directory or tmp
  let signalFilePath: string | undefined
  try {
    const dir = options.directory || process.cwd()
    const signalDir = path.join(dir, ".opencode")
    await fs.mkdir(signalDir, { recursive: true })
    signalFilePath = path.join(signalDir, `wait-${sessionID}.json`)
    await fs.writeFile(
      signalFilePath,
      JSON.stringify(
        {
          host,
          port: boundPort,
          token: sessionID,
          pid: process.pid,
          url: `http://${host}:${boundPort}/wake?token=${encodeURIComponent(sessionID)}`,
          started: new Date().toISOString(),
        },
        null,
        2
      )
    )
  } catch {
    // Non-fatal if writing rendezvous file fails
    signalFilePath = undefined
  }

  const close = async () => {
    if (isClosed) return
    isClosed = true
    if (signalFilePath) {
      await fs.unlink(signalFilePath).catch(() => {})
    }
    if (server) {
      try {
        server.close()
        // closeAllConnections is present in Node 18.2+
        if (typeof (server as any).closeAllConnections === "function") {
          ;(server as any).closeAllConnections()
        }
      } catch {}
    }
  }

  if (options.signal) {
    options.signal.addEventListener("abort", () => {
      close().catch(() => {})
    }, { once: true })
  }

  return {
    port: boundPort,
    host,
    token: sessionID,
    signalFile: signalFilePath,
    wakePromise,
    close,
  }
}
