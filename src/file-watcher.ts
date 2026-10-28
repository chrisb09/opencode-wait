import * as fs from "node:fs/promises"
import * as path from "node:path"

export type FileWatcherOptions = {
  filePath: string
  directory?: string
  readContent?: boolean
  deleteFile?: boolean
  pollIntervalMs?: number
  signal: AbortSignal
  sleep?: (ms: number, signal?: AbortSignal) => Promise<void>
}

export type FileWatcherResult = {
  file: string
  content?: string
}

function defaultSleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) return Promise.reject(new Error("Aborted"))
  return new Promise((resolve, reject) => {
    let timer: ReturnType<typeof setTimeout>
    function onAbort() {
      clearTimeout(timer)
      signal?.removeEventListener("abort", onAbort)
      reject(new Error("Aborted"))
    }
    timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort)
      resolve()
    }, ms)
    signal?.addEventListener("abort", onAbort, { once: true })
  })
}

export async function waitForFileSentinel(options: FileWatcherOptions): Promise<FileWatcherResult | undefined> {
  const dir = options.directory || process.cwd()
  const resolvedPath = path.isAbsolute(options.filePath)
    ? options.filePath
    : path.resolve(dir, options.filePath)

  const pollIntervalMs = Math.max(100, options.pollIntervalMs ?? 5_000)
  const sleep = options.sleep ?? defaultSleep

  while (!options.signal.aborted) {
    try {
      const stat = await fs.stat(resolvedPath)
      if (stat.isFile() || stat.size >= 0) {
        let content: string | undefined
        if (options.readContent !== false) {
          try {
            content = await fs.readFile(resolvedPath, "utf8")
          } catch {
            content = undefined
          }
        }
        if (options.deleteFile !== false) {
          try {
            await fs.unlink(resolvedPath)
          } catch {}
        }
        return { file: resolvedPath, content }
      }
    } catch (err: any) {
      if (err.code !== "ENOENT") {
        // Unexpected filesystem error (e.g. permission error)
      }
    }

    try {
      await sleep(pollIntervalMs, options.signal)
    } catch {
      break
    }
  }

  return undefined
}
