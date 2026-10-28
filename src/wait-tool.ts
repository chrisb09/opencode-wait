import { tool } from "@opencode-ai/plugin"
import { startWakeServer, computeSessionPort, type WakeServerResult } from "./wake-server.js"
import { waitForFileSentinel } from "./file-watcher.js"

export { computeSessionPort }
export type { WakeServerResult }

export type WaitToolInput = {
  now?: () => number
  sleep?: (ms: number, signal?: AbortSignal) => Promise<void>
  pollIntervalMs?: number
  filePollIntervalMs?: number
  progressIntervalMs?: number
  enablePort?: boolean
  basePort?: number
  portRange?: number
  onUserMessage?: (sessionID: string, callback: (msg: UserMessageObservation) => void) => () => void
  client?: {
    session?: {
      messages?: (input: {
        path: { id: string }
        query?: {
          directory?: string
          limit?: number
        }
      }) => Promise<{ data?: SessionMessage[] } | SessionMessage[] | undefined>
    }
  }
}

type SessionMessage = {
  info?: {
    id?: unknown
    role?: unknown
    time?: {
      created?: unknown
    }
  }
  parts?: unknown[]
}

export type UserMessageObservation = {
  id: string
  created?: number
  synthetic: boolean
}

type WaitUntilEvent = "new_user_message"

const DEFAULT_USER_MESSAGE_POLL_INTERVAL_MS = 5_000
const USER_MESSAGE_LOOKBACK_LIMIT = 20

function toIso(ms: number) {
  return new Date(ms).toISOString()
}

function normalizeSeconds(value: unknown) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed) || parsed <= 0) return 0
  return Math.floor(parsed)
}

function normalizePollIntervalMs(value: unknown) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed) || parsed <= 0) return DEFAULT_USER_MESSAGE_POLL_INTERVAL_MS
  return Math.max(1, Math.floor(parsed))
}

function normalizeUntil(value: unknown): WaitUntilEvent | undefined {
  return value === "new_user_message" ? value : undefined
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null
}

function toUserMessageObservation(message: SessionMessage): UserMessageObservation | undefined {
  const info = message.info
  if (info?.role !== "user") return undefined
  if (typeof info.id !== "string" || info.id.length === 0) return undefined

  const created = typeof info.time?.created === "number" && Number.isFinite(info.time.created)
    ? info.time.created
    : undefined
  const synthetic = (message.parts ?? []).some((part) => isRecord(part) && part.synthetic === true)
  return { id: info.id, created, synthetic }
}

function abortableSleep(ms: number, signal?: AbortSignal): Promise<void> {
  const createAbortError = () => {
    const error = new Error("Aborted")
    error.name = "AbortError"
    return error
  }

  if (signal?.aborted) return Promise.reject(createAbortError())

  return new Promise((resolve, reject) => {
    let timeout: ReturnType<typeof setTimeout>
    function cleanup() {
      signal?.removeEventListener("abort", onAbort)
    }
    function onAbort() {
      clearTimeout(timeout)
      cleanup()
      reject(createAbortError())
    }

    timeout = setTimeout(() => {
      cleanup()
      resolve()
    }, ms)
    signal?.addEventListener("abort", onAbort, { once: true })
  })
}

function formatDuration(ms: number) {
  const seconds = Math.max(0, Math.floor(ms / 1_000))
  return `${Math.floor(seconds / 60).toString().padStart(2, "0")}:${(seconds % 60).toString().padStart(2, "0")}`
}

function progressBar(elapsed: number, total?: number) {
  if (!total) return "[..........]"
  const filled = Math.min(10, Math.round((elapsed / total) * 10))
  return `[${"#".repeat(filled)}${"-".repeat(10 - filled)}]`
}

function publishMetadata(context: any, input: { title?: string; metadata?: Record<string, unknown> }) {
  try {
    const res: unknown = context?.metadata?.(input)
    if (res && typeof res === "object" && "then" in res && typeof (res as Promise<unknown>).then === "function") {
      (res as Promise<unknown>).catch(() => {})
    }
  } catch {}
}

function waitForAbort(signal?: AbortSignal) {
  if (!signal) return { promise: new Promise<never>(() => {}), cleanup() {} }
  if (signal.aborted) {
    const err = new Error("Wait aborted")
    err.name = "AbortError"
    return { promise: Promise.reject(err), cleanup() {} }
  }
  let onAbort: (() => void) | undefined
  return {
    promise: new Promise<never>((_, reject) => {
      onAbort = () => {
        const err = new Error("Wait aborted")
        err.name = "AbortError"
        reject(err)
      }
      signal.addEventListener("abort", onAbort, { once: true })
    }),
    cleanup() {
      if (onAbort) signal.removeEventListener("abort", onAbort)
    },
  }
}

async function readRecentUserMessages(input: {
  client: WaitToolInput["client"]
  sessionID: string
  directory: string
}): Promise<UserMessageObservation[] | undefined> {
  const response = await input.client?.session?.messages?.({
    path: { id: input.sessionID },
    query: {
      directory: input.directory,
      limit: USER_MESSAGE_LOOKBACK_LIMIT,
    },
  })
  const messages = Array.isArray(response)
    ? response
    : Array.isArray(response?.data)
      ? response.data
      : undefined

  return messages
    ?.map((message) => toUserMessageObservation(message))
    .filter((message): message is UserMessageObservation => message !== undefined)
}

async function waitForNewUserMessage(input: {
  client: WaitToolInput["client"]
  sessionID: string
  directory: string
  currentMessageID: string
  startedAt: number
  pollIntervalMs: number
  signal: AbortSignal
  onUserMessage?: WaitToolInput["onUserMessage"]
}): Promise<UserMessageObservation | undefined> {
  let resolveEvent: ((msg: UserMessageObservation) => void) | undefined
  const eventPromise = new Promise<UserMessageObservation>((resolve) => {
    resolveEvent = resolve
  })

  let unsubscribe: (() => void) | undefined
  if (input.onUserMessage) {
    unsubscribe = input.onUserMessage(input.sessionID, (msg) => {
      if (msg.id !== input.currentMessageID && (msg.created === undefined || msg.created > input.startedAt)) {
        resolveEvent?.(msg)
      }
    })
  }

  const cleanupSubscription = () => {
    if (unsubscribe) {
      unsubscribe()
      unsubscribe = undefined
    }
  }

  // If in-process hook is provided, we race the event promise with signal abort
  if (input.onUserMessage && !input.client?.session?.messages) {
    const onAbort = new Promise<undefined>((resolve) => {
      input.signal.addEventListener("abort", () => resolve(undefined), { once: true })
    })
    try {
      return await Promise.race([eventPromise, onAbort])
    } finally {
      cleanupSubscription()
    }
  }

  // Polling fallback (when client.session.messages is provided or in tests)
  if (!input.client?.session?.messages) {
    cleanupSubscription()
    return undefined
  }

  const initialMessages = await readRecentUserMessages(input).catch(() => undefined)
  if (!initialMessages) {
    if (input.onUserMessage) {
      const onAbort = new Promise<undefined>((resolve) => {
        input.signal.addEventListener("abort", () => resolve(undefined), { once: true })
      })
      try {
        return await Promise.race([eventPromise, onAbort])
      } finally {
        cleanupSubscription()
      }
    }
    cleanupSubscription()
    return undefined
  }
  const messageAlreadyArrived = initialMessages.find((message) =>
    message.id !== input.currentMessageID && message.created !== undefined && message.created > input.startedAt
  )
  if (messageAlreadyArrived) {
    cleanupSubscription()
    return messageAlreadyArrived
  }

  const knownMessageIDs = new Set(initialMessages.map((message) => message.id))
  knownMessageIDs.add(input.currentMessageID)

  while (!input.signal.aborted) {
    const sleepOrEvent = Promise.race([
      abortableSleep(input.pollIntervalMs, input.signal).catch(() => undefined),
      eventPromise.then((msg) => ({ msg })),
    ])
    const step = await sleepOrEvent
    if (input.signal.aborted) {
      cleanupSubscription()
      return undefined
    }
    if (step && typeof step === "object" && "msg" in step) {
      cleanupSubscription()
      return step.msg
    }

    const nextMessages = await readRecentUserMessages(input).catch(() => undefined)
    if (!nextMessages) {
      cleanupSubscription()
      return undefined
    }

    const newMessage = nextMessages.find((message) => !knownMessageIDs.has(message.id))
    if (newMessage) {
      cleanupSubscription()
      return newMessage
    }

    for (const message of nextMessages) {
      knownMessageIDs.add(message.id)
    }
  }

  cleanupSubscription()
  return undefined
}

function formatUserMessageResult(input: {
  started: number
  finished: number
  seconds?: number
  eventMode: boolean
  message: UserMessageObservation
  purpose?: string
}) {
  const waited = Math.max(0, Math.floor((input.finished - input.started) / 1000))
  const reason = input.message.synthetic ? "new user message (synthetic)" : "new user message"
  const parts = [
    `started: ${toIso(input.started)}`,
    input.seconds === undefined ? undefined : `requested: ${input.seconds}s`,
    `waited: ${waited}s`,
    `now: ${toIso(input.finished)}`,
    `ended: ${input.eventMode ? "event" : "early"}`,
  ]
  if (input.eventMode) parts.push("event: new_user_message")
  parts.push(`reason: ${reason}`, `message: ${input.message.id}`)
  if (input.purpose && input.purpose !== "Unspecified wait") {
    parts.push(`purpose: ${input.purpose}`)
  }
  return parts.filter((part): part is string => part !== undefined).join("; ")
}

function formatUnavailableEventResult(input: {
  started: number
  finished: number
  event: WaitUntilEvent
  purpose?: string
}) {
  const waited = Math.max(0, Math.floor((input.finished - input.started) / 1000))
  let res = `started: ${toIso(input.started)}; waited: ${waited}s; now: ${toIso(input.finished)}; ended: unavailable; event: ${input.event}; reason: session messages unavailable`
  if (input.purpose && input.purpose !== "Unspecified wait") {
    res += `; purpose: ${input.purpose}`
  }
  return res
}

export function createWaitTool(input: WaitToolInput = {}) {
  const now = input.now ?? (() => Date.now())
  const sleep = input.sleep ?? abortableSleep
  const pollIntervalMs = normalizePollIntervalMs(input.pollIntervalMs)
  const defaultEnablePort = input.enablePort

  return tool({
    description: "Use for unattended/background waits that do not require user confirmation, including long-running tasks, Slurm jobs, cooldowns, or expected notifications. Can be signaled to wake up via HTTP port (default 40000 + hash(sessionID)), completion file, or user message.",
    args: {
      seconds: tool.schema.number().optional().describe("How long to wait in seconds. Positive values are honored as requested; missing, invalid, or non-positive values complete immediately."),
      until: tool.schema.string().optional().describe("Event to wait for: 'new_user_message', 'port_signal', or 'file'."),
      reason: tool.schema.string().optional().describe("Optional explanation of why the agent is waiting (shown in live progress and logs)."),
      port: tool.schema.union([tool.schema.number(), tool.schema.literal("auto"), tool.schema.literal("off")]).optional().describe("HTTP wake-up port. Defaults to 'auto' (40000 + hash(sessionID) % 4096). Set to 'off' to disable."),
      file: tool.schema.string().optional().describe("Optional path to a sentinel file to wait for (e.g. 'job.done'). If present, wait ends when the file appears."),
      deleteFile: tool.schema.boolean().optional().describe("Whether to delete the sentinel file after detecting it. Defaults to true."),
      readContent: tool.schema.boolean().optional().describe("Whether to read and return the sentinel file content upon wake-up. Defaults to true."),
      pollIntervalSeconds: tool.schema.number().optional().describe("Check interval in seconds for file sentinel (defaults to 5s, keeps CPU at 0%)."),
    },
    async execute(args, context) {
      const until = normalizeUntil(args.until)
      const eventOnly = until === "new_user_message" && args.seconds === undefined
      const seconds = eventOnly ? undefined : normalizeSeconds(args.seconds)
      const started = now()
      const purpose = typeof args.reason === "string" && args.reason.trim()
        ? args.reason.trim()
        : "Unspecified wait"

      if (eventOnly && !input.client?.session?.messages) {
        return formatUnavailableEventResult({
          started,
          finished: now(),
          event: until,
          purpose,
        })
      }

      const controller = new AbortController()

      // Determine whether port listener should start
      const hasSpecificEvent = eventOnly || args.file || (args.until && args.until !== "new_user_message") || args.port === "auto" || typeof args.port === "number"
      const shouldEnablePort = (seconds === 0 && !hasSpecificEvent)
        ? false
        : (args.port === "off"
          ? false
          : (args.port !== undefined ? true : (defaultEnablePort ?? (context.sessionID ? true : false))))

      let wakeServer: WakeServerResult | undefined
      if (shouldEnablePort) {
        wakeServer = await startWakeServer({
          sessionID: context.sessionID,
          port: args.port,
          basePort: input.basePort,
          portRange: input.portRange,
          directory: context.directory,
          signal: controller.signal,
        }).catch(() => undefined)
      }

      // Progress reporting setup
      const progressIntervalMs = input.progressIntervalMs ?? (
        seconds === undefined
          ? 15_000
          : seconds <= 30
            ? 5_000
            : Math.min(30_000, Math.max(5_000, Math.floor((seconds * 1_000) / 10)))
      )

      const reportProgress = (status: "waiting" | "interrupted" = "waiting") => {
        const elapsedMs = Math.max(0, now() - started)
        const totalMs = seconds === undefined ? undefined : seconds * 1_000
        const remainingMs = totalMs === undefined ? undefined : Math.max(0, totalMs - elapsedMs)
        const bar = progressBar(elapsedMs, totalMs)
        const titleParts = [`Waiting: ${purpose}`]
        if (wakeServer) titleParts.push(`(listening :${wakeServer.port})`)
        publishMetadata(context, {
          title: titleParts.join(" "),
          metadata: {
            kind: "wait",
            purpose,
            status,
            started,
            elapsedMs,
            totalMs,
            remainingMs,
            bar,
            port: wakeServer?.port,
          },
        })
      }

      reportProgress("waiting")
      let progressTimer: ReturnType<typeof setInterval> | undefined
      if (seconds === undefined || seconds > 5) {
        progressTimer = setInterval(() => reportProgress("waiting"), progressIntervalMs)
      }

      const outcomes: Promise<
        | { type: "timeout" }
        | { type: "unavailable"; event: WaitUntilEvent }
        | { type: "user-message"; message: UserMessageObservation }
        | { type: "port-signal"; payload?: Record<string, unknown> | string; port: number }
        | { type: "file-ready"; file: string; content?: string }
      >[] = []

      if (seconds !== undefined) {
        outcomes.push(
          sleep(seconds * 1000, controller.signal).then(
            () => ({ type: "timeout" as const }),
            (error) => {
              if (controller.signal.aborted) return new Promise<never>(() => {})
              throw error
            }
          )
        )
      }

      if (wakeServer) {
        const boundPort = wakeServer.port
        outcomes.push(
          wakeServer.wakePromise.then((wakeResult) => ({
            type: "port-signal" as const,
            payload: wakeResult.payload,
            port: boundPort,
          }))
        )
      }

      if (args.file) {
        const filePollMs = typeof args.pollIntervalSeconds === "number" && args.pollIntervalSeconds > 0
          ? Math.max(100, Math.floor(args.pollIntervalSeconds * 1000))
          : (input.filePollIntervalMs ?? 5_000)

        outcomes.push(
          waitForFileSentinel({
            filePath: args.file,
            directory: context.directory,
            readContent: args.readContent !== false,
            deleteFile: args.deleteFile !== false,
            pollIntervalMs: filePollMs,
            signal: controller.signal,
          }).then((fileResult) => {
            if (!fileResult) return new Promise<never>(() => {})
            return {
              type: "file-ready" as const,
              file: fileResult.file,
              content: fileResult.content,
            }
          })
        )
      }

      outcomes.push(
        waitForNewUserMessage({
          client: input.client,
          sessionID: context.sessionID,
          directory: context.directory,
          currentMessageID: context.messageID,
          startedAt: started,
          pollIntervalMs,
          signal: controller.signal,
          onUserMessage: input.onUserMessage,
        }).then((message) => {
          if (!message) {
            if (eventOnly) return { type: "unavailable" as const, event: "new_user_message" as const }
            return new Promise<never>(() => {})
          }
          return { type: "user-message" as const, message }
        })
      )

      const aborted = waitForAbort(context.abort)

      let outcome: Awaited<(typeof outcomes)[number]>
      try {
        outcome = await Promise.race([...outcomes, aborted.promise])
      } catch (error) {
        if (context.abort?.aborted) {
          reportProgress("interrupted")
        }
        throw error
      } finally {
        aborted.cleanup()
        if (progressTimer) clearInterval(progressTimer)
        controller.abort()
        if (wakeServer) {
          await wakeServer.close().catch(() => {})
        }
      }

      const finished = now()
      const waited = Math.max(0, Math.floor((finished - started) / 1000))

      if (outcome.type === "unavailable") {
        return formatUnavailableEventResult({
          started,
          finished,
          event: outcome.event,
          purpose,
        })
      }

      if (outcome.type === "user-message") {
        return formatUserMessageResult({
          started,
          finished,
          seconds,
          eventMode: eventOnly,
          message: outcome.message,
          purpose,
        })
      }

      if (outcome.type === "port-signal") {
        const payloadStr = outcome.payload !== undefined
          ? `; payload: ${typeof outcome.payload === "string" ? outcome.payload : JSON.stringify(outcome.payload)}`
          : ""
        const purposeStr = purpose !== "Unspecified wait" ? `; purpose: ${purpose}` : ""
        return `started: ${toIso(started)}; waited: ${waited}s; now: ${toIso(finished)}; ended: port_signal; port: ${outcome.port}${payloadStr}${purposeStr}`
      }

      if (outcome.type === "file-ready") {
        const contentStr = outcome.content !== undefined ? `; content: ${outcome.content.trim()}` : ""
        const purposeStr = purpose !== "Unspecified wait" ? `; purpose: ${purpose}` : ""
        return `started: ${toIso(started)}; waited: ${waited}s; now: ${toIso(finished)}; ended: file_ready; file: ${outcome.file}${contentStr}${purposeStr}`
      }

      const purposeStr = purpose !== "Unspecified wait" ? `; purpose: ${purpose}` : ""
      return `started: ${toIso(started)}; waited: ${seconds}s; now: ${toIso(finished)}${purposeStr}`
    },
  })
}
