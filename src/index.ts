import type { Plugin } from "@opencode-ai/plugin"
import { createWaitTool, type WaitToolInput, type UserMessageObservation } from "./wait-tool.js"

const WAIT_TOOL_DESCRIPTION = "Use for unattended/background waits (Slurm jobs, background processes, cooldowns) with 0% CPU. Can be woken up by external programs in two ways: 1) HTTP signal: an external job runs 'curl -s http://<host>:<port>/wake?token=<sessionID>&exitCode=0' (port defaults to 40000 + hash(sessionID) % 4096, and connection details are written to .opencode/wait-<sessionID>.json during wait). 2) Sentinel file: pass file: 'job.done' and have the job run 'echo $? > job.done' upon completion. Also ends on user message, context cancellation, or seconds timeout."

const sessionMessageListeners = new Map<string, Set<(msg: UserMessageObservation) => void>>()

function notifySessionUserMessage(sessionID: string, msg: UserMessageObservation) {
  const listeners = sessionMessageListeners.get(sessionID)
  if (listeners) {
    for (const listener of listeners) {
      try {
        listener(msg)
      } catch {}
    }
  }
}

function subscribeSessionUserMessage(sessionID: string, callback: (msg: UserMessageObservation) => void): () => void {
  let set = sessionMessageListeners.get(sessionID)
  if (!set) {
    set = new Set()
    sessionMessageListeners.set(sessionID, set)
  }
  set.add(callback)
  return () => {
    set?.delete(callback)
    if (set && set.size === 0) {
      sessionMessageListeners.delete(sessionID)
    }
  }
}

export { createWaitTool }
export type { WaitToolInput, UserMessageObservation }

export function createWaitPlugin(waitInput: WaitToolInput = {}): Plugin {
  return async (input) => ({
    tool: {
      wait: createWaitTool({
        onUserMessage: subscribeSessionUserMessage,
        ...waitInput,
        client: waitInput.client ?? input.client,
      }),
    },
    "tool.definition": async (hookInput, output) => {
      if (hookInput.toolID === "wait") {
        output.description = WAIT_TOOL_DESCRIPTION
      }
    },
    "chat.message": async (msgInput, output) => {
      const sessionID = msgInput.sessionID
      if (!sessionID) return
      const isSynthetic = (output.parts ?? []).some((p: any) => p && typeof p === "object" && p.synthetic === true)
      notifySessionUserMessage(sessionID, {
        id: msgInput.messageID ?? `msg-${Date.now()}`,
        created: Date.now(),
        synthetic: isSynthetic,
      })
    },
  })
}

export const WaitPlugin: Plugin = createWaitPlugin()

export default WaitPlugin
