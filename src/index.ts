import type { Plugin } from "@opencode-ai/plugin"
import { createWaitTool, type WaitToolInput, type UserMessageObservation } from "./wait-tool.js"

const WAIT_TOOL_DESCRIPTION = "Use for unattended/background waits that do not require user confirmation, including long-running tasks, Slurm jobs, external jobs, cooldowns, or expected notifications. Supports HTTP wake-up port, completion file, or user message."

const sessionMessageListeners = new Map<string, Set<(msg: UserMessageObservation) => void>>()

export function notifySessionUserMessage(sessionID: string, msg: UserMessageObservation) {
  const listeners = sessionMessageListeners.get(sessionID)
  if (listeners) {
    for (const listener of listeners) {
      try {
        listener(msg)
      } catch {}
    }
  }
}

export function subscribeSessionUserMessage(sessionID: string, callback: (msg: UserMessageObservation) => void): () => void {
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
