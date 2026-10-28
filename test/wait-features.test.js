import assert from "node:assert/strict"
import test from "node:test"
import fs from "node:fs/promises"
import path from "node:path"
import os from "node:os"

import { createWaitTool } from "../dist/wait-tool.js"
import { createWaitPlugin } from "../dist/index.js"

function createContext(overrides = {}) {
  return {
    sessionID: "test-session-123",
    messageID: "m1",
    agent: "task",
    directory: "/tmp/project",
    worktree: "/tmp/project",
    abort: new AbortController().signal,
    metadata() {},
    async ask() {},
    ...overrides,
  }
}

test("wait tool wakes up on HTTP port signal with matching token and payload", async () => {
  const wait = createWaitTool({
    sleep: () => new Promise(() => {}), // infinite sleep
    now: (() => {
      let tick = 0
      return () => 1_700_000_000_000 + tick++ * 10_000
    })(),
  })

  const context = createContext({ sessionID: "sess-port-test-1" })
  let metadataPort = 0
  context.metadata = (input) => {
    if (input.metadata?.port) metadataPort = input.metadata.port
  }

  const waitPromise = wait.execute({ seconds: 3600, reason: "Waiting for simulation" }, context)

  // Poll briefly until server is up and metadata reports port
  let port = 0
  for (let i = 0; i < 50; i++) {
    await new Promise((r) => setTimeout(r, 20))
    if (metadataPort > 0) {
      port = metadataPort
      break
    }
  }
  assert.ok(port > 0, "Server port should be bound and reported")

  // Send wake request with token and json payload
  const response = await fetch(`http://127.0.0.1:${port}/wake`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ token: "sess-port-test-1", exitCode: 0, status: "completed" }),
  })
  assert.equal(response.status, 200)
  const resJson = await response.json()
  assert.equal(resJson.status, "ok")

  const result = await waitPromise
  assert.match(result, /ended: port_signal/)
  assert.match(result, new RegExp(`port: ${port}`))
  assert.match(result, /payload: {"exitCode":0,"status":"completed"}/)
  assert.match(result, /purpose: Waiting for simulation/)
})

test("wait tool rejects HTTP wake-up with invalid token and continues waiting", async () => {
  let timedOut = false
  const wait = createWaitTool({
    sleep: async () => {
      // Short sleep to allow the unauthorized attempt to be checked before timeout completes
      await new Promise((r) => setTimeout(r, 200))
      timedOut = true
    },
    now: () => 1_700_000_000_000,
  })

  const context = createContext({ sessionID: "secure-session" })
  let metadataPort = 0
  context.metadata = (input) => {
    if (input.metadata?.port) metadataPort = input.metadata.port
  }

  const waitPromise = wait.execute({ seconds: 1, reason: "Security test" }, context)

  for (let i = 0; i < 50; i++) {
    await new Promise((r) => setTimeout(r, 10))
    if (metadataPort > 0) break
  }
  assert.ok(metadataPort > 0, "Server should be listening")

  // Try wake with WRONG token
  const badResponse = await fetch(`http://127.0.0.1:${metadataPort}/wake?token=wrong-token`, {
    method: "GET",
  })
  assert.equal(badResponse.status, 401)
  const badJson = await badResponse.json()
  assert.equal(badJson.status, "error")

  // The wait tool should NOT have woken up from the bad request; it completes after the sleep
  const result = await waitPromise
  assert.ok(timedOut, "Sleep should have completed")
  assert.doesNotMatch(result, /ended: port_signal/)
})

test("wait tool respects context.abort and terminates immediately", async () => {
  const abortController = new AbortController()
  const metadataCalls = []
  const wait = createWaitTool({
    sleep: () => new Promise(() => {}),
  })

  const context = createContext({
    abort: abortController.signal,
    metadata(data) {
      metadataCalls.push(data)
    },
  })

  const waitPromise = wait.execute({ seconds: 60, reason: "Cancelled task" }, context)

  // Abort after a few milliseconds
  setTimeout(() => abortController.abort(), 20)

  await assert.rejects(waitPromise, (err) => {
    return err.name === "AbortError"
  })

  const lastMeta = metadataCalls[metadataCalls.length - 1]
  assert.equal(lastMeta?.metadata?.status, "interrupted")
})

test("wait tool wakes up when sentinel file appears and deletes it", async () => {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-wait-test-"))
  const sentinelFile = path.join(tmpDir, "job.done")

  const wait = createWaitTool({
    sleep: () => new Promise(() => {}),
    filePollIntervalMs: 50,
    now: () => 1_700_000_000_000,
  })

  const context = createContext({ directory: tmpDir })
  const waitPromise = wait.execute(
    {
      seconds: 3600,
      file: "job.done",
      readContent: true,
      deleteFile: true,
      pollIntervalSeconds: 0.05,
      reason: "Waiting for batch output",
    },
    context
  )

  // Wait 30ms then create sentinel file
  await new Promise((r) => setTimeout(r, 30))
  await fs.writeFile(sentinelFile, "Slurm job completed: 0\n")

  const result = await waitPromise

  assert.match(result, /ended: file_ready/)
  assert.match(result, /content: Slurm job completed: 0/)
  assert.match(result, /purpose: Waiting for batch output/)

  // Verify file was cleaned up
  await assert.rejects(fs.stat(sentinelFile), { code: "ENOENT" })

  await fs.rm(tmpDir, { recursive: true, force: true }).catch(() => {})
})

test("plugin chat.message hook wakes up waiting session in-process without polling", async () => {
  const plugin = createWaitPlugin({
    sleep: () => new Promise(() => {}),
    now: () => 1_700_000_000_000,
  })

  const hooks = await plugin({
    client: {
      session: {
        messages: async () => {
          throw new Error("HTTP messages polling should not be called when in-process hook is used")
        },
      },
    },
    directory: "/tmp/project",
    serverUrl: "http://localhost",
  })

  const sessionID = "sess-inprocess-test"
  const context = createContext({ sessionID, messageID: "m1" })

  const waitPromise = hooks.tool.wait.execute({ until: "new_user_message" }, context)

  // Trigger chat.message hook on plugin
  await new Promise((r) => setTimeout(r, 20))
  await hooks["chat.message"]?.(
    { sessionID, messageID: "m2" },
    { message: { id: "m2", role: "user" }, parts: [{ type: "text", text: "Resume now" }] }
  )

  const result = await waitPromise
  assert.match(result, /ended: event/)
  assert.match(result, /message: m2/)
  assert.match(result, /reason: new user message/)
})
