# opencode-wait

[![License: MPL-2.0](https://img.shields.io/badge/License-MPL--2.0-brightgreen.svg)](LICENSE)

`opencode-wait` is a lightweight, zero-CPU **OpenCode** plugin for unattended and background waiting workflows. It gives agents the ability to wait for timers, Slurm/HPC batch jobs, external completion signals, or sentinel files without burning CPU or getting killed by login-node watchdogs.

---

## Key Capabilities

1. **HTTP Port Wake-Up (`port = 40000 + hash(sessionID) % 4096`)**:
   - Automatically opens a lightweight, zero-CPU HTTP listener bound to an easily predictable port.
   - External programs (such as compute nodes running a Slurm batch job) can immediately wake up the waiting agent by sending a simple `curl` request.
   - Protected with token authentication matching the `sessionID`.
   - Supports passing payloads (e.g. exit codes, results summary) directly back to the model.

2. **Completion Sentinel Files (`file: "job.done"`)**:
   - Watches for the appearance of a sentinel file on shared cluster storage (NFS/Lustre).
   - Reads the file's content (e.g. simulation status, error logs) and optionally cleans up the file automatically.
   - Polls at a gentle, configurable cadence (default: 5s) with true kernel sleep between checks.

3. **Login-Node Safe (0.00% CPU Consumption)**:
   - Uses native Linux kernel `epoll_wait` for socket listening and true `setTimeout` sleep.
   - Emits in-process event hooks instead of aggressive 1 Hz HTTP polling over loopback.
   - Prevents HPC login-node watchdogs (`cgroups`, runaway process monitors) from terminating your session during long waits.

4. **UI Cancellation Support (`context.abort`)**:
   - Listens to OpenCode's abort controller so pressing Stop or cancelling immediately terminates the wait and frees all sockets.

5. **Live Progress Reporting**:
   - Displays live progress bar, elapsed time, remaining time, and bound port in OpenCode's metadata bar.
   - Adaptive update interval prevents timer churn on long waits.

---

## Slurm / HPC Integration Example

### 1. In OpenCode
The agent submits a Slurm job and calls `wait`:

```json
{
  "seconds": 3600,
  "reason": "Waiting for Slurm job 5251529",
  "port": "auto"
}
```

The tool starts listening on `http://${HOSTNAME}:${PORT}/wake` and displays the port in progress metadata (or writes to `.opencode/wait-${sessionID}.json`).

### 2. In your Slurm batch script (`job.sh`)

```bash
#!/bin/bash
#SBATCH -J my_simulation
#SBATCH -t 01:00:00

srun ./my_simulation
EXIT_CODE=$?

# Wake up the waiting OpenCode session on the login node:
curl -s -X POST "http://${OPENCODE_HOST}:${OPENCODE_PORT}/wake" \
     -H "Content-Type: application/json" \
     -d "{\"token\":\"${OPENCODE_SESSION}\",\"exitCode\":${EXIT_CODE}}"
```

### 3. Agent Outcome
The wait ends instantly when the curl request arrives, returning:

```text
started: 2026-10-27T22:00:00.000Z; waited: 142s; now: 2026-10-27T22:02:22.000Z; ended: port_signal; port: 41234; payload: {"exitCode":0}; purpose: Waiting for Slurm job 5251529
```

The model immediately receives the exit code and proceeds with analysis.

---

## Tool Arguments

| Argument | Type | Description |
| :--- | :--- | :--- |
| `seconds` | `number` | Optional maximum wait time in seconds (timeout). Positive values are honored; missing or non-positive values complete immediately. |
| `reason` | `string` | Optional description of what the agent is waiting for (displayed in progress bar and logs). |
| `port` | `number \| "auto" \| "off"` | HTTP wake-up port. Default is `"auto"` (`40000 + hash(sessionID) % 4096`). Set to `"off"` to disable. |
| `file` | `string` | Optional path to a sentinel file (e.g. `job.done`). Wait completes when the file appears. |
| `readContent` | `boolean` | Whether to read and return the sentinel file content upon wake-up (default: `true`). |
| `deleteFile` | `boolean` | Whether to remove the sentinel file after wake-up (default: `true`). |
| `pollIntervalSeconds` | `number` | Check interval in seconds for the sentinel file (default: `5s`, keeping CPU at 0%). |
| `until` | `"new_user_message"` | Wait for a new user message in the session. |

---

## Usage in Code

Default export is the OpenCode plugin function:

```typescript
import WaitPlugin from "opencode-wait"

export default WaitPlugin
```

For tests or custom runners, customize seams using `createWaitPlugin`:

```typescript
import { createWaitPlugin } from "opencode-wait"

export default createWaitPlugin({
  sleep: async () => {},
  enablePort: true,
})
```

---

## Local Development

```bash
bun install
bun test
bun run build
```

---

## License

MPL-2.0 License. See [LICENSE](LICENSE) for details.
