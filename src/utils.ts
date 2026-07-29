/**
 * CodeGraph Utilities
 *
 * Common utility functions for memory management, concurrency, batching,
 * and security validation.
 *
 * @module utils
 *
 * @example
 * ```typescript
 * import { Mutex, processInBatches, MemoryMonitor, validatePathWithinRoot } from 'codegraph';
 *
 * // Use mutex for concurrent safety
 * const mutex = new Mutex();
 * await mutex.withLock(async () => {
 *   await performCriticalOperation();
 * });
 *
 * // Process items in batches to manage memory
 * const results = await processInBatches(items, 100, async (item) => {
 *   return await processItem(item);
 * });
 *
 * // Monitor memory usage
 * const monitor = new MemoryMonitor(512, (usage) => {
 *   console.warn(`Memory usage exceeded 512MB: ${usage / 1024 / 1024}MB`);
 * });
 * monitor.start();
 * ```
 */

import * as fs from 'fs';
import * as crypto from 'crypto';
import * as os from 'os';
import * as path from 'path';
import { spawnSync } from 'child_process';
import { Worker } from 'worker_threads';

// ============================================================
// SECURITY UTILITIES
// ============================================================

/**
 * Sensitive system directories that should never be used as project roots.
 * Checked on all platforms; non-applicable paths are harmlessly skipped.
 */
const SENSITIVE_PATHS = new Set([
  '/', '/etc', '/usr', '/bin', '/sbin', '/var', '/tmp', '/dev', '/proc', '/sys',
  '/root', '/boot', '/lib', '/lib64', '/opt',
  'c:\\', 'c:\\windows', 'c:\\windows\\system32',
]);

/**
 * Config "languages" whose nodes are pure key/value DATA lifted from a config
 * file (e.g. Spring `application.{yml,properties}`), not source code.
 */
export const CONFIG_LEAF_LANGUAGES: ReadonlySet<string> = new Set(['yaml', 'properties']);

/**
 * A config-leaf node is a single key lifted out of a pure config/data file —
 * `kind: 'constant'` in a {@link CONFIG_LEAF_LANGUAGES} language. Its on-disk
 * line is `key = <value>`, and that value is routinely a secret (DB password,
 * API key, JDBC URL with embedded creds). CodeGraph must surface the KEY only
 * and never read/return the value, or it pushes secrets into agent context
 * unbidden — the value isn't needed for resolution, and an agent that genuinely
 * needs it can read the file directly. (#383)
 */
export function isConfigLeafNode(node: { kind: string; language?: string }): boolean {
  return node.kind === 'constant' && !!node.language && CONFIG_LEAF_LANGUAGES.has(node.language);
}

/**
 * Whether `child` is `parent` itself or sits underneath it. Case-insensitive on
 * Windows — NTFS is case-insensitive, and realpathSync can hand back a different
 * case than the lexical root, which would otherwise false-reject a valid file.
 */
function isWithinDir(child: string, parent: string): boolean {
  let c = child;
  let p = parent;
  if (process.platform === 'win32') {
    c = c.toLowerCase();
    p = p.toLowerCase();
  }
  return c === p || c.startsWith(p + path.sep);
}

/**
 * Validate that a file path stays within the project root, resolving symlinks.
 *
 * Two layers: a cheap lexical check that catches `../` traversal, then a
 * realpath check that catches symlink escapes — an in-repo symlink whose
 * logical path is inside the root but whose real target points outside it
 * (issue #527). A symlink that stays within the root is still allowed, so
 * legitimate in-tree symlinks keep working. Both content-serving read sinks
 * (codegraph_node `includeCode`, codegraph_explore source) go through here, so
 * this is the chokepoint that keeps out-of-root file contents from leaking.
 *
 * `allowSymlinkEscape` waives **only** the realpath-escape rejection (the
 * lexical `../` guard still applies) for the INDEXING read path. The directory
 * walk deliberately descends into in-root symlinks whose targets live outside
 * the root (e.g. a `game/` symlink in a Dota custom-game tree, #935); discovery
 * and the reader must agree, or every file the walk enumerated fails to index.
 * Indexing only reads paths it just discovered, into a local index — it never
 * serves them to an agent, so this does not widen the #527 leak surface. The
 * content-serving sinks must never pass this flag.
 *
 * @param projectRoot - The project root directory
 * @param filePath - The (relative or absolute) file path to validate
 * @param options.allowSymlinkEscape - Follow in-root symlinks out of the root
 *   (indexing read path only); defaults to the strict, leak-safe behavior.
 * @returns The resolved absolute path (realpath when it exists), or null if it
 *   escapes the root
 */
export function validatePathWithinRoot(
  projectRoot: string,
  filePath: string,
  options?: { allowSymlinkEscape?: boolean }
): string | null {
  const resolved = path.resolve(projectRoot, filePath);
  const normalizedRoot = path.resolve(projectRoot);

  // 1. Lexical containment — cheap, catches `../` traversal. Applies even on
  //    the indexing read path: a crafted `../` escape is still rejected.
  if (!isWithinDir(resolved, normalizedRoot)) {
    return null;
  }

  // 2. Symlink-aware containment — resolve symlinks on both sides and re-check,
  //    so an in-repo symlink whose real target escapes the root is rejected.
  //    The indexing read path (allowSymlinkEscape) skips only this rejection so
  //    it stays consistent with the directory walk, which already followed the
  //    in-root symlink to enumerate these files (#935).
  try {
    const realRoot = fs.realpathSync(normalizedRoot);
    const realResolved = fs.realpathSync(resolved);
    if (options?.allowSymlinkEscape) {
      return realResolved;
    }
    return isWithinDir(realResolved, realRoot) ? realResolved : null;
  } catch (err) {
    // ENOENT: the path doesn't exist yet (a file about to be written, or an
    // index entry for a since-deleted file) — no symlink to follow, and the
    // lexical check already passed, so allow the lexical path. Any other
    // resolution failure (ELOOP, EACCES, …) is treated as unsafe → reject.
    if ((err as NodeJS.ErrnoException).code === 'ENOENT') {
      return resolved;
    }
    return null;
  }
}

/**
 * Validate that a path is a safe project root directory.
 *
 * Rejects sensitive system directories and ensures the path is
 * a real, existing directory. Used at MCP and API entry points
 * to prevent arbitrary directory access.
 *
 * @param dirPath - The path to validate
 * @returns An error message if invalid, or null if valid
 */
export function validateProjectPath(dirPath: string): string | null {
  const resolved = path.resolve(dirPath);

  // Block sensitive system directories
  if (SENSITIVE_PATHS.has(resolved) || SENSITIVE_PATHS.has(resolved.toLowerCase())) {
    return `Refusing to operate on sensitive system directory: ${resolved}`;
  }

  // Also block common sensitive home subdirectories
  const homeDir = require('os').homedir();
  const sensitiveHomeDirs = ['.ssh', '.gnupg', '.aws', '.config'];
  for (const dir of sensitiveHomeDirs) {
    const sensitivePath = path.join(homeDir, dir);
    if (resolved === sensitivePath || resolved.startsWith(sensitivePath + path.sep)) {
      return `Refusing to operate on sensitive directory: ${resolved}`;
    }
  }

  // Verify it's a real directory
  try {
    const stats = fs.statSync(resolved);
    if (!stats.isDirectory()) {
      return `Path is not a directory: ${resolved}`;
    }
  } catch {
    return `Path does not exist or is not accessible: ${resolved}`;
  }

  return null;
}

/**
 * Safely parse JSON with a fallback value.
 * Prevents crashes from corrupted database metadata.
 */
export function safeJsonParse<T>(value: string, fallback: T): T {
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

/**
 * Clamp a numeric value to a range.
 * Used to enforce sane limits on MCP tool inputs.
 */
export function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

/**
 * Normalize a file path to use forward slashes.
 * Fixes Windows backslash paths so glob matching works consistently.
 */
export function normalizePath(filePath: string): string {
  return filePath.replace(/\\/g, '/');
}

interface FileLockLease {
  nonce: string;
  pid: number;
  /** OS process-birth identity used to distinguish a live owner from PID reuse. */
  processStartToken: string | null;
  hostId: string;
  acquiredAt: number;
  heartbeatAt: number;
}

interface FileLockSnapshot {
  raw: string;
  stat: fs.Stats;
}

export interface FileLockOptions {
  /** How often the independent lease worker refreshes the heartbeat. */
  heartbeatIntervalMs?: number;
  /** How long a remote or unreadable lease may go without a heartbeat. */
  staleTimeoutMs?: number;
}

/**
 * Identify one OS/PID namespace, not merely one physical machine.
 *
 * Native Windows and WSL commonly report the same hostname while using
 * unrelated PID namespaces. Including the platform profile forces those
 * contenders to use the renewable-heartbeat (remote lease) rules.
 */
export function fileLockHostId(): string {
  const hostname = os.hostname() || 'unknown-host';
  const isWsl = process.platform === 'linux'
    && (
      Boolean(process.env.WSL_DISTRO_NAME)
      || /microsoft|wsl/iu.test(os.release())
    );
  const platform = isWsl
    ? `wsl:${process.env.WSL_DISTRO_NAME ?? 'unknown'}`
    : process.platform;
  return `${hostname}:${platform}:${process.arch}`;
}

/**
 * Cross-process file lock backed by a renewable lease.
 *
 * Acquisition uses O_CREAT|O_EXCL, so only one contender can create the lease.
 * A dedicated worker thread renews the heartbeat even while indexing keeps the
 * main event loop busy. Local leases are additionally protected by a live-PID
 * check; remote leases use the heartbeat because their PID namespace is not
 * meaningful on this host.
 */
export class FileLock {
  private static readonly DEFAULT_HEARTBEAT_INTERVAL_MS = 15_000;
  private static readonly DEFAULT_STALE_TIMEOUT_MS = 90_000;
  private static readonly MAX_ACQUIRE_ATTEMPTS = 8;
  private static sharedHeartbeatWorker: Worker | null = null;
  private static currentProcessStartToken: string | null | undefined;

  private readonly lockPath: string;
  private readonly heartbeatIntervalMs: number;
  private readonly staleTimeoutMs: number;
  private readonly hostId = fileLockHostId();
  private held = false;
  private nonce: string | null = null;
  private heartbeatId: string | null = null;
  private heartbeatControl: Int32Array | null = null;

  constructor(lockPath: string, options: FileLockOptions = {}) {
    this.lockPath = lockPath;
    this.heartbeatIntervalMs = FileLock.positiveDuration(
      options.heartbeatIntervalMs,
      FileLock.DEFAULT_HEARTBEAT_INTERVAL_MS,
      'heartbeatIntervalMs'
    );
    this.staleTimeoutMs = FileLock.positiveDuration(
      options.staleTimeoutMs,
      FileLock.DEFAULT_STALE_TIMEOUT_MS,
      'staleTimeoutMs'
    );
  }

  /**
   * Acquire the lock. Throws if the lock is held by another live process.
   */
  acquire(): void {
    const now = Date.now();
    const lease: FileLockLease = {
      nonce: crypto.randomUUID(),
      pid: process.pid,
      processStartToken: FileLock.getProcessStartToken(process.pid),
      hostId: this.hostId,
      acquiredAt: now,
      heartbeatAt: now,
    };

    for (let attempt = 0; attempt < FileLock.MAX_ACQUIRE_ATTEMPTS; attempt++) {
      try {
        fs.writeFileSync(this.lockPath, FileLock.serializeLease(lease), {
          encoding: 'utf8',
          flag: 'wx',
          mode: 0o600,
        });
        this.held = true;
        this.nonce = lease.nonce;
        this.startHeartbeat();
        return;
      } catch (err: unknown) {
        if ((err as NodeJS.ErrnoException).code !== 'EEXIST') throw err;
      }

      const snapshot = this.readSnapshot(this.lockPath);
      // The owner may have released between our exclusive-create failure and
      // the read. Retry creation instead of interpreting ENOENT as corruption.
      if (!snapshot) continue;

      if (!this.isStale(snapshot)) {
        throw this.lockedError(snapshot);
      }

      // Reclamation verifies that the path still points at the exact lease we
      // classified as stale. If it changed, retry and inspect the new owner.
      if (!this.reclaimIfUnchanged(snapshot)) continue;
    }

    throw this.lockedError(this.readSnapshot(this.lockPath));
  }

  /**
   * Release the lock. Only the exact nonce created by this instance may remove
   * the lease; matching a recycled PID is deliberately insufficient.
   */
  release(): void {
    if (!this.held) return;

    this.stopHeartbeat();
    try {
      const snapshot = this.readSnapshot(this.lockPath);
      const lease = snapshot ? FileLock.parseLease(snapshot.raw) : null;
      if (lease?.nonce === this.nonce) {
        fs.unlinkSync(this.lockPath);
      }
    } catch {
      // Lock file already gone or replaced — either way, it is no longer ours.
    } finally {
      this.held = false;
      this.nonce = null;
    }
  }

  /**
   * Execute a function while holding the lock
   */
  withLock<T>(fn: () => T): T {
    this.acquire();
    try {
      return fn();
    } finally {
      this.release();
    }
  }

  /**
   * Execute an async function while holding the lock
   */
  async withLockAsync<T>(fn: () => Promise<T>): Promise<T> {
    this.acquire();
    try {
      return await fn();
    } finally {
      this.release();
    }
  }

  private static positiveDuration(
    value: number | undefined,
    fallback: number,
    name: string
  ): number {
    if (value === undefined) return fallback;
    if (!Number.isFinite(value) || value <= 0) {
      throw new RangeError(`${name} must be a positive finite number`);
    }
    return Math.floor(value);
  }

  private static serializeLease(lease: FileLockLease): string {
    return JSON.stringify(lease) + '\n';
  }

  private static parseLease(raw: string): FileLockLease | null {
    try {
      const value = JSON.parse(raw.trim()) as Partial<FileLockLease>;
      if (
        typeof value.nonce === 'string' &&
        value.nonce.length > 0 &&
        Number.isInteger(value.pid) &&
        (value.pid ?? 0) > 0 &&
        (
          value.processStartToken === undefined ||
          value.processStartToken === null ||
          (
            typeof value.processStartToken === 'string' &&
            value.processStartToken.length > 0
          )
        ) &&
        typeof value.hostId === 'string' &&
        value.hostId.length > 0 &&
        typeof value.acquiredAt === 'number' &&
        Number.isFinite(value.acquiredAt) &&
        typeof value.heartbeatAt === 'number' &&
        Number.isFinite(value.heartbeatAt)
      ) {
        return {
          ...value,
          // Structured leases written before the renewable-lease upgrade did
          // not carry a process-birth token. Keep them readable and fall back
          // to the conservative live-PID rule.
          processStartToken: value.processStartToken ?? null,
        } as FileLockLease;
      }
    } catch {
      // Legacy PID-only and malformed records are handled conservatively below.
    }
    return null;
  }

  private readSnapshot(filePath: string): FileLockSnapshot | null {
    let fd: number | null = null;
    try {
      fd = fs.openSync(filePath, 'r');
      return {
        raw: fs.readFileSync(fd, 'utf8'),
        stat: fs.fstatSync(fd),
      };
    } catch (err: unknown) {
      if ((err as NodeJS.ErrnoException).code === 'ENOENT') return null;
      throw err;
    } finally {
      if (fd !== null) fs.closeSync(fd);
    }
  }

  private isStale(snapshot: FileLockSnapshot): boolean {
    const lease = FileLock.parseLease(snapshot.raw);
    if (lease) {
      if (this.sameHost(lease.hostId)) {
        if (!this.isProcessAlive(lease.pid)) return true;
        // A live PID can belong to a different process after PID reuse. When
        // both sides have an OS process-birth token, a mismatch is definitive
        // evidence that the original owner died. If the platform cannot expose
        // one, remain conservative and keep the live PID authoritative.
        if (lease.processStartToken !== null) {
          const currentToken = FileLock.getProcessStartToken(lease.pid);
          if (
            !lease.processStartToken.startsWith('fallback:') &&
            currentToken !== null &&
            !currentToken.startsWith('fallback:') &&
            currentToken !== lease.processStartToken
          ) {
            return true;
          }
        }
        // A live local process with the same birth token remains authoritative
        // even if the main thread was suspended beyond the heartbeat timeout.
        return false;
      }
      const lastRenewal = Math.max(lease.heartbeatAt, snapshot.stat.mtimeMs);
      return Date.now() - lastRenewal > this.staleTimeoutMs;
    }

    const legacyPid = FileLock.parseLegacyPid(snapshot.raw);
    if (legacyPid !== null) return !this.isProcessAlive(legacyPid);

    // A recent partial/malformed record may be the tiny create/write window of
    // a legitimate owner. Only reclaim it after a full stale interval.
    return Date.now() - snapshot.stat.mtimeMs > this.staleTimeoutMs;
  }

  private static parseLegacyPid(raw: string): number | null {
    const trimmed = raw.trim();
    if (!/^\d+$/.test(trimmed)) return null;
    const pid = Number(trimmed);
    return Number.isSafeInteger(pid) && pid > 0 ? pid : null;
  }

  private sameHost(hostId: string): boolean {
    return hostId.toLocaleLowerCase() === this.hostId.toLocaleLowerCase();
  }

  private lockedError(snapshot: FileLockSnapshot | null): Error {
    const lease = snapshot ? FileLock.parseLease(snapshot.raw) : null;
    const legacyPid = snapshot ? FileLock.parseLegacyPid(snapshot.raw) : null;
    const pid = lease?.pid ?? legacyPid;
    const owner = pid === null || pid === undefined ? '' : ` (PID ${pid})`;
    return new Error(
      `CodeGraph database is locked by another process${owner}. ` +
      `If this is stale, run 'codegraph unlock' or delete ${this.lockPath}`
    );
  }

  /**
   * Remove the observed stale inode without accidentally unlinking a lease that
   * won the path in the meantime. Hard links provide an inode-stable comparison
   * on normal local filesystems; a guarded compare-and-unlink is the fallback
   * for ExFAT/network filesystems that do not support hard links.
   */
  private reclaimIfUnchanged(observed: FileLockSnapshot): boolean {
    const claimPath =
      `${this.lockPath}.stale.${process.pid}.${crypto.randomUUID()}`;
    try {
      fs.linkSync(this.lockPath, claimPath);
    } catch (err: unknown) {
      const code = (err as NodeJS.ErrnoException).code;
      if (code === 'ENOENT') return false;
      if (['EPERM', 'EACCES', 'ENOTSUP', 'EXDEV', 'EINVAL'].includes(code ?? '')) {
        return this.reclaimWithGuard(observed);
      }
      throw err;
    }

    try {
      const claimed = this.readSnapshot(claimPath);
      const current = this.readSnapshot(this.lockPath);
      if (
        !claimed ||
        !current ||
        !FileLock.sameSnapshot(observed, claimed) ||
        !FileLock.sameSnapshot(claimed, current) ||
        !this.isStale(current)
      ) {
        return false;
      }
      fs.unlinkSync(this.lockPath);
      return true;
    } catch (err: unknown) {
      if ((err as NodeJS.ErrnoException).code === 'ENOENT') return false;
      throw err;
    } finally {
      try { fs.unlinkSync(claimPath); } catch { /* best-effort tombstone cleanup */ }
    }
  }

  private reclaimWithGuard(observed: FileLockSnapshot): boolean {
    const guardPath = `${this.lockPath}.reclaim`;
    const guardNonce = crypto.randomUUID();
    try {
      fs.writeFileSync(guardPath, guardNonce, {
        encoding: 'utf8',
        flag: 'wx',
        mode: 0o600,
      });
    } catch (err: unknown) {
      if ((err as NodeJS.ErrnoException).code === 'EEXIST') {
        // A reclaimer can itself crash. Its short-lived guard is safe to clear
        // after the same conservative timeout used for malformed leases.
        try {
          const stat = fs.statSync(guardPath);
          if (Date.now() - stat.mtimeMs > this.staleTimeoutMs) {
            fs.unlinkSync(guardPath);
          }
        } catch { /* another contender changed the guard */ }
        return false;
      }
      throw err;
    }

    try {
      const current = this.readSnapshot(this.lockPath);
      if (!current || !FileLock.sameSnapshot(observed, current) || !this.isStale(current)) {
        return false;
      }
      fs.unlinkSync(this.lockPath);
      return true;
    } catch (err: unknown) {
      if ((err as NodeJS.ErrnoException).code === 'ENOENT') return false;
      throw err;
    } finally {
      try {
        if (fs.readFileSync(guardPath, 'utf8') === guardNonce) {
          fs.unlinkSync(guardPath);
        }
      } catch { /* another recovery already cleaned it */ }
    }
  }

  private static sameSnapshot(a: FileLockSnapshot, b: FileLockSnapshot): boolean {
    return (
      a.raw === b.raw &&
      a.stat.dev === b.stat.dev &&
      a.stat.ino === b.stat.ino
    );
  }

  /**
   * The heartbeat runs outside the main event loop so synchronous extraction
   * cannot make a healthy lease appear abandoned to another host.
   */
  private startHeartbeat(): void {
    if (!this.nonce) return;

    // control[0] = heartbeat write in progress; control[1] = stop requested.
    const control = new Int32Array(new SharedArrayBuffer(2 * Int32Array.BYTES_PER_ELEMENT));
    const heartbeatId = crypto.randomUUID();
    const worker = FileLock.getHeartbeatWorker();
    try {
      worker.postMessage({
        type: 'add',
        id: heartbeatId,
        lockPath: this.lockPath,
        nonce: this.nonce,
        intervalMs: this.heartbeatIntervalMs,
        control: control.buffer,
      });
      this.heartbeatId = heartbeatId;
      this.heartbeatControl = control;
    } catch {
      // The live local-PID check still protects this lease. A later lock
      // acquisition recreates the shared worker if this one exited.
    }
  }

  /**
   * One process-wide scheduler renews every FileLock lease. Creating a worker
   * for each short sync floods Windows with threads waiting to exit and can
   * temporarily pin test/project directories; a shared worker has constant
   * resource cost while preserving independent intervals and stop controls.
   */
  private static getHeartbeatWorker(): Worker {
    if (FileLock.sharedHeartbeatWorker) return FileLock.sharedHeartbeatWorker;

    const worker = new Worker(
      `
        const fs = require('node:fs');
        const { parentPort } = require('node:worker_threads');
        const jobs = new Map();

        function remove(id) {
          const job = jobs.get(id);
          if (!job) return;
          clearInterval(job.timer);
          jobs.delete(id);
        }

        function heartbeat(id) {
          const job = jobs.get(id);
          if (!job) return;
          const control = job.control;
          if (Atomics.load(control, 1) !== 0) {
            remove(id);
            return;
          }
          if (Atomics.compareExchange(control, 0, 0, 1) !== 0) return;

          let fd = null;
          let shouldStop = false;
          try {
            if (Atomics.load(control, 1) !== 0) return;
            fd = fs.openSync(job.lockPath, 'r+');
            const raw = fs.readFileSync(fd, 'utf8');
            const lease = JSON.parse(raw.trim());
            if (!lease || lease.nonce !== job.nonce) {
              shouldStop = true;
              return;
            }

            const marker = '"heartbeatAt":';
            const markerIndex = raw.indexOf(marker);
            const valueStart = markerIndex + marker.length;
            const oldValue = markerIndex >= 0
              ? (raw.slice(valueStart).match(/^\\d+/) || [])[0]
              : undefined;
            const nextValue = String(Date.now());
            if (!oldValue || oldValue.length !== nextValue.length) {
              shouldStop = true;
              return;
            }

            const byteOffset = Buffer.byteLength(raw.slice(0, valueStart), 'utf8');
            fs.writeSync(fd, nextValue, byteOffset, 'utf8');
            fs.fsyncSync(fd);
          } catch (err) {
            if (err && (err.code === 'ENOENT' || err.code === 'EACCES')) {
              shouldStop = true;
            }
          } finally {
            if (fd !== null) {
              try { fs.closeSync(fd); } catch {}
            }
            Atomics.store(control, 0, 0);
            Atomics.notify(control, 0);
            if (shouldStop) remove(id);
          }
        }

        if (parentPort) {
          parentPort.on('message', (message) => {
            if (message.type === 'add') {
              const control = new Int32Array(message.control);
              if (Atomics.load(control, 1) !== 0) return;
              const job = {
                lockPath: message.lockPath,
                nonce: message.nonce,
                control,
                timer: null,
              };
              job.timer = setInterval(
                () => heartbeat(message.id),
                message.intervalMs
              );
              jobs.set(message.id, job);
            } else if (message.type === 'remove') {
              remove(message.id);
            }
          });
        }
      `,
      { eval: true }
    );
    worker.unref();
    worker.on('error', () => {
      if (FileLock.sharedHeartbeatWorker === worker) {
        FileLock.sharedHeartbeatWorker = null;
      }
    });
    worker.on('exit', () => {
      if (FileLock.sharedHeartbeatWorker === worker) {
        FileLock.sharedHeartbeatWorker = null;
      }
    });
    FileLock.sharedHeartbeatWorker = worker;
    return worker;
  }

  private stopHeartbeat(): void {
    const control = this.heartbeatControl;
    const heartbeatId = this.heartbeatId;
    this.heartbeatControl = null;
    this.heartbeatId = null;

    if (control) {
      Atomics.store(control, 1, 1);
      // Wait for an already-running write so release never observes a partial
      // heartbeat record and leaves behind a lease it still owns.
      const deadline = Date.now() + 2_000;
      while (Atomics.load(control, 0) !== 0 && Date.now() < deadline) {
        Atomics.wait(control, 0, 1, 100);
      }
    }
    if (heartbeatId && FileLock.sharedHeartbeatWorker) {
      try {
        FileLock.sharedHeartbeatWorker.postMessage({
          type: 'remove',
          id: heartbeatId,
        });
      } catch { /* worker already exited */ }
    }
  }

  /**
   * Check if a process is still running. EPERM means the process exists but
   * belongs to another user, so it must still be treated as alive.
   */
  private isProcessAlive(pid: number): boolean {
    try {
      process.kill(pid, 0);
      return true;
    } catch (err: unknown) {
      return (err as NodeJS.ErrnoException).code === 'EPERM';
    }
  }

  /**
   * Return an OS process-birth identity for PID-reuse detection.
   *
   * Linux exposes the kernel start tick in `/proc/<pid>/stat`; Windows exposes
   * creation ticks through Get-Process; POSIX systems without procfs use `ps`.
   * Failure is intentionally represented as null so callers keep a live PID
   * rather than risk stealing a valid writer lease.
   */
  private static getProcessStartToken(pid: number): string | null {
    if (pid === process.pid && FileLock.currentProcessStartToken !== undefined) {
      return FileLock.currentProcessStartToken;
    }

    let token: string | null = null;
    try {
      if (process.platform === 'linux') {
        const raw = fs.readFileSync(`/proc/${pid}/stat`, 'utf8');
        const closeParen = raw.lastIndexOf(') ');
        if (closeParen >= 0) {
          // The tail starts at field 3 (`state`); starttime is field 22.
          const fields = raw.slice(closeParen + 2).trim().split(/\s+/);
          const startTicks = fields[19];
          if (startTicks && /^\d+$/.test(startTicks)) {
            token = `linux:${startTicks}`;
          }
        }
      } else if (process.platform === 'win32') {
        const command =
          `(Get-Process -Id ${pid} -ErrorAction Stop).StartTime` +
          '.ToUniversalTime().Ticks';
        const result = spawnSync(
          'powershell.exe',
          ['-NoProfile', '-NonInteractive', '-Command', command],
          {
            encoding: 'utf8',
            timeout: 3_000,
            windowsHide: true,
          }
        );
        const ticks = result.status === 0 ? result.stdout.trim() : '';
        if (/^\d+$/.test(ticks)) token = `windows:${ticks}`;
      } else {
        const result = spawnSync(
          'ps',
          ['-o', 'lstart=', '-p', String(pid)],
          { encoding: 'utf8', timeout: 3_000 }
        );
        const started = result.status === 0
          ? result.stdout.trim().replace(/\s+/g, ' ')
          : '';
        if (started) token = `${process.platform}:${started}`;
      }
    } catch {
      token = null;
    }

    if (pid === process.pid) {
      token ??=
        `fallback:${process.pid}:` +
        `${Math.floor(Date.now() - process.uptime() * 1_000)}`;
      FileLock.currentProcessStartToken = token;
    }
    return token;
  }
}

/**
 * Process items in batches to manage memory
 *
 * @param items - Array of items to process
 * @param batchSize - Number of items per batch
 * @param processor - Function to process each item
 * @param onBatchComplete - Optional callback after each batch
 * @returns Array of results
 */
export async function processInBatches<T, R>(
  items: T[],
  batchSize: number,
  processor: (item: T, index: number) => Promise<R>,
  onBatchComplete?: (completed: number, total: number) => void
): Promise<R[]> {
  const results: R[] = [];

  for (let i = 0; i < items.length; i += batchSize) {
    const batch = items.slice(i, Math.min(i + batchSize, items.length));
    const batchResults = await Promise.all(
      batch.map((item, idx) => processor(item, i + idx))
    );
    results.push(...batchResults);

    if (onBatchComplete) {
      onBatchComplete(Math.min(i + batchSize, items.length), items.length);
    }

    // Allow GC between batches
    if (global.gc) {
      global.gc();
    }
  }

  return results;
}

/**
 * Simple mutex lock for preventing concurrent operations
 */
export class Mutex {
  private locked = false;
  private waitQueue: Array<() => void> = [];

  /**
   * Acquire the lock
   *
   * @returns A release function to call when done
   */
  async acquire(): Promise<() => void> {
    while (this.locked) {
      await new Promise<void>((resolve) => {
        this.waitQueue.push(resolve);
      });
    }

    this.locked = true;

    return () => {
      this.locked = false;
      const next = this.waitQueue.shift();
      if (next) {
        next();
      }
    };
  }

  /**
   * Execute a function while holding the lock
   */
  async withLock<T>(fn: () => Promise<T> | T): Promise<T> {
    const release = await this.acquire();
    try {
      return await fn();
    } finally {
      release();
    }
  }

  /**
   * Check if the lock is currently held
   */
  isLocked(): boolean {
    return this.locked;
  }
}

/**
 * Chunked file reader for large files
 *
 * Reads a file in chunks to avoid loading entire file into memory.
 */
export async function* readFileInChunks(
  filePath: string,
  chunkSize: number = 64 * 1024
): AsyncGenerator<string, void, undefined> {
  const fs = await import('fs');

  const fd = fs.openSync(filePath, 'r');
  const buffer = Buffer.alloc(chunkSize);

  try {
    let bytesRead: number;
    while ((bytesRead = fs.readSync(fd, buffer, 0, chunkSize, null)) > 0) {
      yield buffer.toString('utf-8', 0, bytesRead);
    }
  } finally {
    fs.closeSync(fd);
  }
}

/**
 * Debounce a function
 *
 * @param fn - Function to debounce
 * @param delay - Delay in milliseconds
 * @returns Debounced function
 */
export function debounce<T extends (...args: unknown[]) => unknown>(
  fn: T,
  delay: number
): (...args: Parameters<T>) => void {
  let timeoutId: ReturnType<typeof setTimeout> | null = null;

  return (...args: Parameters<T>) => {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }
    timeoutId = setTimeout(() => {
      fn(...args);
      timeoutId = null;
    }, delay);
  };
}

/**
 * Throttle a function
 *
 * @param fn - Function to throttle
 * @param limit - Minimum time between calls in milliseconds
 * @returns Throttled function
 */
export function throttle<T extends (...args: unknown[]) => unknown>(
  fn: T,
  limit: number
): (...args: Parameters<T>) => void {
  let lastCall = 0;
  let timeoutId: ReturnType<typeof setTimeout> | null = null;

  return (...args: Parameters<T>) => {
    const now = Date.now();
    const remaining = limit - (now - lastCall);

    if (remaining <= 0) {
      if (timeoutId) {
        clearTimeout(timeoutId);
        timeoutId = null;
      }
      lastCall = now;
      fn(...args);
    } else if (!timeoutId) {
      timeoutId = setTimeout(() => {
        lastCall = Date.now();
        timeoutId = null;
        fn(...args);
      }, remaining);
    }
  };
}

/**
 * Estimate memory usage of an object (rough approximation)
 *
 * @param obj - Object to measure
 * @returns Approximate size in bytes
 */
export function estimateSize(obj: unknown): number {
  const seen = new WeakSet();

  function sizeOf(value: unknown): number {
    if (value === null || value === undefined) {
      return 0;
    }

    switch (typeof value) {
      case 'boolean':
        return 4;
      case 'number':
        return 8;
      case 'string':
        return 2 * (value as string).length;
      case 'object':
        if (seen.has(value as object)) {
          return 0;
        }
        seen.add(value as object);

        if (Array.isArray(value)) {
          return value.reduce((acc: number, item) => acc + sizeOf(item), 0);
        }

        return Object.entries(value as object).reduce(
          (acc, [key, val]) => acc + sizeOf(key) + sizeOf(val),
          0
        );
      default:
        return 0;
    }
  }

  return sizeOf(obj);
}

/**
 * Memory monitor for tracking usage during operations
 */
export class MemoryMonitor {
  private checkInterval: ReturnType<typeof setInterval> | null = null;
  private peakUsage = 0;
  private threshold: number;
  private onThresholdExceeded?: (usage: number) => void;

  constructor(
    thresholdMB: number = 500,
    onThresholdExceeded?: (usage: number) => void
  ) {
    this.threshold = thresholdMB * 1024 * 1024;
    this.onThresholdExceeded = onThresholdExceeded;
  }

  /**
   * Start monitoring memory usage
   */
  start(intervalMs: number = 1000): void {
    this.stop();
    this.peakUsage = 0;

    this.checkInterval = setInterval(() => {
      const usage = process.memoryUsage().heapUsed;
      if (usage > this.peakUsage) {
        this.peakUsage = usage;
      }
      if (usage > this.threshold && this.onThresholdExceeded) {
        this.onThresholdExceeded(usage);
      }
    }, intervalMs);
  }

  /**
   * Stop monitoring
   */
  stop(): void {
    if (this.checkInterval) {
      clearInterval(this.checkInterval);
      this.checkInterval = null;
    }
  }

  /**
   * Get peak memory usage in bytes
   */
  getPeakUsage(): number {
    return this.peakUsage;
  }

  /**
   * Get current memory usage in bytes
   */
  getCurrentUsage(): number {
    return process.memoryUsage().heapUsed;
  }
}
