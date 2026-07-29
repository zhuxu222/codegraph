/**
 * Query pool — runs CPU-heavy read-tool calls on a pool of worker threads so
 * the shared daemon's main event loop stays free for the MCP transport.
 *
 * Why this exists: see {@link ./query-worker}. One daemon, one event loop, one
 * synchronous SQLite connection serializes every concurrent `codegraph_explore`
 * AND starves the transport (a 10-way wave delivered 0 transport heartbeats in
 * 25s — responses can't flush until the whole batch drains, so clients time
 * out). Spreading the dispatch across worker threads (each its own WAL read
 * connection) restores true multi-core parallelism and an idle main loop.
 *
 * Properties:
 *   - lazy growth: one warm worker on construct, grows to `size` on demand, so a
 *     single-agent session pays for one connection and a 10-subagent burst grows
 *     to the core budget.
 *   - crash recovery: a dead worker is respawned and its in-flight call retried
 *     once; a poison call that keeps crashing fails gracefully (never wedges the
 *     pool). A crash budget trips a circuit breaker (`healthy` → false) so the
 *     caller falls back to in-process dispatch instead of thrashing respawns.
 *   - graceful backstop: a call that can't be served within `softTimeoutMs`
 *     resolves with SUCCESS-shaped "busy, retry" guidance — never `isError`, so
 *     a momentary overload can't teach the agent to abandon codegraph — instead
 *     of hanging past the client's hard timeout.
 */

import { Worker } from 'worker_threads';
import * as path from 'path';
import * as os from 'os';
import type { ToolResult } from './tools';
import { findNearestCodeGraphRoot, getCodeGraphDir } from '../directory';

/** Compiled sibling — `query-worker.js` lives next to this file in `dist/mcp/`. */
const WORKER_FILE = path.join(__dirname, 'query-worker.js');

/**
 * Minimal worker surface the pool drives — satisfied by a real `worker_threads`
 * Worker. Abstracted so tests can inject a fake worker and exercise the pool's
 * queue / growth / crash-recovery / backstop logic without spawning threads or
 * needing a built `dist/`.
 */
export interface PoolWorker {
  postMessage(msg: unknown): void;
  terminate(): Promise<number> | void;
  on(event: 'message', cb: (m: unknown) => void): void;
  on(event: 'error', cb: (e: Error) => void): void;
  on(event: 'exit', cb: (code: number) => void): void;
}

/** Default linger before a queued call is answered with busy-guidance. */
const DEFAULT_BUSY_TIMEOUT_MS = 45_000; // < the ~60s MCP client request timeout

/** Hard ceiling on pool size regardless of core count / env. */
const MAX_POOL_SIZE = 16;

/**
 * Total worker deaths before the pool declares itself unhealthy and the caller
 * reverts to in-process dispatch. High enough to ride out a few transient
 * crashes, low enough that a systematically-broken worker (e.g. a platform that
 * can't spawn threads) degrades quickly instead of respawning forever.
 */
const CRASH_BUDGET = 12;

/**
 * Max workers cold-starting at once. A worker's cold start is heavy — full
 * module load (tree-sitter etc.) + opening a large WAL DB — and starting the
 * whole pool simultaneously thrashes CPU/I-O so badly it can stall the daemon's
 * main loop for tens of seconds. Warming a couple at a time keeps each start
 * fast; as one reports ready the next begins, so the pool still reaches full
 * size within a few calls of a burst, just without the thundering herd.
 */
const MAX_CONCURRENT_SPAWN = 2;

/** Shape of a message a worker posts back (ready handshake or a tool result). */
interface WorkerMessage {
  type?: string;
  ok?: boolean;
  error?: string;
  id?: number;
  drainId?: number;
  result?: ToolResult;
}

export interface QueryGenerationLease {
  release(): void;
}

interface Job {
  id: number;
  toolName: string;
  args: Record<string, unknown>;
  project: QueryProjectDescriptor;
  /** A provider already acquired a lease for this exact immutable generation. */
  pinnedGeneration: boolean;
  resolve: (r: ToolResult) => void;
  retries: number;
  settled: boolean;
  state: 'queued' | 'inflight' | 'finished';
  generationLease?: QueryGenerationLease;
  enqueuedAt: number;
  softTimer?: NodeJS.Timeout;
}

interface WorkerDrain {
  remaining: Set<PoolWorker>;
  resolve: () => void;
  reject: (error: Error) => void;
}

/**
 * Immutable index generation a query worker can open.
 *
 * `projectRoot` is always the source tree. `dataDir` is the generation's
 * CodeGraph data directory and may live anywhere (for example under a
 * workspace-owned cache). The tuple `(projectId, generationId)` is the worker
 * cache key; paths are deliberately not used as identity so repository renames
 * do not alias generations.
 */
export interface QueryProjectDescriptor {
  projectId: string;
  generationId: string;
  projectRoot: string;
  dataDir: string;
}

/** Versioned snapshot of the generations that should receive new queries. */
export interface QueryCatalogSnapshot {
  revision: number;
  projects: QueryProjectDescriptor[];
  /** Omit for a workspace root that intentionally has no default project. */
  defaultProjectId?: string;
}

export interface QueryPoolOptions {
  /** Default project root each worker opens at spawn. */
  root: string;
  /**
   * Explicit default generation. Omit for the legacy single-project layout,
   * which is represented as `<root>/<CODEGRAPH_DIR || ".codegraph">`.
   */
  defaultProject?: QueryProjectDescriptor;
  /** Optional initial multi-project catalog. */
  catalog?: QueryCatalogSnapshot;
  /** Max worker threads. Defaults to `clamp(cores-1, 1, 16)`. */
  size?: number;
  /** Linger before a queued call gets busy-guidance. Default 45s. */
  softTimeoutMs?: number;
  /** Retries for an in-flight call whose worker crashed. Default 1. */
  maxRetries?: number;
  /** Worker factory (tests inject a fake). Defaults to a real `worker_threads` Worker. */
  createWorker?: () => PoolWorker;
  /**
   * Optional manager-owned generation lease. The pool acquires it immediately
   * before dispatching a worker call and releases it only when worker execution
   * actually ends (result, terminal crash, or destroy). A client-facing soft
   * timeout does not release this lease.
   */
  acquireGenerationLease?: (
    project: QueryProjectDescriptor,
  ) => QueryGenerationLease | null;
}

/**
 * Resolve the pool size from the `CODEGRAPH_QUERY_POOL_SIZE` override and the
 * machine's core count. `0` (or a negative) explicitly disables the pool (the
 * caller serves in-process — today's behavior). Unset → `clamp(cores-1, 1, 16)`:
 * leave a core for the main loop + OS, but never zero, since even one worker
 * frees the transport and lets responses flush incrementally.
 */
export function resolvePoolSize(envVal: string | undefined, cpuCount: number): number {
  if (envVal !== undefined && envVal !== '') {
    const n = Number(envVal);
    if (Number.isFinite(n) && n >= 0) return Math.min(Math.floor(n), MAX_POOL_SIZE);
    // non-numeric / negative → fall through to the default
  }
  return Math.max(1, Math.min(cpuCount - 1, MAX_POOL_SIZE));
}

function resolveBusyTimeoutMs(): number {
  const raw = process.env.CODEGRAPH_QUERY_BUSY_TIMEOUT_MS;
  if (raw === undefined || raw === '') return DEFAULT_BUSY_TIMEOUT_MS;
  const n = Number(raw);
  if (!Number.isFinite(n) || n < 1000) return DEFAULT_BUSY_TIMEOUT_MS;
  return Math.floor(n);
}

/** Success-shaped overload guidance (NEVER isError — see the abandonment rule). */
function busyGuidance(waitedMs: number): ToolResult {
  const secs = Math.max(1, Math.round(waitedMs / 1000));
  return {
    content: [{
      type: 'text',
      text:
        `CodeGraph is busy serving other concurrent requests right now (this call waited ${secs}s in the queue). ` +
        `This is NOT an error and the index is fine — wait a few seconds and retry this exact call; it will return normally. ` +
        `If you can't wait, use your built-in tools for just this one step.`,
    }],
  };
}

export class QueryPool {
  private idle: PoolWorker[] = [];
  private queue: Job[] = [];
  private inflight = new Map<PoolWorker, Job>();
  private workers = new Set<PoolWorker>();
  private terminatingWorkers = new Set<PoolWorker>();
  // Workers spawned but not yet 'ready'. Growth must count these so a single
  // first call (with the eager worker still starting) doesn't spawn the WHOLE
  // pool at once — N simultaneous cold worker starts (each a full module load +
  // a large DB open) saturate the box and starve the main loop. Grow only when
  // the queue outstrips idle + pending.
  private pendingWorkers = new Set<PoolWorker>();
  private nextId = 1;
  private nextDrainId = 1;
  private totalCrashes = 0;
  private destroyed = false;
  private readonly root: string;
  private readonly legacyProject: QueryProjectDescriptor;
  private catalog = new Map<string, QueryProjectDescriptor>();
  private catalogRevisionValue = 0;
  private defaultProjectId: string | null;
  private catalogManaged = false;
  private readonly maxSize: number;
  private readonly softTimeoutMs: number;
  private readonly maxRetries: number;
  private readonly createWorker: () => PoolWorker;
  private readonly acquireGenerationLease:
    ((project: QueryProjectDescriptor) => QueryGenerationLease | null) | null;
  private readonly generationDrainStates =
    new Map<string, 'draining' | 'drained'>();
  private readonly generationDrains = new Map<string, Promise<void>>();
  private readonly generationDrainTokens = new Map<string, symbol>();
  private readonly generationJobWaiters =
    new Map<string, Set<() => void>>();
  private readonly workerDrains = new Map<number, WorkerDrain>();

  constructor(opts: QueryPoolOptions) {
    this.root = opts.root;
    this.legacyProject = opts.defaultProject ?? {
      projectId: 'legacy',
      generationId: 'legacy',
      projectRoot: this.root,
      dataDir: getCodeGraphDir(this.root),
    };
    this.catalogManaged = opts.defaultProject !== undefined;
    this.defaultProjectId = this.legacyProject.projectId;
    this.catalog.set(this.legacyProject.projectId, this.legacyProject);
    if (opts.catalog) this.applyCatalog(opts.catalog);
    this.maxSize = Math.max(1, Math.min(opts.size ?? Math.max(1, os.cpus().length - 1), MAX_POOL_SIZE));
    this.softTimeoutMs = opts.softTimeoutMs ?? resolveBusyTimeoutMs();
    this.maxRetries = opts.maxRetries ?? 1;
    this.acquireGenerationLease = opts.acquireGenerationLease ?? null;
    this.createWorker = opts.createWorker ?? (() => new Worker(WORKER_FILE, {
      workerData: {
        root: this.root,
        defaultProject: this.getWorkerDefaultProject(),
      },
    }));
    this.spawnOne(); // one eager warm worker, ready for the first call
  }

  /** Pool size cap (for logging/status). */
  get size(): number { return this.maxSize; }

  /** Live worker count (for tests/status). */
  get liveWorkers(): number { return this.workers.size; }

  /** Catalog revision currently used to route newly-enqueued calls. */
  get catalogRevision(): number { return this.catalogRevisionValue; }

  /**
   * False once the crash budget is exhausted (or after destroy). The ToolHandler
   * checks this and falls back to in-process dispatch — a broken worker platform
   * degrades to today's behavior instead of failing tool calls.
   */
  get healthy(): boolean {
    return !this.destroyed && this.totalCrashes < CRASH_BUDGET;
  }

  /**
   * True once at least one worker has completed its cold start (posted the
   * 'ready' handshake). Until then the ToolHandler serves calls IN-PROCESS:
   * a worker cold start is a full module load + DB open — seconds normally,
   * tens of seconds on a loaded machine — and a call queued behind it gets
   * nothing until the 45s busy backstop. The daemon's very first tool call
   * hitting that window was the recurring #662 test flake (and a real
   * first-call stall for agents). The pool exists for CONCURRENT load, which
   * by definition arrives after warm-up; the pre-pool in-process path is
   * strictly better while nothing is warm. Stays true for the pool's
   * lifetime — later crash-respawn gaps are covered by retry + backstop.
   */
  get ready(): boolean {
    return this.everReady && !this.destroyed;
  }
  private everReady = false;

  /**
   * Replace the active-generation catalog. Stale/equal revisions are ignored,
   * which makes reconnect/replay of controller notifications idempotent.
   *
   * Existing in-flight jobs keep their captured descriptor. Queued jobs are not
   * in flight yet, so they are rebound to the newly-active generation.
   */
  setCatalog(snapshot: QueryCatalogSnapshot): void {
    if (
      this.destroyed ||
      snapshot.revision < this.catalogRevisionValue ||
      (this.catalogManaged && snapshot.revision === this.catalogRevisionValue)
    ) return;
    this.applyCatalog(snapshot);
    this.broadcastCatalog();
  }

  /** Alias used by controllers that call their operation an update. */
  updateCatalog(snapshot: QueryCatalogSnapshot): void {
    this.setCatalog(snapshot);
  }

  /**
   * Atomically route future/queued queries for one project to a new immutable
   * generation, then tell every worker to retire the prior cached generation.
   */
  promote(project: QueryProjectDescriptor, revision = this.catalogRevisionValue + 1): void {
    if (this.destroyed || revision <= this.catalogRevisionValue) return;
    this.assertDescriptor(project);
    const previous = this.catalog.get(project.projectId);
    this.catalogManaged = true;
    this.catalog.set(project.projectId, project);
    this.clearGenerationDrain(project);
    this.catalogRevisionValue = revision;
    this.retargetQueuedJobs(project);
    this.broadcast({
      type: 'promote',
      revision,
      project,
      previousGenerationId: previous?.generationId,
    });
  }

  /**
   * Retire cached connections. With a generation id only that immutable
   * generation is invalidated; without one every generation for the project is
   * retired. In-flight calls finish before the worker closes their connection.
   */
  invalidate(projectId: string, generationId?: string): void {
    if (this.destroyed) return;
    const active = this.catalog.get(projectId);
    if (!generationId || active?.generationId === generationId) {
      this.catalog.delete(projectId);
      if (this.defaultProjectId === projectId) this.defaultProjectId = null;
      for (const job of [...this.queue]) {
      if (!job.settled && job.project.projectId === projectId) {
          this.queue = this.queue.filter((candidate) => candidate !== job);
          this.settle(job, {
            isError: true,
            content: [{
              type: 'text',
              text: `CodeGraph generation for project "${projectId}" was invalidated; retry after the catalog refreshes.`,
            }],
          });
          this.finishWork(job);
        }
      }
    }
    this.broadcast({ type: 'invalidate', projectId, generationId });
    this.drain();
  }

  /**
   * Stop dispatching one inactive generation, wait for every already-dispatched
   * call to finish, then invalidate that generation in every live worker and
   * wait for explicit acknowledgements. This is the deletion barrier used by
   * generation managers before removing SQLite/WAL files (especially on
   * Windows, where an idle worker handle otherwise prevents deletion).
   *
   * Client-facing soft timeouts do not complete this barrier: an in-flight job
   * remains tracked until its worker returns or terminates.
   */
  drainGeneration(projectId: string, generationId: string): Promise<void> {
    if (this.destroyed) return Promise.resolve();
    if (!projectId || !generationId) {
      return Promise.reject(
        new Error('projectId and generationId are required to drain a generation'),
      );
    }
    const key = generationKey(projectId, generationId);
    const existing = this.generationDrains.get(key);
    if (existing) return existing;
    if (this.generationDrainStates.get(key) === 'drained') {
      return Promise.resolve();
    }

    const token = Symbol(key);
    this.generationDrainStates.set(key, 'draining');
    this.generationDrainTokens.set(key, token);
    let operation!: Promise<void>;
    operation = this.performGenerationDrain(
      key,
      token,
      projectId,
      generationId,
    )
      .then(() => {
        if (
          this.generationDrainTokens.get(key) === token
          && this.generationDrainStates.get(key) === 'draining'
        ) {
          this.generationDrainStates.set(key, 'drained');
        }
      })
      .catch((error: unknown) => {
        if (
          this.generationDrainTokens.get(key) === token
          && this.generationDrainStates.get(key) === 'draining'
        ) {
          this.generationDrainStates.delete(key);
        }
        throw error;
      })
      .finally(() => {
        if (this.generationDrains.get(key) === operation) {
          this.generationDrains.delete(key);
        }
        if (this.generationDrainTokens.get(key) === token) {
          this.generationDrainTokens.delete(key);
        }
      });
    this.generationDrains.set(key, operation);
    return operation;
  }

  private applyCatalog(snapshot: QueryCatalogSnapshot): void {
    const previousIds = new Set(this.catalog.keys());
    const next = new Map<string, QueryProjectDescriptor>();
    for (const project of snapshot.projects) {
      this.assertDescriptor(project);
      if (next.has(project.projectId)) {
        throw new Error(`Duplicate query catalog project id "${project.projectId}"`);
      }
      next.set(project.projectId, project);
      this.clearGenerationDrain(project);
      previousIds.delete(project.projectId);
    }
    if (snapshot.defaultProjectId && !next.has(snapshot.defaultProjectId)) {
      throw new Error(`Query catalog default project "${snapshot.defaultProjectId}" is not present`);
    }
    this.catalogManaged = true;
    this.catalog = next;
    this.catalogRevisionValue = snapshot.revision;
    this.defaultProjectId = snapshot.defaultProjectId ?? null;
    for (const project of next.values()) this.retargetQueuedJobs(project);
    if (previousIds.size > 0 && this.queue.length > 0) {
      const retained: Job[] = [];
      for (const job of this.queue) {
        if (!job.settled && previousIds.has(job.project.projectId)) {
          this.settle(job, {
            isError: true,
            content: [{
              type: 'text',
              text: `CodeGraph project "${job.project.projectId}" was removed from the catalog; retry with a registered project.`,
            }],
          });
          this.finishWork(job);
        } else {
          retained.push(job);
        }
      }
      this.queue = retained;
    }
  }

  private assertDescriptor(project: QueryProjectDescriptor): void {
    for (const field of ['projectId', 'generationId', 'projectRoot', 'dataDir'] as const) {
      const value = project[field];
      if (typeof value !== 'string' || value.length === 0) {
        throw new Error(`Invalid query project descriptor field "${field}"`);
      }
    }
  }

  private retargetQueuedJobs(project: QueryProjectDescriptor): void {
    for (const job of this.queue) {
      if (
        !job.settled &&
        !job.pinnedGeneration &&
        job.project.projectId === project.projectId
      ) {
        job.project = project;
      }
    }
  }

  private clearGenerationDrain(project: QueryProjectDescriptor): void {
    const key = generationKey(project.projectId, project.generationId);
    this.generationDrainStates.delete(key);
    this.generationDrainTokens.delete(key);
    this.generationDrains.delete(key);
    this.maybeResolveGenerationWaiters(key);
  }

  private async performGenerationDrain(
    key: string,
    token: symbol,
    projectId: string,
    generationId: string,
  ): Promise<void> {
    const retained: Job[] = [];
    for (const job of this.queue) {
      if (generationKeyFor(job.project) !== key) {
        retained.push(job);
        continue;
      }
      if (!job.settled) {
        this.settle(job, {
          isError: true,
          content: [{
            type: 'text',
            text:
              `CodeGraph generation ${projectId}/${generationId} is being retired; `
              + 'retry the call against the active generation.',
          }],
        });
      }
      this.finishWork(job);
    }
    this.queue = retained;
    this.drain();

    await this.waitForGenerationJobs(key);
    // A rollback/catalog activation can cancel a pending drain while it waits
    // for an old worker call. In that case do not invalidate the active target.
    if (this.destroyed) return;
    if (
      this.generationDrainTokens.get(key) !== token
      || this.generationDrainStates.get(key) !== 'draining'
    ) {
      throw new Error(
        `CodeGraph generation drain for ${projectId}/${generationId} `
        + 'was canceled by catalog reactivation.',
      );
    }
    await this.requestWorkerDrain(projectId, generationId);
    if (
      !this.destroyed
      && (
        this.generationDrainTokens.get(key) !== token
        || this.generationDrainStates.get(key) !== 'draining'
      )
    ) {
      throw new Error(
        `CodeGraph generation drain for ${projectId}/${generationId} `
        + 'was canceled by catalog reactivation.',
      );
    }
  }

  private waitForGenerationJobs(key: string): Promise<void> {
    if (!this.hasGenerationWork(key)) return Promise.resolve();
    return new Promise<void>((resolve) => {
      const waiters = this.generationJobWaiters.get(key) ?? new Set();
      waiters.add(resolve);
      this.generationJobWaiters.set(key, waiters);
    });
  }

  private hasGenerationWork(key: string): boolean {
    return (
      this.queue.some(
        (job) =>
          job.state !== 'finished'
          && generationKeyFor(job.project) === key,
      )
      || [...this.inflight.values()].some(
        (job) =>
          job.state !== 'finished'
          && generationKeyFor(job.project) === key,
      )
    );
  }

  private maybeResolveGenerationWaiters(key: string): void {
    if (
      this.generationDrainStates.get(key) === 'draining'
      && this.hasGenerationWork(key)
    ) {
      return;
    }
    const waiters = this.generationJobWaiters.get(key);
    if (!waiters) return;
    this.generationJobWaiters.delete(key);
    for (const resolve of waiters) resolve();
  }

  private requestWorkerDrain(
    projectId: string,
    generationId: string,
  ): Promise<void> {
    const workers = new Set(this.workers);
    if (workers.size === 0) return Promise.resolve();
    const drainId = this.nextDrainId++;
    return new Promise<void>((resolve, reject) => {
      this.workerDrains.set(drainId, { remaining: workers, resolve, reject });
      for (const worker of [...workers]) {
        try {
          worker.postMessage({
            type: 'drain',
            drainId,
            projectId,
            generationId,
          });
        } catch {
          this.terminateFailedWorker(worker);
        }
      }
      this.maybeResolveWorkerDrain(drainId);
    });
  }

  private maybeResolveWorkerDrain(drainId: number): void {
    const drain = this.workerDrains.get(drainId);
    if (!drain || drain.remaining.size > 0) return;
    this.workerDrains.delete(drainId);
    drain.resolve();
  }

  private removeWorkerFromDrains(worker: PoolWorker): void {
    for (const [drainId, drain] of this.workerDrains) {
      drain.remaining.delete(worker);
      this.maybeResolveWorkerDrain(drainId);
    }
  }

  private getDefaultProject(): QueryProjectDescriptor | null {
    return this.defaultProjectId ? this.catalog.get(this.defaultProjectId) ?? null : null;
  }

  private getWorkerDefaultProject(): QueryProjectDescriptor | undefined {
    const project = this.catalogManaged
      ? this.getDefaultProject()
      : this.legacyProject;
    if (
      project === null
      || this.generationDrainStates.has(generationKeyFor(project))
    ) {
      return undefined;
    }
    return project;
  }

  private broadcastCatalog(worker?: PoolWorker): void {
    const message = {
      type: 'catalog',
      revision: this.catalogRevisionValue,
      projects: [...this.catalog.values()],
      defaultProjectId: this.defaultProjectId ?? undefined,
    };
    if (worker) worker.postMessage(message);
    else this.broadcast(message);
  }

  private broadcast(message: unknown): void {
    for (const worker of this.workers) {
      try { worker.postMessage(message); } catch { /* worker exit path handles it */ }
    }
  }

  private spawnOne(): void {
    if (this.destroyed || this.workers.size >= this.maxSize) return;
    let w: PoolWorker;
    try {
      w = this.createWorker();
    } catch {
      this.totalCrashes++; // counts toward the circuit breaker
      return;
    }
    this.workers.add(w);
    this.pendingWorkers.add(w);
    w.on('message', (m) => this.onMessage(w, (m ?? {}) as WorkerMessage));
    w.on('error', () => this.terminateFailedWorker(w));
    // Any unrequested exit removes the worker from generation-drain barriers.
    // destroy() clears `workers` first, so expected termination is ignored.
    w.on('exit', () => this.onWorkerGone(w));
  }

  private onMessage(w: PoolWorker, m: WorkerMessage): void {
    if (
      !m
      || !this.workers.has(w)
      || this.terminatingWorkers.has(w)
    ) return;
    if (m.type === 'drained' && typeof m.drainId === 'number') {
      const drain = this.workerDrains.get(m.drainId);
      if (!drain || !drain.remaining.has(w)) return;
      if (m.ok === false) {
        this.workerDrains.delete(m.drainId);
        drain.reject(new Error(
          m.error
            ?? 'CodeGraph query worker could not close a drained generation',
        ));
        return;
      }
      drain.remaining.delete(w);
      this.maybeResolveWorkerDrain(m.drainId);
      return;
    }
    if (m.type === 'ready') {
      this.pendingWorkers.delete(w);
      if (m.ok === false) this.totalCrashes++; // hard open failure
      else this.everReady = true;
      // A worker may have spent seconds cold-starting while promotions happened.
      // Seed it with the latest catalog before it can receive a queued call.
      this.broadcastCatalog(w);
      this.idle.push(w);
      this.drain();
      return;
    }
    if (m.type === 'result') {
      const job = this.inflight.get(w);
      this.inflight.delete(w);
      this.idle.push(w);
      if (job) {
        this.settle(job, m.result ?? busyGuidance(0));
        this.finishWork(job);
      }
      this.drain();
    }
  }

  // A worker died (crash hook, OOM, segfault, exit≠0). Respawn a replacement and
  // retry its in-flight job once; a job that keeps crashing workers fails
  // gracefully so it can't loop the pool forever.
  /**
   * An `error` event or a failed postMessage does not itself prove that the
   * worker thread (and its SQLite handles) is gone. Keep it in every drain
   * barrier until termination completes or the worker emits `exit`.
   */
  private terminateFailedWorker(w: PoolWorker): void {
    if (
      !this.workers.has(w)
      || this.terminatingWorkers.has(w)
    ) return;
    this.terminatingWorkers.add(w);
    this.pendingWorkers.delete(w);
    this.idle = this.idle.filter((candidate) => candidate !== w);
    void (async () => {
      try {
        await w.terminate();
      } catch {
        // exit/finalization still owns cleanup
      } finally {
        this.terminatingWorkers.delete(w);
        this.onWorkerGone(w);
      }
    })();
  }

  private onWorkerGone(w: PoolWorker): void {
    if (!this.workers.has(w)) return; // already handled (error+exit both fire)
    this.workers.delete(w);
    this.terminatingWorkers.delete(w);
    this.pendingWorkers.delete(w);
    this.idle = this.idle.filter((x) => x !== w);
    this.removeWorkerFromDrains(w);
    this.totalCrashes++;
    const job = this.inflight.get(w);
    this.inflight.delete(w);
    if (this.healthy) this.spawnOne(); // keep capacity
    if (job) {
      const draining =
        this.generationDrainStates.get(generationKeyFor(job.project))
        === 'draining';
      if (
        !job.settled
        && !draining
        && job.retries < this.maxRetries
        && this.healthy
      ) {
        job.retries++;
        job.state = 'queued';
        this.queue.unshift(job); // head of line — retry promptly
      } else {
        this.settle(job, {
          isError: true,
          content: [{
            type: 'text',
            text: draining
              ? 'codegraph generation is being retired; retry the call.'
              : 'codegraph worker crashed; please retry the call.',
          }],
        });
        this.finishWork(job);
      }
    }
    this.drain();
  }

  private drain(): void {
    // Grow toward maxSize while queued work outstrips workers that are idle OR
    // already on their way up (pending) — so we never spawn the whole pool for a
    // single call whose eager worker just hasn't reported ready yet.
    while (
      this.queue.length > this.idle.length + this.pendingWorkers.size &&
      this.workers.size < this.maxSize &&
      this.pendingWorkers.size < MAX_CONCURRENT_SPAWN &&
      this.healthy
    ) {
      this.spawnOne();
    }
    while (this.idle.length && this.queue.length) {
      // Skip jobs the backstop already answered.
      let job: Job | undefined;
      while (this.queue.length && (job = this.queue.shift()) && job.settled) {
        this.finishWork(job);
        job = undefined;
      }
      if (!job || job.settled) break;
      const generationKey = generationKeyFor(job.project);
      if (this.generationDrainStates.has(generationKey)) {
        this.settle(job, {
          isError: true,
          content: [{
            type: 'text',
            text:
              'CodeGraph generation is being retired; '
              + 'retry against the active generation.',
          }],
        });
        this.finishWork(job);
        continue;
      }
      if (!job.generationLease && this.acquireGenerationLease) {
        try {
          const lease = this.acquireGenerationLease(job.project);
          if (lease === null) {
            this.settle(job, {
              isError: true,
              content: [{
                type: 'text',
                text:
                  'CodeGraph generation is no longer available; '
                  + 'retry against the active generation.',
              }],
            });
            this.finishWork(job);
            continue;
          }
          job.generationLease = lease;
        } catch (error) {
          this.settle(job, {
            isError: true,
            content: [{
              type: 'text',
              text:
                `Could not lease CodeGraph generation: ${
                  error instanceof Error ? error.message : String(error)
                }`,
            }],
          });
          this.finishWork(job);
          continue;
        }
      }
      const w = this.idle.pop()!;
      job.state = 'inflight';
      this.inflight.set(w, job);
      try {
        w.postMessage({
          type: 'call',
          id: job.id,
          toolName: job.toolName,
          args: job.args,
          project: job.project,
          pinnedGeneration: job.pinnedGeneration,
          catalogRevision: this.catalogRevisionValue,
        });
      } catch {
        this.terminateFailedWorker(w);
      }
    }
  }

  private settle(job: Job, result: ToolResult): void {
    if (job.settled) return; // already answered (by backstop or worker)
    job.settled = true;
    if (job.softTimer) clearTimeout(job.softTimer);
    job.resolve(result);
  }

  private finishWork(job: Job): void {
    if (job.state === 'finished') return;
    job.state = 'finished';
    const lease = job.generationLease;
    job.generationLease = undefined;
    if (lease) {
      try { lease.release(); } catch { /* lease owner remains authoritative */ }
    }
    this.maybeResolveGenerationWaiters(generationKeyFor(job.project));
  }

  /**
   * Run a read tool on the pool. Always resolves (never rejects).
   *
   * The optional target may be a complete descriptor (useful before publishing
   * it into a catalog) or a registered project id. Without it, `projectPath` is
   * longest-prefix matched against the catalog; otherwise the catalog default
   * is used. Legacy pools keep routing to their original single root.
   */
  run(
    toolName: string,
    args: Record<string, unknown>,
    target?: QueryProjectDescriptor | string,
  ): Promise<ToolResult> {
    return new Promise<ToolResult>((resolve) => {
      if (this.destroyed) {
        resolve({
          isError: true,
          content: [{
            type: 'text',
            text: 'codegraph is shutting down; retry shortly.',
          }],
        });
        return;
      }
      const project = this.resolveProject(args, target);
      if (!project) {
        resolve({
          isError: true,
          content: [{
            type: 'text',
            text: 'No registered CodeGraph project matches this query. Pass a registered projectPath.',
          }],
        });
        return;
      }
      const job: Job = {
        id: this.nextId++, toolName, args, project, resolve,
        pinnedGeneration: typeof target === 'object' && target !== null,
        retries: 0, settled: false, state: 'queued', enqueuedAt: Date.now(),
      };
      // Don't let the caller wait past softTimeoutMs. The worker may still be
      // busy (we can't cancel synchronous CPU), but the CLIENT gets a prompt,
      // success-shaped "retry" instead of a hard timeout.
      job.softTimer = setTimeout(() => {
        if (job.settled) return;
        this.settle(job, busyGuidance(Date.now() - job.enqueuedAt));
        if (job.state === 'queued') {
          this.queue = this.queue.filter((candidate) => candidate !== job);
          this.finishWork(job);
          this.drain();
        }
      }, this.softTimeoutMs);
      job.softTimer.unref?.();
      this.queue.push(job);
      this.drain();
    });
  }

  private resolveProject(
    args: Record<string, unknown>,
    target?: QueryProjectDescriptor | string,
  ): QueryProjectDescriptor | null {
    if (typeof target === 'object' && target !== null) {
      this.assertDescriptor(target);
      return target;
    }
    if (typeof target === 'string') return this.catalog.get(target) ?? null;

    const requestedPath = typeof args.projectPath === 'string'
      ? path.resolve(args.projectPath)
      : null;
    if (requestedPath && !this.catalogManaged) {
      const projectRoot = findNearestCodeGraphRoot(requestedPath);
      if (!projectRoot) return null;
      const canonicalRoot = path.resolve(projectRoot);
      const identityRoot = process.platform === 'win32'
        ? canonicalRoot.toLowerCase()
        : canonicalRoot;
      return {
        projectId: `legacy:${identityRoot}`,
        generationId: 'legacy',
        projectRoot: canonicalRoot,
        dataDir: getCodeGraphDir(canonicalRoot),
      };
    }
    if (requestedPath && this.catalogManaged) {
      const foldedRequested = process.platform === 'win32'
        ? requestedPath.toLowerCase()
        : requestedPath;
      let best: QueryProjectDescriptor | null = null;
      for (const project of this.catalog.values()) {
        const root = path.resolve(project.projectRoot);
        const foldedRoot = process.platform === 'win32' ? root.toLowerCase() : root;
        const relative = path.relative(foldedRoot, foldedRequested);
        if (relative === '' || (!relative.startsWith('..') && !path.isAbsolute(relative))) {
          if (!best || root.length > path.resolve(best.projectRoot).length) best = project;
        }
      }
      return best;
    }

    return this.getDefaultProject() ?? (this.catalogManaged ? null : this.legacyProject);
  }

  /** Terminate all workers and answer any outstanding calls gracefully. */
  async destroy(): Promise<void> {
    if (this.destroyed) return;
    this.destroyed = true;
    const ws = [...this.workers];
    this.workers.clear();
    this.terminatingWorkers.clear();
    this.pendingWorkers.clear();
    this.idle = [];
    const outstanding = [...this.inflight.values(), ...this.queue];
    for (const job of outstanding) {
      this.settle(job, { isError: true, content: [{ type: 'text', text: 'codegraph is shutting down; retry shortly.' }] });
    }
    this.inflight.clear();
    this.queue = [];
    // Do not release generation leases or drain waiters until termination has
    // completed: on Windows, a merely-requested termination can still own the
    // SQLite/WAL file handles that generation GC is waiting to delete.
    await Promise.all(ws.map(async (w) => {
      try {
        await w.terminate();
      } catch {
        // already gone
      }
    }));
    for (const job of outstanding) this.finishWork(job);
    for (const drain of this.workerDrains.values()) {
      drain.remaining.clear();
      drain.resolve();
    }
    this.workerDrains.clear();
    for (const waiters of this.generationJobWaiters.values()) {
      for (const resolve of waiters) resolve();
    }
    this.generationJobWaiters.clear();
  }
}

function generationKey(projectId: string, generationId: string): string {
  return `${projectId}\u0000${generationId}`;
}

function generationKeyFor(project: QueryProjectDescriptor): string {
  return generationKey(project.projectId, project.generationId);
}
