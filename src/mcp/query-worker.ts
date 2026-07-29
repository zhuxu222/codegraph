/**
 * Query worker thread — issue: concurrent MCP tool calls starve the daemon.
 *
 * The shared daemon serves every session on ONE event loop with synchronous
 * `node:sqlite`. `codegraph_explore` is CPU-heavy (FTS + RWR/personalized-
 * PageRank + impact + output building) stitched together by microtask `await`s,
 * so N concurrent explores keep the microtask queue continuously full and
 * starve the macrotask phases — timers AND socket I/O. The transport freezes:
 * no response flushes, no request is read, until the whole batch drains. With
 * ~10 subagents that routinely exceeds the MCP client's request timeout.
 *
 * This worker moves the heavy read-tool dispatch OFF the daemon's main loop.
 * Each worker owns its OWN read connection (node:sqlite WAL allows N concurrent
 * readers across connections — verified: a worker reader sees the main writer's
 * committed catch-up/watcher writes), so {@link QueryPool} runs N tool calls in
 * true parallel up to core count while the main loop stays free for the MCP
 * transport. The worker runs {@link ToolHandler.executeReadTool} — validation +
 * dispatch + error classification — and returns the raw {@link ToolResult}; the
 * MAIN thread keeps the catch-up gate, the watcher-state notices (staleness /
 * worktree), `codegraph_status`, and telemetry, none of which a watcher-less
 * read connection can answer.
 */

import { parentPort, workerData } from 'worker_threads';
import * as path from 'path';
import type CodeGraph from '../index';
import type { ToolResult } from './tools';
import type { QueryProjectDescriptor } from './query-pool';

interface WorkerInit {
  root: string;
  defaultProject?: QueryProjectDescriptor;
}

interface CallMessage {
  type: 'call';
  id: number;
  toolName: string;
  args: Record<string, unknown>;
  project: QueryProjectDescriptor;
  /** Main-thread provider holds a whole-request lease on this generation. */
  pinnedGeneration: boolean;
  catalogRevision: number;
}

interface CatalogMessage {
  type: 'catalog';
  revision: number;
  projects: QueryProjectDescriptor[];
  defaultProjectId?: string;
}

interface PromoteMessage {
  type: 'promote';
  revision: number;
  project: QueryProjectDescriptor;
  previousGenerationId?: string;
}

interface InvalidateMessage {
  type: 'invalidate';
  projectId: string;
  generationId?: string;
}

interface DrainMessage {
  type: 'drain';
  drainId: number;
  projectId: string;
  generationId: string;
}

type WorkerRequest =
  | CallMessage
  | CatalogMessage
  | PromoteMessage
  | InvalidateMessage
  | DrainMessage;

interface CachedProject {
  descriptor: QueryProjectDescriptor;
  graph: CodeGraph;
  handler: InstanceType<typeof import('./tools').ToolHandler>;
  lastUsed: number;
  references: number;
  retired: boolean;
  closeError?: string;
}

/** Four SQLite handles per worker bounds FD/WAL and memory growth. */
const MAX_CACHED_PROJECTS = 4;

// Mirror the engine's lazy-require of the heavy CodeGraph + tools chain. This
// module is only ever loaded as a Worker, so the require runs once on spawn.
const loadCodeGraph = (): typeof import('../index').default =>
  (require('../index') as typeof import('../index')).default;
const loadToolHandler = (): typeof import('./tools').ToolHandler =>
  (require('./tools') as typeof import('./tools')).ToolHandler;

if (parentPort) {
  const port = parentPort;
  const { defaultProject } = workerData as WorkerInit;
  const cache = new Map<string, CachedProject>();
  const activeCatalog = new Map<string, QueryProjectDescriptor>();
  const invalidatedProjects = new Set<string>();
  const invalidatedGenerations = new Set<string>();
  const pendingDrainAcks = new Map<
    number,
    { projectId: string; generationId: string }
  >();
  let catalogRevision = -1;
  let lruClock = 0;
  let initError: string | null = null;

  // Preserve the legacy warm-first-call behavior. A workspace catalog with no
  // default intentionally starts with an empty cache and opens on first target.
  if (defaultProject) {
    activeCatalog.set(defaultProject.projectId, defaultProject);
    try {
      openProject(defaultProject);
    } catch (err) {
      initError = err instanceof Error ? err.message : String(err);
    }
  }

  // Tell the pool we're up. `ok:false` lets the pool count a hard open failure
  // against its crash budget (→ fall back to in-process) without hanging.
  port.postMessage({ type: 'ready', ok: initError === null, error: initError });

  port.on('message', (msg: WorkerRequest) => {
    if (!msg) return;
    if (msg.type === 'catalog') {
      applyCatalog(msg);
      return;
    }
    if (msg.type === 'promote') {
      applyPromotion(msg);
      return;
    }
    if (msg.type === 'invalidate') {
      invalidate(msg.projectId, msg.generationId);
      return;
    }
    if (msg.type === 'drain') {
      pendingDrainAcks.set(msg.drainId, {
        projectId: msg.projectId,
        generationId: msg.generationId,
      });
      invalidate(msg.projectId, msg.generationId);
      flushDrainAcks();
      return;
    }
    if (msg.type === 'call') void serve(msg);
  });

  const serve = async (msg: CallMessage): Promise<void> => {
    // Test-only crash hook so the pool's worker-recovery path is exercisable
    // deterministically. Gated behind an env flag only the suite sets — inert in
    // normal operation (and `__test_crash__` isn't a real tool name anyway).
    if (msg.toolName === '__test_crash__' && process.env.CODEGRAPH_QUERY_WORKER_ALLOW_TEST_CRASH === '1') {
      process.exit(13);
    }
    let entry: CachedProject | null = null;
    try {
      if (
        invalidatedProjects.has(msg.project.projectId) ||
        invalidatedGenerations.has(projectKey(msg.project))
      ) {
        throw new Error(
          `generation ${msg.project.generationId} for project ${msg.project.projectId} was invalidated`,
        );
      }
      const active = activeCatalog.get(msg.project.projectId);
      if (
        !msg.pinnedGeneration &&
        active &&
        !sameDescriptor(active, msg.project)
      ) {
        throw new Error(
          `generation ${msg.project.generationId} is retired; active generation is ${active.generationId}`,
        );
      }
      entry = acquireProject(msg.project);
      // executeReadTool already classifies NotIndexed/PathRefusal/internal errors
      // into a ToolResult and never throws — the catch is belt-and-suspenders.
      // `projectPath` selected this descriptor on the main thread. Removing only
      // that routing hint prevents ToolHandler's legacy embedded-index discovery
      // from bypassing the explicitly external `dataDir`.
      const callArgs = { ...msg.args };
      delete callArgs.projectPath;
      const result: ToolResult = await entry.handler.executeReadTool(msg.toolName, callArgs);
      port.postMessage({ type: 'result', id: msg.id, result });
    } catch (err) {
      port.postMessage({
        type: 'result',
        id: msg.id,
        result: errorResult(err instanceof Error ? err.message : String(err)),
      });
    } finally {
      if (entry) {
        releaseProject(entry);
        flushDrainAcks();
      }
    }
  };

  function projectKey(project: QueryProjectDescriptor): string {
    return `${project.projectId}\u0000${project.generationId}`;
  }

  function sameDescriptor(
    left: QueryProjectDescriptor,
    right: QueryProjectDescriptor,
  ): boolean {
    const normalize = (value: string): string => {
      const resolved = path.resolve(value);
      return process.platform === 'win32' ? resolved.toLowerCase() : resolved;
    };
    return (
      left.projectId === right.projectId &&
      left.generationId === right.generationId &&
      normalize(left.projectRoot) === normalize(right.projectRoot) &&
      normalize(left.dataDir) === normalize(right.dataDir)
    );
  }

  function openProject(project: QueryProjectDescriptor): CachedProject {
    const key = projectKey(project);
    const cached = cache.get(key);
    if (cached) {
      if (
        sameDescriptor(cached.descriptor, project)
        && cached.closeError === undefined
      ) {
        cached.lastUsed = ++lruClock;
        cached.retired = false;
        return cached;
      }
      cached.retired = true;
      if (cached.references > 0) {
        throw new Error(
          `generation ${project.projectId}/${project.generationId} changed location while an old query is still in flight; retry shortly`,
        );
      }
      if (!closeProject(cached)) {
        throw new Error(
          `could not close prior CodeGraph generation handle: ${cached.closeError}`,
        );
      }
    }

    evictToFit();
    if (cache.size >= MAX_CACHED_PROJECTS) {
      throw new Error('all CodeGraph worker cache entries are serving in-flight queries');
    }

    const graph = loadCodeGraph().openSync({
      projectRoot: project.projectRoot,
      dataDir: project.dataDir,
    });
    const entry: CachedProject = {
      descriptor: project,
      graph,
      handler: new (loadToolHandler())(graph),
      lastUsed: ++lruClock,
      references: 0,
      retired: false,
    };
    cache.set(key, entry);
    return entry;
  }

  function acquireProject(project: QueryProjectDescriptor): CachedProject {
    const entry = openProject(project);
    entry.references++;
    entry.lastUsed = ++lruClock;
    return entry;
  }

  function releaseProject(entry: CachedProject): void {
    entry.references = Math.max(0, entry.references - 1);
    if (entry.retired && entry.references === 0) closeProject(entry);
  }

  function closeProject(entry: CachedProject): boolean {
    try {
      entry.graph.close();
      entry.closeError = undefined;
      cache.delete(projectKey(entry.descriptor));
      return true;
    } catch (error) {
      // Keep the entry visible to drain ACK logic. Removing it from the cache
      // before a successful close would let generation GC race a live Windows
      // SQLite/WAL handle.
      entry.closeError = error instanceof Error ? error.message : String(error);
      return false;
    }
  }

  function flushDrainAcks(): void {
    for (const [drainId, target] of pendingDrainAcks) {
      const matching = [...cache.values()].filter((entry) => (
        entry.descriptor.projectId === target.projectId
        && entry.descriptor.generationId === target.generationId
      ));
      if (matching.length > 0) {
        const failed = matching.find((entry) => (
          entry.retired
          && entry.references === 0
          && entry.closeError !== undefined
        ));
        if (!failed) continue;
        pendingDrainAcks.delete(drainId);
        port.postMessage({
          type: 'drained',
          drainId,
          ok: false,
          error:
            `failed to close CodeGraph generation `
            + `${target.projectId}/${target.generationId}: ${failed.closeError}`,
        });
        continue;
      }
      pendingDrainAcks.delete(drainId);
      port.postMessage({ type: 'drained', drainId, ok: true });
    }
  }

  function evictToFit(): void {
    while (cache.size >= MAX_CACHED_PROJECTS) {
      const candidate = [...cache.values()]
        .filter((entry) => entry.references === 0)
        .sort((a, b) => {
          if (a.retired !== b.retired) return a.retired ? -1 : 1;
          return a.lastUsed - b.lastUsed;
        })[0];
      if (!candidate) return;
      if (!closeProject(candidate)) return;
    }
  }

  function retireWhere(predicate: (entry: CachedProject) => boolean): void {
    for (const entry of [...cache.values()]) {
      if (!predicate(entry)) continue;
      entry.retired = true;
      if (entry.references === 0) closeProject(entry);
    }
  }

  function applyCatalog(message: CatalogMessage): void {
    if (message.revision < catalogRevision) return;
    catalogRevision = message.revision;
    const previouslyActive = new Set(activeCatalog.keys());
    activeCatalog.clear();
    for (const project of message.projects) {
      activeCatalog.set(project.projectId, project);
      invalidatedProjects.delete(project.projectId);
      invalidatedGenerations.delete(projectKey(project));
      previouslyActive.delete(project.projectId);
    }
    for (const removedProjectId of previouslyActive) {
      invalidatedProjects.add(removedProjectId);
    }
    retireWhere((entry) => {
      const active = activeCatalog.get(entry.descriptor.projectId);
      return !active || !sameDescriptor(active, entry.descriptor);
    });
  }

  function applyPromotion(message: PromoteMessage): void {
    if (message.revision < catalogRevision) return;
    catalogRevision = message.revision;
    activeCatalog.set(message.project.projectId, message.project);
    invalidatedProjects.delete(message.project.projectId);
    invalidatedGenerations.delete(projectKey(message.project));
    retireWhere((entry) => (
      entry.descriptor.projectId === message.project.projectId &&
      !sameDescriptor(entry.descriptor, message.project)
    ));
  }

  function invalidate(projectId: string, generationId?: string): void {
    const active = activeCatalog.get(projectId);
    if (!generationId || active?.generationId === generationId) {
      activeCatalog.delete(projectId);
      invalidatedProjects.add(projectId);
    }
    if (generationId) {
      invalidatedGenerations.add(`${projectId}\u0000${generationId}`);
    }
    retireWhere((entry) => (
      entry.descriptor.projectId === projectId &&
      (!generationId || entry.descriptor.generationId === generationId)
    ));
  }
}

function errorResult(text: string): ToolResult {
  return { isError: true, content: [{ type: 'text', text }] };
}
