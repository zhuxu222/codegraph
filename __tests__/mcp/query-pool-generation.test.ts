import * as path from 'path';
import { describe, expect, it } from 'vitest';
import {
  QueryPool,
  type PoolWorker,
  type QueryProjectDescriptor,
} from '../../src/mcp/query-pool';
import type { ToolResult } from '../../src/mcp/tools';

interface CallMessage {
  type: 'call';
  id: number;
  toolName: string;
  args: Record<string, unknown>;
  project: QueryProjectDescriptor;
  pinnedGeneration: boolean;
  catalogRevision: number;
}

interface DrainMessage {
  type: 'drain';
  drainId: number;
  projectId: string;
  generationId: string;
}

type WorkerMessage = CallMessage | DrainMessage | {
  type: string;
  [key: string]: unknown;
};

const ok = (text: string): ToolResult => ({
  content: [{ type: 'text', text }],
});

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

async function waitUntil(
  predicate: () => boolean,
  timeoutMs = 1_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) {
      throw new Error(`condition was not met within ${timeoutMs}ms`);
    }
    await sleep(1);
  }
}

class RecordingWorker implements PoolWorker {
  readonly messages: WorkerMessage[] = [];
  private messageListener?: (message: unknown) => void;
  private exitListener?: (code: number) => void;

  constructor(
    private readonly serve: (message: CallMessage) => Promise<ToolResult> | ToolResult,
    private readonly autoAckDrains = true,
    private readonly terminationGate?: Promise<void>,
  ) {
    setTimeout(() => this.messageListener?.({ type: 'ready', ok: true }), 0);
  }

  on(event: 'message' | 'error' | 'exit', callback: (...args: any[]) => void): void {
    if (event === 'message') this.messageListener = callback;
    if (event === 'exit') this.exitListener = callback;
  }

  postMessage(message: unknown): void {
    const wire = message as WorkerMessage;
    this.messages.push(wire);
    if (wire.type === 'drain') {
      if (this.autoAckDrains) {
        const drain = wire as DrainMessage;
        queueMicrotask(() => this.ackDrain(drain.drainId));
      }
      return;
    }
    if (wire.type !== 'call') return;
    void Promise.resolve(this.serve(wire)).then((result) => {
      this.messageListener?.({ type: 'result', id: wire.id, result });
    });
  }

  ackDrain(drainId: number, ok = true, error?: string): void {
    this.messageListener?.({ type: 'drained', drainId, ok, error });
  }

  async terminate(): Promise<number> {
    await this.terminationGate;
    this.exitListener = undefined;
    return 0;
  }
}

function descriptor(
  projectId: string,
  generationId: string,
  projectRoot = path.resolve(projectId),
): QueryProjectDescriptor {
  return {
    projectId,
    generationId,
    projectRoot,
    dataDir: path.join(projectRoot, 'external', generationId, '.codegraph'),
  };
}

describe('QueryPool generation routing', () => {
  it('puts the complete external generation descriptor on every worker call', async () => {
    const alpha = descriptor('alpha', 'generation-1');
    let worker!: RecordingWorker;
    worker = new RecordingWorker((message) => ok(JSON.stringify(message.project)));
    const pool = new QueryPool({
      root: path.resolve('.'),
      size: 1,
      catalog: {
        revision: 7,
        projects: [alpha],
        defaultProjectId: alpha.projectId,
      },
      createWorker: () => worker,
    });

    const result = await pool.run('codegraph_explore', { query: 'q' });
    expect(JSON.parse(result.content[0].text)).toEqual(alpha);
    const call = worker.messages.find((message): message is CallMessage => message.type === 'call');
    expect(call?.catalogRevision).toBe(7);
    expect(call?.project).toEqual(alpha);
    await pool.destroy();
  });

  it('keeps an in-flight old generation but retargets queued/new calls on promotion', async () => {
    const oldGeneration = descriptor('alpha', 'generation-1');
    const newGeneration = descriptor('alpha', 'generation-2');
    let releaseFirst!: () => void;
    const firstGate = new Promise<void>((resolve) => { releaseFirst = resolve; });
    let callCount = 0;
    let worker!: RecordingWorker;
    worker = new RecordingWorker(async (message) => {
      callCount++;
      if (callCount === 1) await firstGate;
      return ok(message.project.generationId);
    });
    const pool = new QueryPool({
      root: path.resolve('.'),
      size: 1,
      catalog: {
        revision: 1,
        projects: [oldGeneration],
        defaultProjectId: 'alpha',
      },
      createWorker: () => worker,
    });

    const first = pool.run('codegraph_explore', { query: 'first' });
    while (!worker.messages.some((message) => message.type === 'call')) await sleep(1);
    const queued = pool.run('codegraph_explore', { query: 'queued' });
    pool.promote(newGeneration, 2);
    const afterPromotion = pool.run('codegraph_explore', { query: 'new' });
    releaseFirst();

    expect((await first).content[0].text).toBe('generation-1');
    expect((await queued).content[0].text).toBe('generation-2');
    expect((await afterPromotion).content[0].text).toBe('generation-2');
    const calls = worker.messages.filter(
      (message): message is CallMessage => message.type === 'call',
    );
    expect(calls.map((message) => message.project.generationId)).toEqual([
      'generation-1',
      'generation-2',
      'generation-2',
    ]);
    expect(worker.messages).toContainEqual(expect.objectContaining({
      type: 'promote',
      revision: 2,
      project: newGeneration,
      previousGenerationId: 'generation-1',
    }));
    await pool.destroy();
  });

  it('does not retarget a queued provider-pinned generation after promotion', async () => {
    const oldGeneration = descriptor('alpha', 'generation-1');
    const newGeneration = descriptor('alpha', 'generation-2');
    let releaseFirst!: () => void;
    const firstGate = new Promise<void>((resolve) => { releaseFirst = resolve; });
    let callCount = 0;
    let worker!: RecordingWorker;
    worker = new RecordingWorker(async (message) => {
      callCount++;
      if (callCount === 1) await firstGate;
      return ok(message.project.generationId);
    });
    const pool = new QueryPool({
      root: path.resolve('.'),
      size: 1,
      catalog: {
        revision: 1,
        projects: [oldGeneration],
        defaultProjectId: 'alpha',
      },
      createWorker: () => worker,
    });

    const blocker = pool.run('codegraph_explore', { query: 'blocker' });
    while (!worker.messages.some((message) => message.type === 'call')) await sleep(1);
    const pinned = pool.run(
      'codegraph_explore',
      { query: 'pinned' },
      oldGeneration,
    );
    pool.promote(newGeneration, 2);
    releaseFirst();

    await blocker;
    expect((await pinned).content[0].text).toBe('generation-1');
    const calls = worker.messages.filter(
      (message): message is CallMessage => message.type === 'call',
    );
    expect(calls[1]).toEqual(expect.objectContaining({
      project: oldGeneration,
      pinnedGeneration: true,
    }));
    await pool.destroy();
  });

  it('longest-prefix routes projectPath and never falls back in managed mode', async () => {
    const parent = descriptor('parent', 'p1', path.resolve('repos', 'parent'));
    const child = descriptor('child', 'c1', path.resolve('repos', 'parent', 'child'));
    let worker!: RecordingWorker;
    worker = new RecordingWorker((message) => ok(message.project.projectId));
    const pool = new QueryPool({
      root: path.resolve('.'),
      size: 1,
      catalog: { revision: 1, projects: [parent, child] },
      createWorker: () => worker,
    });

    const nested = await pool.run('codegraph_node', {
      symbol: 'x',
      projectPath: path.join(child.projectRoot, 'src', 'index.ts'),
    });
    expect(nested.content[0].text).toBe('child');

    const noTarget = await pool.run('codegraph_node', { symbol: 'x' });
    expect(noTarget.isError).toBe(true);
    expect(noTarget.content[0].text).toMatch(/registered CodeGraph project/i);

    const unknown = await pool.run('codegraph_node', {
      symbol: 'x',
      projectPath: path.resolve('somewhere-else'),
    });
    expect(unknown.isError).toBe(true);
    await pool.destroy();
  });

  it('ignores stale catalog revisions', async () => {
    const alpha = descriptor('alpha', 'g3');
    const pool = new QueryPool({
      root: path.resolve('.'),
      size: 1,
      catalog: { revision: 3, projects: [alpha], defaultProjectId: 'alpha' },
      createWorker: () => new RecordingWorker(() => ok('ok')),
    });
    pool.setCatalog({
      revision: 2,
      projects: [descriptor('alpha', 'g2')],
      defaultProjectId: 'alpha',
    });
    expect(pool.catalogRevision).toBe(3);
    await pool.destroy();
  });

  it('holds a generation lease through soft timeout and worker drain acknowledgement', async () => {
    const oldGeneration = descriptor('alpha', 'generation-1');
    const newGeneration = descriptor('alpha', 'generation-2');
    let releaseExecution!: () => void;
    const executionGate = new Promise<void>((resolve) => {
      releaseExecution = resolve;
    });
    let worker!: RecordingWorker;
    worker = new RecordingWorker(async (message) => {
      if (message.project.generationId === oldGeneration.generationId) {
        await executionGate;
      }
      return ok(message.project.generationId);
    }, false);

    let acquired = 0;
    let released = 0;
    const pool = new QueryPool({
      root: path.resolve('.'),
      size: 1,
      softTimeoutMs: 20,
      catalog: {
        revision: 1,
        projects: [oldGeneration],
        defaultProjectId: 'alpha',
      },
      acquireGenerationLease: () => {
        acquired++;
        let active = true;
        return {
          release: () => {
            if (!active) return;
            active = false;
            released++;
          },
        };
      },
      createWorker: () => worker,
    });

    const call = pool.run('codegraph_explore', { query: 'slow' });
    await waitUntil(() => worker.messages.some((message) => message.type === 'call'));
    pool.promote(newGeneration, 2);

    const timedOut = await call;
    expect(timedOut.isError).not.toBe(true);
    expect(timedOut.content[0].text).toMatch(/busy/i);
    expect(acquired).toBe(1);
    expect(released).toBe(0);

    let drained = false;
    const drain = pool
      .drainGeneration(oldGeneration.projectId, oldGeneration.generationId)
      .then(() => { drained = true; });
    await sleep(10);
    expect(drained).toBe(false);
    expect(worker.messages.some((message) => message.type === 'drain')).toBe(false);

    releaseExecution();
    await waitUntil(() => released === 1);
    await waitUntil(() => worker.messages.some((message) => message.type === 'drain'));
    expect(drained).toBe(false);

    const drainMessage = worker.messages.find(
      (message): message is DrainMessage => message.type === 'drain',
    );
    expect(drainMessage).toBeDefined();
    worker.ackDrain(drainMessage!.drainId);
    await drain;
    expect(drained).toBe(true);

    const retired = await pool.run(
      'codegraph_explore',
      { query: 'retired' },
      oldGeneration,
    );
    expect(retired.isError).toBe(true);
    expect(acquired).toBe(1);

    // A catalog rollback/reactivation clears the drain tombstone.
    pool.setCatalog({
      revision: 3,
      projects: [oldGeneration],
      defaultProjectId: 'alpha',
    });
    const reactivated = await pool.run('codegraph_explore', { query: 'rollback' });
    expect(reactivated.content[0].text).toBe(oldGeneration.generationId);
    expect(acquired).toBe(2);
    expect(released).toBe(2);
    await pool.destroy();
  });

  it('rejects a drain when a worker reports that its generation handle did not close', async () => {
    const generation = descriptor('alpha', 'generation-1');
    let worker!: RecordingWorker;
    worker = new RecordingWorker(() => ok('unused'), false);
    const pool = new QueryPool({
      root: path.resolve('.'),
      size: 1,
      catalog: {
        revision: 1,
        projects: [generation],
        defaultProjectId: 'alpha',
      },
      createWorker: () => worker,
    });

    const drain = pool.drainGeneration(
      generation.projectId,
      generation.generationId,
    );
    await waitUntil(() => worker.messages.some((message) => message.type === 'drain'));
    const message = worker.messages.find(
      (candidate): candidate is DrainMessage => candidate.type === 'drain',
    )!;
    worker.ackDrain(message.drainId, false, 'database is locked');
    await expect(drain).rejects.toThrow(/database is locked/i);
    await pool.destroy();
  });

  it('cancels an obsolete drain when its generation is reactivated', async () => {
    const oldGeneration = descriptor('alpha', 'generation-1');
    const newGeneration = descriptor('alpha', 'generation-2');
    let releaseExecution!: () => void;
    const executionGate = new Promise<void>((resolve) => {
      releaseExecution = resolve;
    });
    let worker!: RecordingWorker;
    worker = new RecordingWorker(async (message) => {
      if (message.project.generationId === oldGeneration.generationId) {
        await executionGate;
      }
      return ok(message.project.generationId);
    });
    const pool = new QueryPool({
      root: path.resolve('.'),
      size: 1,
      catalog: {
        revision: 1,
        projects: [oldGeneration],
        defaultProjectId: 'alpha',
      },
      createWorker: () => worker,
    });

    const oldCall = pool.run('codegraph_explore', { query: 'slow' });
    await waitUntil(() => worker.messages.some((message) => message.type === 'call'));
    pool.promote(newGeneration, 2);
    const drain = pool.drainGeneration(
      oldGeneration.projectId,
      oldGeneration.generationId,
    );

    pool.setCatalog({
      revision: 3,
      projects: [oldGeneration],
      defaultProjectId: 'alpha',
    });
    await expect(drain).rejects.toThrow(/canceled by catalog reactivation/i);

    const reactivated = pool.run('codegraph_explore', { query: 'reactivated' });
    releaseExecution();
    expect((await oldCall).content[0].text).toBe(oldGeneration.generationId);
    expect((await reactivated).content[0].text).toBe(oldGeneration.generationId);
    await pool.destroy();
  });

  it('keeps generation leases until worker termination completes during destroy', async () => {
    const generation = descriptor('alpha', 'generation-1');
    let releaseTermination!: () => void;
    const terminationGate = new Promise<void>((resolve) => {
      releaseTermination = resolve;
    });
    let worker!: RecordingWorker;
    worker = new RecordingWorker(
      () => new Promise<ToolResult>(() => undefined),
      true,
      terminationGate,
    );
    let released = 0;
    const pool = new QueryPool({
      root: path.resolve('.'),
      size: 1,
      softTimeoutMs: 1_000,
      catalog: {
        revision: 1,
        projects: [generation],
        defaultProjectId: 'alpha',
      },
      acquireGenerationLease: () => ({
        release: () => { released++; },
      }),
      createWorker: () => worker,
    });

    const call = pool.run('codegraph_explore', { query: 'hang' });
    await waitUntil(() => worker.messages.some((message) => message.type === 'call'));
    let destroyed = false;
    const destroying = pool.destroy().then(() => { destroyed = true; });
    const shutdown = await call;
    expect(shutdown.isError).toBe(true);
    expect(released).toBe(0);
    expect(destroyed).toBe(false);

    releaseTermination();
    await destroying;
    expect(released).toBe(1);
    expect(destroyed).toBe(true);
  });
});
