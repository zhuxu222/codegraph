import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  CodeGraph,
  ToolHandler,
  type MCPProjectProvider,
  type ProjectFreshness,
  type ProjectHandle,
  type ToolDefinition,
  type ToolResult,
} from '../../src';

class TestWorkspaceProvider implements MCPProjectProvider {
  private readonly handles = new Map<string, ProjectHandle>();
  private readonly ownedHandles = new Set<ProjectHandle>();
  private prepared: ProjectHandle | null = null;
  private defaultPath: string | null = null;
  private replacementAfterPrepare: ProjectHandle | null = null;
  nextPrepareErrorCode: 'PROJECT_NOT_INDEXED' | null = null;
  readonly released: ProjectHandle[] = [];
  freshness: ProjectFreshness = {
    state: 'ready',
    lastSuccessfulSyncAt: Date.now(),
    pendingFiles: 0,
    degradedReason: null,
    stale: false,
  };

  add(handle: ProjectHandle): void {
    this.handles.set(path.resolve(handle.location.projectRoot), handle);
    this.ownedHandles.add(handle);
  }

  replaceAfterNextPrepare(handle: ProjectHandle): void {
    this.replacementAfterPrepare = handle;
    this.ownedHandles.add(handle);
  }

  setDefaultProjectPath(projectPath: string): void {
    this.defaultPath = projectPath;
  }

  hasDefaultProject(): boolean {
    return this.defaultPath !== null;
  }

  async prepare(projectPath?: string): Promise<ProjectHandle> {
    if (this.nextPrepareErrorCode) {
      const code = this.nextPrepareErrorCode;
      this.nextPrepareErrorCode = null;
      throw providerError(code, `${code}: index is unavailable`);
    }
    this.prepared = this.resolve(projectPath);
    if (this.replacementAfterPrepare) {
      const replacement = this.replacementAfterPrepare;
      this.replacementAfterPrepare = null;
      this.handles.set(
        path.resolve(replacement.location.projectRoot),
        replacement,
      );
    }
    return this.prepared;
  }

  getPrepared(projectPath?: string): ProjectHandle {
    if (projectPath !== undefined) return this.resolve(projectPath);
    if (this.prepared) return this.prepared;
    return this.resolve(undefined);
  }

  getFreshness(): ProjectFreshness {
    return this.freshness;
  }

  getAdditionalTools(): ToolDefinition[] {
    return [{
      name: 'codegraph_projects',
      description: 'List registered projects.',
      inputSchema: { type: 'object', properties: {} },
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: false,
      },
    }];
  }

  async executeAdditionalTool(toolName: string): Promise<ToolResult | undefined> {
    if (toolName !== 'codegraph_projects') return undefined;
    return {
      content: [{
        type: 'text',
        text: JSON.stringify([...this.handles.values()].map((handle) => handle.projectId)),
      }],
    };
  }

  release(handle: ProjectHandle): void {
    this.released.push(handle);
  }

  async close(): Promise<void> {
    for (const handle of this.ownedHandles) handle.graph.close();
    this.handles.clear();
    this.ownedHandles.clear();
  }

  private resolve(projectPath?: string): ProjectHandle {
    const candidate = path.resolve(projectPath ?? this.defaultPath ?? '');
    const matches = [...this.handles.entries()]
      .filter(([root]) => candidate === root || candidate.startsWith(`${root}${path.sep}`))
      .sort(([left], [right]) => right.length - left.length);
    const handle = matches[0]?.[1];
    if (!handle) {
      throw providerError(
        'PROJECT_NOT_REGISTERED',
        `PROJECT_NOT_REGISTERED: ${candidate}`,
      );
    }
    return handle;
  }
}

function providerError(code: string, message: string): Error & { code: string } {
  return Object.assign(new Error(message), { code });
}

describe('MCPProjectProvider integration', () => {
  let tempDir: string;
  let provider: TestWorkspaceProvider;
  let handler: ToolHandler;

  beforeEach(async () => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'codegraph-provider-'));
    provider = new TestWorkspaceProvider();

    for (const [id, symbol] of [
      ['alpha', 'alphaOnlySymbol'],
      ['beta', 'betaOnlySymbol'],
    ] as const) {
      const projectRoot = path.join(tempDir, 'sources', id);
      const dataDir = path.join(tempDir, 'indexes', id, '.codegraph');
      fs.mkdirSync(projectRoot, { recursive: true });
      fs.writeFileSync(
        path.join(projectRoot, `${id}.ts`),
        `export function ${symbol}(): string { return '${id}'; }\n`,
      );
      const graph = await CodeGraph.init({ projectRoot, dataDir }, { index: true });
      provider.add({
        projectId: id,
        generationId: 'generation-1',
        location: { projectRoot, dataDir },
        graph,
      });
    }

    provider.setDefaultProjectPath(path.join(tempDir, 'sources', 'alpha'));
    handler = new ToolHandler(null, provider);
  });

  afterEach(async () => {
    await provider.close();
    fs.rmSync(tempDir, { recursive: true, force: true });
  });

  it('prepares and routes an explicit source path to the matching external index', async () => {
    const betaRoot = path.join(tempDir, 'sources', 'beta');
    const result = await handler.execute('codegraph_explore', {
      query: 'betaOnlySymbol',
      projectPath: path.join(betaRoot, 'beta.ts'),
    });

    expect(result.isError).toBeUndefined();
    expect(result.content[0]?.text).toContain('betaOnlySymbol');
    expect(result.content[0]?.text).not.toContain('alphaOnlySymbol');
    expect(provider.getPrepared(betaRoot).location.dataDir).toContain(
      path.join('indexes', 'beta'),
    );
  });

  it('exposes provider-owned read-only tools without requiring a project path', async () => {
    provider.setDefaultProjectPath(path.join(tempDir, 'sources', 'alpha'));
    const projects = handler.getTools().find((tool) => tool.name === 'codegraph_projects');
    expect(projects?.annotations?.readOnlyHint).toBe(true);
    expect(projects?.inputSchema.required ?? []).not.toContain('projectPath');

    const result = await handler.execute('codegraph_projects', {});
    expect(result.content[0]?.text).toContain('alpha');
    expect(result.content[0]?.text).toContain('beta');
  });

  it('marks successful results stale when the selected runtime misses its freshness gate', async () => {
    provider.freshness = {
      ...provider.freshness,
      state: 'syncing',
      pendingFiles: 1,
      stale: true,
    };

    const result = await handler.execute('codegraph_explore', {
      query: 'alphaOnlySymbol',
    });
    expect(result.isError).toBeUndefined();
    expect(result.content[0]?.text).toMatch(/may be stale/i);
  });

  it('returns an actionable error instead of falling through to another project', async () => {
    const result = await handler.execute('codegraph_explore', {
      query: 'anything',
      projectPath: path.join(tempDir, 'unknown'),
    });
    expect(result.isError).toBeUndefined();
    expect(result.content[0]?.text).toContain('PROJECT_NOT_REGISTERED');
  });

  it('returns PROJECT_NOT_INDEXED as success-shaped fallback guidance', async () => {
    provider.nextPrepareErrorCode = 'PROJECT_NOT_INDEXED';
    const result = await handler.execute('codegraph_explore', {
      query: 'anything',
    });

    expect(result.isError).toBeUndefined();
    expect(result.content[0]?.text).toContain('PROJECT_NOT_INDEXED');
    expect(provider.released).toHaveLength(0);
  });

  it('never exposes an external generation path in provider tool errors', async () => {
    const alphaRoot = path.join(tempDir, 'sources', 'alpha');
    const handle = provider.getPrepared(alphaRoot);
    const privateDataDir = handle.location.dataDir!;
    (handle.graph as CodeGraph & {
      searchNodes: CodeGraph['searchNodes'];
    }).searchNodes = (() => {
      throw new Error(`SQLite open failed at ${privateDataDir}`);
    }) as CodeGraph['searchNodes'];

    const result = await handler.execute('codegraph_search', {
      query: 'alphaOnlySymbol',
      projectPath: alphaRoot,
    });

    expect(result.isError).toBe(true);
    expect(result.content[0]?.text).toMatch(/workspace-managed index storage/i);
    expect(result.content[0]?.text).not.toContain(privateDataDir);
  });

  it('pins the prepared generation for the whole tool call and releases it once', async () => {
    const alphaRoot = path.join(tempDir, 'sources', 'alpha');
    const alphaFile = path.join(alphaRoot, 'alpha.ts');
    const oldHandle = provider.getPrepared(alphaRoot);
    const nextDataDir = path.join(
      tempDir,
      'indexes',
      'alpha-generation-2',
      '.codegraph',
    );

    fs.writeFileSync(
      alphaFile,
      "export function alphaNextGenerationSymbol(): string { return 'next'; }\n",
    );
    const nextGraph = await CodeGraph.init(
      { projectRoot: alphaRoot, dataDir: nextDataDir },
      { index: true },
    );
    fs.writeFileSync(
      alphaFile,
      "export function alphaOnlySymbol(): string { return 'alpha'; }\n",
    );
    const nextHandle: ProjectHandle = {
      projectId: 'alpha',
      generationId: 'generation-2',
      location: { projectRoot: alphaRoot, dataDir: nextDataDir },
      graph: nextGraph,
    };
    provider.replaceAfterNextPrepare(nextHandle);

    const result = await handler.execute('codegraph_explore', {
      query: 'alphaOnlySymbol',
      projectPath: alphaRoot,
    });

    expect(result.isError).toBeUndefined();
    expect(result.content[0]?.text).toContain('alphaOnlySymbol');
    expect(provider.getPrepared(alphaRoot).generationId).toBe('generation-2');
    expect(provider.released).toEqual([oldHandle]);
  });
});
