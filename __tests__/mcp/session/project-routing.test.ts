import * as os from 'node:os';
import * as path from 'node:path';
import { pathToFileURL } from 'node:url';
import { afterEach, describe, expect, it } from 'vitest';
import {
  MCPEngine,
  MCPSession,
  type JsonRpcTransport,
  type MCPProjectProvider,
  type ProjectFreshness,
  type ProjectHandle,
  type ToolDefinition,
  type ToolResult,
} from '../../../src';

type MessageHandler = Parameters<JsonRpcTransport['start']>[0];
type IncomingMessage = Parameters<MessageHandler>[0];
type RpcResponse = Parameters<JsonRpcTransport['send']>[0];

class MemoryTransport implements JsonRpcTransport {
  private handler: MessageHandler | null = null;
  readonly responses: RpcResponse[] = [];

  constructor(private readonly rootsResult?: unknown) {}

  start(handler: MessageHandler): void {
    this.handler = handler;
  }

  stop(): void {
    this.handler = null;
  }

  async receive(message: IncomingMessage): Promise<void> {
    if (this.handler === null) {
      throw new Error('Transport has not been started.');
    }
    await this.handler(message);
  }

  send(response: RpcResponse): void {
    this.responses.push(response);
  }

  notify(_method: string, _params?: unknown): void {}

  async request(
    method: string,
    _params?: unknown,
    _timeoutMs?: number,
  ): Promise<unknown> {
    if (method === 'roots/list' && this.rootsResult !== undefined) {
      return this.rootsResult;
    }
    throw new Error('No server-initiated request expected in this test.');
  }

  sendResult(id: string | number, result: unknown): void {
    this.send({ jsonrpc: '2.0', id, result });
  }

  sendError(
    id: string | number | null,
    code: number,
    message: string,
    data?: unknown,
  ): void {
    this.send({
      jsonrpc: '2.0',
      id,
      error: { code, message, data },
    });
  }

  result(id: string | number): unknown {
    const response = this.responses.find(
      (candidate) => candidate.id === id,
    );
    if (response === undefined || !('result' in response)) {
      throw new Error(`No successful response for request ${String(id)}.`);
    }
    return response.result;
  }
}

class RoutingProbeProvider implements MCPProjectProvider {
  defaultProjectPath: string | null = null;
  readonly calls: Array<Record<string, unknown>> = [];

  async prepare(): Promise<ProjectHandle> {
    throw new Error('Routing probe must be handled as a provider-owned tool.');
  }

  getPrepared(): ProjectHandle {
    throw new Error('Routing probe must be handled as a provider-owned tool.');
  }

  getFreshness(): ProjectFreshness {
    return {
      state: 'ready',
      lastSuccessfulSyncAt: Date.now(),
      pendingFiles: 0,
      degradedReason: null,
      stale: false,
    };
  }

  setDefaultProjectPath(projectPath: string): void {
    this.defaultProjectPath = projectPath;
  }

  hasDefaultProject(): boolean {
    return this.defaultProjectPath !== null;
  }

  getAdditionalTools(): ToolDefinition[] {
    return [{
      name: 'codegraph_routing_probe',
      description: 'Return the projectPath received by the shared handler.',
      inputSchema: {
        type: 'object',
        properties: {
          projectPath: {
            type: 'string',
            description: 'Project source path.',
          },
        },
      },
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: false,
      },
    }];
  }

  async executeAdditionalTool(
    toolName: string,
    args: Record<string, unknown>,
  ): Promise<ToolResult | undefined> {
    if (toolName !== 'codegraph_routing_probe') {
      return undefined;
    }
    this.calls.push({ ...args });
    return {
      content: [{
        type: 'text',
        text: String(args.projectPath),
      }],
    };
  }

  async close(): Promise<void> {}
}

describe('MCPSession project routing with a shared engine', () => {
  const engines: MCPEngine[] = [];

  afterEach(() => {
    for (const engine of engines.splice(0)) {
      engine.stop();
    }
  });

  it('keeps omitted projectPath session-local and lets an explicit path win', async () => {
    const provider = new RoutingProbeProvider();
    const engine = new MCPEngine({
      watch: false,
      queryPool: false,
      projectProvider: provider,
    });
    engines.push(engine);

    const alphaPath = path.resolve(
      os.tmpdir(),
      'codegraph-session-alpha',
    );
    const betaPath = path.resolve(
      os.tmpdir(),
      'codegraph-session-beta',
    );
    const explicitPath = path.resolve(
      os.tmpdir(),
      'codegraph-session-explicit',
    );
    const alphaTransport = new MemoryTransport();
    const betaTransport = new MemoryTransport();
    const alphaSession = new MCPSession(alphaTransport, engine);
    const betaSession = new MCPSession(betaTransport, engine);
    alphaSession.start();
    betaSession.start();

    await alphaTransport.receive({
      jsonrpc: '2.0',
      id: 'alpha-init',
      method: 'initialize',
      params: { rootUri: pathToFileURL(alphaPath).href },
    });
    await betaTransport.receive({
      jsonrpc: '2.0',
      id: 'beta-init',
      method: 'initialize',
      params: { rootUri: pathToFileURL(betaPath).href },
    });

    // The shared provider default was overwritten by the second initialize.
    // Per-session injection must still route alpha back to alpha.
    expect(provider.defaultProjectPath).toBe(betaPath);

    await alphaTransport.receive({
      jsonrpc: '2.0',
      id: 'alpha-call',
      method: 'tools/call',
      params: {
        name: 'codegraph_routing_probe',
        arguments: {},
      },
    });
    await betaTransport.receive({
      jsonrpc: '2.0',
      id: 'beta-call',
      method: 'tools/call',
      params: {
        name: 'codegraph_routing_probe',
        arguments: {},
      },
    });
    await alphaTransport.receive({
      jsonrpc: '2.0',
      id: 'explicit-call',
      method: 'tools/call',
      params: {
        name: 'codegraph_routing_probe',
        arguments: { projectPath: explicitPath },
      },
    });

    expect(toolText(alphaTransport.result('alpha-call'))).toBe(alphaPath);
    expect(toolText(betaTransport.result('beta-call'))).toBe(betaPath);
    expect(toolText(alphaTransport.result('explicit-call'))).toBe(
      explicitPath,
    );
    expect(provider.calls.map((call) => call.projectPath)).toEqual([
      alphaPath,
      betaPath,
      explicitPath,
    ]);

    alphaSession.stop();
    betaSession.stop();
  });

  it('injects a root discovered during the first tools/call roots/list gate', async () => {
    const provider = new RoutingProbeProvider();
    const engine = new MCPEngine({
      watch: false,
      queryPool: false,
      projectProvider: provider,
    });
    engines.push(engine);

    const rootsPath = path.resolve(os.tmpdir(), 'codegraph-session-roots-list');
    const transport = new MemoryTransport({
      roots: [{ uri: pathToFileURL(rootsPath).href, name: 'roots-project' }],
    });
    const session = new MCPSession(transport, engine);
    session.start();

    await transport.receive({
      jsonrpc: '2.0',
      id: 'init',
      method: 'initialize',
      params: { capabilities: { roots: {} } },
    });
    await transport.receive({
      jsonrpc: '2.0',
      id: 'first-call',
      method: 'tools/call',
      params: {
        name: 'codegraph_routing_probe',
        arguments: {},
      },
    });

    expect(toolText(transport.result('first-call'))).toBe(rootsPath);
    session.stop();
  });
});

function toolText(result: unknown): string {
  const toolResult = result as ToolResult;
  return toolResult.content[0]?.text ?? '';
}
