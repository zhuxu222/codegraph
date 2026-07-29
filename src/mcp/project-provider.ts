import type CodeGraph from '../index';
import type { ProjectLocation } from '../project/storage/location';
import type { ToolDefinition, ToolResult } from './tools';

/**
 * A concrete, generation-aware project selected for one MCP tool call.
 *
 * `projectPath` remains a source-tree path at the protocol boundary. Providers
 * translate it to this handle so tools never need to know where an external
 * index is stored.
 */
export interface ProjectHandle {
  projectId: string;
  generationId: string;
  location: ProjectLocation;
  graph: CodeGraph;
}

export type ProjectRuntimeState =
  | 'uninitialized'
  | 'building'
  | 'ready'
  | 'syncing'
  | 'rebuilding'
  | 'degraded'
  | 'failed';

/** Freshness information surfaced by workspace-aware status and notices. */
export interface ProjectFreshness {
  state: ProjectRuntimeState;
  lastSuccessfulSyncAt: number | null;
  pendingFiles: number;
  degradedReason: string | null;
  /**
   * True when a freshness gate timed out or the provider otherwise knows that
   * the selected generation may lag behind the source tree.
   */
  stale: boolean;
}

/**
 * Stable extension point for workspace/multi-project MCP hosts.
 *
 * The provider owns project discovery, generation selection, catch-up, watch
 * lifecycle, and connection ownership. ToolHandler owns only CodeGraph tool
 * validation/dispatch and reads the prepared handle synchronously after the
 * asynchronous `prepare` boundary.
 */
export interface MCPProjectProvider {
  /** Resolve and make the target project query-ready. */
  prepare(projectPath?: string): Promise<ProjectHandle>;

  /** Return the handle prepared for this path, or throw an actionable error. */
  getPrepared(projectPath?: string): ProjectHandle;

  /** Runtime freshness for notices and status. */
  getFreshness(projectPath?: string): ProjectFreshness;

  /** Seed or replace the session default inferred from an MCP root. */
  setDefaultProjectPath?(projectPath: string): void;

  /** Whether a path-less tool call can resolve to a registered default. */
  hasDefaultProject?(): boolean;

  /** Optional workspace-owned, read-only tools such as codegraph_projects. */
  getAdditionalTools?(): ToolDefinition[];

  /**
   * Execute an additional tool. Return undefined when the name is not owned by
   * this provider so ToolHandler can continue normal dispatch/error handling.
   */
  executeAdditionalTool?(
    toolName: string,
    args: Record<string, unknown>,
  ): Promise<ToolResult | undefined>;

  /**
   * Release a whole-tool-call handle returned by prepare(). Providers that
   * atomically promote generations use this to keep the old connection alive
   * until every graph method in the call has completed.
   */
  release?(handle: ProjectHandle): void;

  /** Close every watcher/connection owned by the provider. */
  close(): Promise<void>;
}
