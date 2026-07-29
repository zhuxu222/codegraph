/**
 * Source and storage locations for a CodeGraph project.
 *
 * `projectRoot` is always the source tree CodeGraph reads and watches.
 * `dataDir`, when supplied, is the directory that owns generated CodeGraph
 * state (database, lock, logs, and other runtime files).
 */
export interface ProjectLocation {
  projectRoot: string;
  dataDir?: string;
}

/**
 * Backwards-compatible project input accepted by the public lifecycle API.
 *
 * A string keeps the legacy per-project `.codegraph`/`CODEGRAPH_DIR` behavior;
 * a location object may place generated state outside the source tree.
 */
export type ProjectInput = string | ProjectLocation;

/** Canonical, absolute locations retained by a live CodeGraph instance. */
export interface ResolvedProjectLocation {
  projectRoot: string;
  dataDir: string;
}
