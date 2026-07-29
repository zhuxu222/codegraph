import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  CodeGraph,
  CODEGRAPH_LOCATION_MARKER,
  getCodeGraphDir,
  getDatabasePath,
  type ProjectInput,
  type ProjectLocation,
} from '../../../src';

describe('external CodeGraph data directories', () => {
  const originalCodeGraphDir = process.env.CODEGRAPH_DIR;
  let sandbox: string;
  let projectRoot: string;
  let dataDir: string;
  let instances: CodeGraph[];

  beforeEach(() => {
    delete process.env.CODEGRAPH_DIR;
    sandbox = fs.mkdtempSync(path.join(os.tmpdir(), 'codegraph-external-storage-'));
    projectRoot = path.join(sandbox, 'source');
    dataDir = path.join(sandbox, 'workspace-state', 'generation-1', '.codegraph');
    fs.mkdirSync(projectRoot, { recursive: true });
    instances = [];
  });

  afterEach(() => {
    for (const instance of instances.reverse()) {
      try {
        instance.close();
      } catch {
        // A test may already have uninitialized the instance.
      }
    }
    if (originalCodeGraphDir === undefined) delete process.env.CODEGRAPH_DIR;
    else process.env.CODEGRAPH_DIR = originalCodeGraphDir;
    fs.rmSync(sandbox, { recursive: true, force: true });
  });

  function track(instance: CodeGraph): CodeGraph {
    instances.push(instance);
    return instance;
  }

  function location(): ProjectLocation {
    return { projectRoot, dataDir };
  }

  it('initializes and opens with source and generated state in separate roots', () => {
    const input: ProjectInput = location();
    expect(getCodeGraphDir(input)).toBe(path.resolve(dataDir));
    expect(getDatabasePath(input)).toBe(path.join(path.resolve(dataDir), 'codegraph.db'));
    expect(CodeGraph.isInitialized(input)).toBe(false);

    const created = track(CodeGraph.initSync(input));
    expect(created.getProjectRoot()).toBe(path.resolve(projectRoot));
    expect(created.getDataDir()).toBe(path.resolve(dataDir));
    expect(CodeGraph.isInitialized(input)).toBe(true);
    expect(fs.existsSync(getDatabasePath(input))).toBe(true);
    expect(fs.existsSync(path.join(dataDir, '.gitignore'))).toBe(true);
    expect(fs.existsSync(path.join(dataDir, CODEGRAPH_LOCATION_MARKER))).toBe(true);
    expect(fs.existsSync(path.join(projectRoot, '.codegraph'))).toBe(false);

    created.close();
    const reopened = track(CodeGraph.openSync(input));
    expect(reopened.getProjectRoot()).toBe(path.resolve(projectRoot));
    expect(reopened.getDataDir()).toBe(path.resolve(dataDir));
  });

  it('supports external storage across async init, open, and recreate', async () => {
    const input = location();
    fs.writeFileSync(
      path.join(projectRoot, 'first.ts'),
      'export function indexedFromSourceRoot() { return 1; }\n'
    );
    const created = track(await CodeGraph.init(input, { index: true }));
    expect(created.searchNodes('indexedFromSourceRoot').length).toBeGreaterThan(0);
    created.close();

    fs.writeFileSync(
      path.join(projectRoot, 'second.ts'),
      'export function syncedFromSourceRoot() { return 2; }\n'
    );
    const opened = track(await CodeGraph.open(input, { sync: true }));
    expect(opened.getDataDir()).toBe(path.resolve(dataDir));
    expect(opened.searchNodes('syncedFromSourceRoot').length).toBeGreaterThan(0);
    opened.close();

    const recreated = track(await CodeGraph.recreate(input));
    expect(recreated.getProjectRoot()).toBe(path.resolve(projectRoot));
    expect(recreated.getDataDir()).toBe(path.resolve(dataDir));
    expect(recreated.getStats()).toMatchObject({
      nodeCount: 0,
      edgeCount: 0,
      fileCount: 0,
    });
    expect(fs.existsSync(path.join(projectRoot, '.codegraph'))).toBe(false);
  });

  it('uninitializes only the instance data directory', () => {
    const sourceSentinel = path.join(projectRoot, 'keep.ts');
    const embeddedSentinel = path.join(projectRoot, '.codegraph', 'keep.txt');
    fs.writeFileSync(sourceSentinel, 'export const keep = true;\n');
    fs.mkdirSync(path.dirname(embeddedSentinel), { recursive: true });
    fs.writeFileSync(embeddedSentinel, 'not managed by this instance\n');

    const instance = track(CodeGraph.initSync(location()));
    process.env.CODEGRAPH_DIR = '.codegraph-changed-after-init';
    instance.uninitialize();

    expect(fs.existsSync(dataDir)).toBe(false);
    expect(fs.existsSync(projectRoot)).toBe(true);
    expect(fs.readFileSync(sourceSentinel, 'utf8')).toContain('keep');
    expect(fs.readFileSync(embeddedSentinel, 'utf8')).toContain('not managed');
    expect(fs.existsSync(path.join(projectRoot, '.codegraph-changed-after-init'))).toBe(false);
  });

  it('keeps legacy CODEGRAPH_DIR behavior when dataDir is omitted', () => {
    process.env.CODEGRAPH_DIR = '.codegraph-win';
    const input: ProjectLocation = { projectRoot };
    const instance = track(CodeGraph.initSync(input));
    const expected = path.join(path.resolve(projectRoot), '.codegraph-win');

    expect(instance.getDataDir()).toBe(expected);
    expect(getCodeGraphDir(input)).toBe(path.join(projectRoot, '.codegraph-win'));
    expect(fs.existsSync(path.join(expected, 'codegraph.db'))).toBe(true);
    expect(fs.existsSync(path.join(projectRoot, '.codegraph'))).toBe(false);
  });

  it('lets an explicit dataDir take precedence over CODEGRAPH_DIR', () => {
    process.env.CODEGRAPH_DIR = '.codegraph-win';
    const input = location();
    const instance = track(CodeGraph.initSync(input));

    expect(instance.getDataDir()).toBe(path.resolve(dataDir));
    expect(fs.existsSync(path.join(dataDir, 'codegraph.db'))).toBe(true);
    expect(fs.existsSync(path.join(projectRoot, '.codegraph-win'))).toBe(false);
  });

  it('refuses an external dataDir that could delete the source tree', () => {
    const sourceSentinel = path.join(projectRoot, 'keep.ts');
    fs.writeFileSync(sourceSentinel, 'export const keep = true;\n');

    expect(() =>
      CodeGraph.initSync({ projectRoot, dataDir: projectRoot })
    ).toThrow(/must not be the projectRoot/);
    expect(() =>
      CodeGraph.initSync({ projectRoot, dataDir: sandbox })
    ).toThrow(/parent directories/);
    expect(fs.readFileSync(sourceSentinel, 'utf8')).toContain('keep');
  });

  it('refuses external storage routed through a symlink or junction', () => {
    const actualStorage = path.join(sandbox, 'actual-storage');
    const linkedStorage = path.join(sandbox, 'linked-storage');
    fs.mkdirSync(actualStorage, { recursive: true });
    fs.symlinkSync(
      actualStorage,
      linkedStorage,
      process.platform === 'win32' ? 'junction' : 'dir',
    );

    expect(() => CodeGraph.initSync({
      projectRoot,
      dataDir: path.join(linkedStorage, 'generation-1', '.codegraph'),
    })).toThrow(/symlink or junction/i);
    expect(fs.readdirSync(actualStorage)).toEqual([]);
  });

  it('refuses to claim a non-empty unmarked external directory', () => {
    const sentinel = path.join(dataDir, 'keep.txt');
    fs.mkdirSync(dataDir, { recursive: true });
    fs.writeFileSync(sentinel, 'belongs to another application\n');

    expect(() => CodeGraph.initSync(location())).toThrow(
      /non-empty unmarked external CodeGraph data/i,
    );
    expect(fs.readFileSync(sentinel, 'utf8')).toContain('another application');
    expect(fs.existsSync(path.join(dataDir, 'codegraph.db'))).toBe(false);
  });

  it('refuses a regular file used as an external data directory', () => {
    fs.mkdirSync(path.dirname(dataDir), { recursive: true });
    fs.writeFileSync(dataDir, 'do not remove\n');

    expect(() => CodeGraph.initSync(location())).toThrow(
      /must be a real directory/i,
    );
    expect(fs.readFileSync(dataDir, 'utf8')).toContain('do not remove');
  });

  it('refuses to delete unmarked or mismatched external storage', () => {
    const instance = track(CodeGraph.initSync(location()));
    const markerPath = path.join(dataDir, CODEGRAPH_LOCATION_MARKER);
    fs.unlinkSync(markerPath);

    expect(() => instance.uninitialize()).toThrow(/unmarked external/i);
    expect(fs.existsSync(dataDir)).toBe(true);

    fs.writeFileSync(markerPath, JSON.stringify({
      schemaVersion: 1,
      projectRoot,
      dataDir: path.join(sandbox, 'different-index'),
    }));
    expect(() => instance.uninitialize()).toThrow(/mismatched ownership marker/i);
    expect(fs.existsSync(dataDir)).toBe(true);
  });
});
