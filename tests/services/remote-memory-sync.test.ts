import { afterEach, beforeEach, describe, expect, it } from 'bun:test';
import { existsSync, rmSync } from 'fs';
import { join } from 'path';
import { tmpdir } from 'os';
import { SessionStore } from '../../src/services/sqlite/SessionStore.js';
import {
  RemoteMemorySync,
  type RemoteMemoryBackend,
  type RemoteMemoryDocument,
} from '../../src/services/sync/RemoteMemorySync.js';

class FakeRemoteMemoryBackend implements RemoteMemoryBackend {
  readonly label = 'fake-remote';
  readonly targetFingerprint = 'fake-target';

  readonly storedDocuments = new Map<string, RemoteMemoryDocument>();

  constructor(seedDocuments: RemoteMemoryDocument[] = []) {
    for (const document of seedDocuments) {
      this.storedDocuments.set(document.id, document);
    }
  }

  async initialize(): Promise<void> {}

  async upsertDocuments(documents: RemoteMemoryDocument[]): Promise<void> {
    for (const document of documents) {
      this.storedDocuments.set(document.id, document);
    }
  }

  async fetchDocumentsUpdatedSince(sinceEpoch: number): Promise<RemoteMemoryDocument[]> {
    return [...this.storedDocuments.values()]
      .filter(document => document.updatedAtEpoch > sinceEpoch)
      .sort((left, right) => left.updatedAtEpoch - right.updatedAtEpoch);
  }
}

describe('RemoteMemorySync', () => {
  let store: SessionStore;
  let statePath: string;

  beforeEach(() => {
    store = new SessionStore(':memory:');
    statePath = join(
      tmpdir(),
      `claude-mem-remote-sync-${Date.now()}-${Math.random().toString(36).slice(2)}.json`
    );
  });

  afterEach(async () => {
    store.close();
    if (existsSync(statePath)) {
      rmSync(statePath, { force: true });
    }
  });

  it('bootstraps local memory to the remote backend and imports remote memory into SQLite', async () => {
    const backend = new FakeRemoteMemoryBackend([
      {
        id: 'session:remote-content',
        kind: 'session',
        sortEpoch: 200,
        updatedAtEpoch: 200,
        payload: {
          content_session_id: 'remote-content',
          memory_session_id: 'remote-memory',
          project: 'remote-project',
          user_prompt: 'Remote prompt',
          custom_title: null,
          started_at: new Date(200).toISOString(),
          started_at_epoch: 200,
          completed_at: null,
          completed_at_epoch: null,
          status: 'active',
        },
      },
      {
        id: 'prompt:remote-content:1',
        kind: 'prompt',
        sortEpoch: 210,
        updatedAtEpoch: 210,
        payload: {
          content_session_id: 'remote-content',
          project: 'remote-project',
          prompt_number: 1,
          prompt_text: 'Remote prompt',
          created_at: new Date(210).toISOString(),
          created_at_epoch: 210,
        },
      },
      {
        id: 'observation:remote',
        kind: 'observation',
        sortEpoch: 220,
        updatedAtEpoch: 220,
        payload: {
          memory_session_id: 'remote-memory',
          project: 'remote-project',
          text: null,
          type: 'discovery',
          title: 'Remote observation',
          subtitle: null,
          facts: '[]',
          narrative: 'Imported from another machine',
          concepts: '[]',
          files_read: '[]',
          files_modified: '[]',
          prompt_number: 1,
          discovery_tokens: 0,
          created_at: new Date(220).toISOString(),
          created_at_epoch: 220,
        },
      },
      {
        id: 'summary:remote',
        kind: 'summary',
        sortEpoch: 230,
        updatedAtEpoch: 230,
        payload: {
          memory_session_id: 'remote-memory',
          project: 'remote-project',
          request: 'Remote request',
          investigated: 'Remote investigation',
          learned: 'Remote learning',
          completed: 'Remote completion',
          next_steps: 'Remote next steps',
          files_read: null,
          files_edited: null,
          notes: null,
          prompt_number: 1,
          discovery_tokens: 0,
          created_at: new Date(230).toISOString(),
          created_at_epoch: 230,
        },
      },
    ]);

    const localSessionId = store.createSDKSession('local-content', 'local-project', 'Local prompt');
    store.updateMemorySessionId(localSessionId, 'local-memory');
    store.saveUserPrompt('local-content', 1, 'Local prompt');
    store.storeObservation('local-memory', 'local-project', {
      type: 'feature',
      title: 'Local observation',
      subtitle: null,
      facts: [],
      narrative: 'Saved locally first',
      concepts: [],
      files_read: [],
      files_modified: [],
    }, 1, 0, 120);
    store.storeSummary('local-memory', 'local-project', {
      request: 'Local request',
      investigated: 'Local investigation',
      learned: 'Local learning',
      completed: 'Local completion',
      next_steps: 'Local next steps',
      notes: null,
    }, 1, 0, 130);

    const sync = new RemoteMemorySync(backend, statePath, 60_000);
    await sync.initialize(store);
    await sync.flush();
    await sync.close();

    expect(backend.storedDocuments.has('session:local-content')).toBe(true);
    expect(backend.storedDocuments.has('prompt:local-content:1')).toBe(true);
    expect(
      [...backend.storedDocuments.values()].some(document =>
        document.kind === 'observation' &&
        document.payload.memory_session_id === 'local-memory'
      )
    ).toBe(true);
    expect(
      [...backend.storedDocuments.values()].some(document =>
        document.kind === 'summary' &&
        document.payload.memory_session_id === 'local-memory'
      )
    ).toBe(true);

    const remotePromptCount = store.getPromptNumberFromUserPrompts('remote-content');
    const remoteSummary = store.getSummaryForSession('remote-memory');
    const remoteObservationCount = (
      store.db.prepare('SELECT COUNT(*) as count FROM observations WHERE memory_session_id = ?')
        .get('remote-memory') as { count: number }
    ).count;

    expect(remotePromptCount).toBe(1);
    expect(remoteObservationCount).toBe(1);
    expect(remoteSummary?.request).toBe('Remote request');
  });

  it('piggybacks session updates when syncing a new observation', async () => {
    const backend = new FakeRemoteMemoryBackend();
    const sync = new RemoteMemorySync(backend, statePath, 60_000);

    const sessionId = store.createSDKSession('piggyback-content', 'piggyback-project', 'First prompt');
    const promptId = store.saveUserPrompt('piggyback-content', 1, 'First prompt');

    sync.scheduleUserPromptSync(store, promptId);
    await sync.flush();

    const initialSessionDocument = backend.storedDocuments.get('session:piggyback-content');
    expect(initialSessionDocument?.kind).toBe('session');
    if (!initialSessionDocument || initialSessionDocument.kind !== 'session') {
      throw new Error('Expected initial session document to exist');
    }
    expect(initialSessionDocument.payload.memory_session_id).toBeNull();

    store.updateMemorySessionId(sessionId, 'piggyback-memory');
    const observation = store.storeObservation('piggyback-memory', 'piggyback-project', {
      type: 'discovery',
      title: 'Observed after memory id capture',
      subtitle: null,
      facts: [],
      narrative: 'The session document should carry the latest memory session ID.',
      concepts: [],
      files_read: [],
      files_modified: [],
    }, 1, 0, 500);

    sync.scheduleObservationSync(store, observation.id);
    await sync.flush();
    await sync.close();

    const updatedSessionDocument = backend.storedDocuments.get('session:piggyback-content');
    expect(updatedSessionDocument?.kind).toBe('session');
    if (!updatedSessionDocument || updatedSessionDocument.kind !== 'session') {
      throw new Error('Expected updated session document to exist');
    }
    expect(updatedSessionDocument.payload.memory_session_id).toBe('piggyback-memory');
  });
});
