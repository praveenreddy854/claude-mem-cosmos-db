import { createHash } from 'crypto';
import { dirname } from 'path';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'fs';
import type { Database } from 'bun:sqlite';
import { SettingsDefaultsManager, type SettingsDefaults } from '../../shared/SettingsDefaultsManager.js';
import { REMOTE_SYNC_STATE_PATH, USER_SETTINGS_PATH } from '../../shared/paths.js';
import { logger } from '../../utils/logger.js';
import { AzureCosmosRemoteStore } from './AzureCosmosRemoteStore.js';

export type RemoteMemoryKind = 'session' | 'prompt' | 'observation' | 'summary';

export interface RemoteSessionPayload {
  content_session_id: string;
  memory_session_id: string | null;
  project: string;
  user_prompt: string | null;
  custom_title: string | null;
  started_at: string;
  started_at_epoch: number;
  completed_at: string | null;
  completed_at_epoch: number | null;
  status: string;
}

export interface RemotePromptPayload {
  content_session_id: string;
  project: string;
  prompt_number: number;
  prompt_text: string;
  created_at: string;
  created_at_epoch: number;
}

export interface RemoteObservationPayload {
  memory_session_id: string;
  project: string;
  text: string | null;
  type: string;
  title: string | null;
  subtitle: string | null;
  facts: string | null;
  narrative: string | null;
  concepts: string | null;
  files_read: string | null;
  files_modified: string | null;
  prompt_number: number | null;
  discovery_tokens: number;
  created_at: string;
  created_at_epoch: number;
}

export interface RemoteSummaryPayload {
  memory_session_id: string;
  project: string;
  request: string | null;
  investigated: string | null;
  learned: string | null;
  completed: string | null;
  next_steps: string | null;
  files_read: string | null;
  files_edited: string | null;
  notes: string | null;
  prompt_number: number | null;
  discovery_tokens: number;
  created_at: string;
  created_at_epoch: number;
}

interface RemoteMemoryBaseDocument<K extends RemoteMemoryKind, P> {
  id: string;
  kind: K;
  sortEpoch: number;
  updatedAtEpoch: number;
  payload: P;
}

export type RemoteSessionDocument = RemoteMemoryBaseDocument<'session', RemoteSessionPayload>;
export type RemotePromptDocument = RemoteMemoryBaseDocument<'prompt', RemotePromptPayload>;
export type RemoteObservationDocument = RemoteMemoryBaseDocument<'observation', RemoteObservationPayload>;
export type RemoteSummaryDocument = RemoteMemoryBaseDocument<'summary', RemoteSummaryPayload>;

export type RemoteMemoryDocument =
  | RemoteSessionDocument
  | RemotePromptDocument
  | RemoteObservationDocument
  | RemoteSummaryDocument;

export interface RemoteMemoryBackend {
  label: string;
  targetFingerprint: string;
  initialize(): Promise<void>;
  upsertDocuments(documents: RemoteMemoryDocument[]): Promise<void>;
  fetchDocumentsUpdatedSince(sinceEpoch: number): Promise<RemoteMemoryDocument[]>;
  close?(): Promise<void>;
}

interface RemoteSyncTargetState {
  bootstrapComplete: boolean;
  lastLocalPushEpoch: number;
  lastPullEpoch: number;
}

interface RemoteSyncStateFile {
  version: 1;
  targets: Record<string, RemoteSyncTargetState>;
}

interface LocalSessionRow extends RemoteSessionPayload {
  id: number;
}

interface LocalPromptRow extends RemotePromptPayload {
  id: number;
}

interface LocalObservationRow extends RemoteObservationPayload {
  id: number;
}

interface LocalSummaryRow extends RemoteSummaryPayload {
  id: number;
}

export interface RemoteSyncStore {
  db: Database;
}

const STATE_VERSION = 1;
const LOCAL_INCREMENTAL_OVERLAP_MS = 5_000;
const REMOTE_INCREMENTAL_OVERLAP_MS = 5_000;

export class RemoteMemorySync {
  private queue: Promise<void> = Promise.resolve();
  private intervalRef: ReturnType<typeof setInterval> | null = null;

  constructor(
    private readonly backend: RemoteMemoryBackend,
    private readonly statePath: string = REMOTE_SYNC_STATE_PATH,
    private readonly syncIntervalMs: number = 30_000
  ) {}

  static createFromSettings(
    settings: SettingsDefaults = SettingsDefaultsManager.loadFromFile(USER_SETTINGS_PATH),
    statePath: string = REMOTE_SYNC_STATE_PATH
  ): RemoteMemorySync | null {
    if (settings.CLAUDE_MEM_REMOTE_DB_ENABLED !== 'true') {
      return null;
    }

    const provider = settings.CLAUDE_MEM_REMOTE_DB_PROVIDER || 'azure-cosmos';
    if (provider !== 'azure-cosmos') {
      logger.warn('REMOTE_SYNC', 'Unsupported remote DB provider, remote sync disabled', {
        provider,
      });
      return null;
    }

    try {
      const backend = new AzureCosmosRemoteStore({
        connectionString: settings.CLAUDE_MEM_AZURE_COSMOS_CONNECTION_STRING || undefined,
        endpoint: settings.CLAUDE_MEM_AZURE_COSMOS_ENDPOINT || undefined,
        key: settings.CLAUDE_MEM_AZURE_COSMOS_KEY || undefined,
        database: settings.CLAUDE_MEM_AZURE_COSMOS_DATABASE || 'claude-mem',
        container: settings.CLAUDE_MEM_AZURE_COSMOS_CONTAINER || 'memory-records',
      });

      const intervalMs = parsePositiveInt(
        settings.CLAUDE_MEM_REMOTE_DB_SYNC_INTERVAL_MS,
        30_000
      );

      return new RemoteMemorySync(backend, statePath, intervalMs);
    } catch (error) {
      logger.error('REMOTE_SYNC', 'Failed to configure remote sync, continuing with local DB only', {}, error as Error);
      return null;
    }
  }

  async initialize(store: RemoteSyncStore): Promise<void> {
    try {
      await this.runTask(() => this.performSynchronization(store, 'startup', true));
      this.startBackgroundSync(store);
    } catch (error) {
      logger.error('REMOTE_SYNC', 'Initial remote sync failed, continuing with local DB only', {}, error as Error);
    }
  }

  close(): Promise<void> {
    if (this.intervalRef) {
      clearInterval(this.intervalRef);
      this.intervalRef = null;
    }

    return this.runTask(async () => {
      await this.backend.close?.();
    });
  }

  flush(): Promise<void> {
    return this.runTask(async () => undefined);
  }

  scheduleUserPromptSync(store: RemoteSyncStore, promptId: number): void {
    void this.runTask(async () => {
      const prompt = getPromptById(store.db, promptId);
      if (!prompt) {
        return;
      }

      const documents = collectPromptDocuments(store.db, [prompt]);
      await this.pushDocuments(documents, 'prompt');
    }).catch(error => {
      logger.error('REMOTE_SYNC', 'Prompt sync failed', { promptId }, error as Error);
    });
  }

  scheduleObservationSync(store: RemoteSyncStore, observationId: number): void {
    void this.runTask(async () => {
      const observation = getObservationById(store.db, observationId);
      if (!observation) {
        return;
      }

      const documents = collectObservationDocuments(store.db, [observation]);
      await this.pushDocuments(documents, 'observation');
    }).catch(error => {
      logger.error('REMOTE_SYNC', 'Observation sync failed', { observationId }, error as Error);
    });
  }

  scheduleSummarySync(store: RemoteSyncStore, summaryId: number): void {
    void this.runTask(async () => {
      const summary = getSummaryById(store.db, summaryId);
      if (!summary) {
        return;
      }

      const documents = collectSummaryDocuments(store.db, [summary]);
      await this.pushDocuments(documents, 'summary');
    }).catch(error => {
      logger.error('REMOTE_SYNC', 'Summary sync failed', { summaryId }, error as Error);
    });
  }

  scheduleFullSync(store: RemoteSyncStore, reason: string): void {
    void this.runTask(() => this.performSynchronization(store, reason, false)).catch(error => {
      logger.error('REMOTE_SYNC', 'Periodic remote sync failed', { reason }, error as Error);
    });
  }

  private startBackgroundSync(store: RemoteSyncStore): void {
    if (this.intervalRef || this.syncIntervalMs <= 0) {
      return;
    }

    this.intervalRef = setInterval(() => {
      this.scheduleFullSync(store, 'interval');
    }, this.syncIntervalMs);
  }

  private async performSynchronization(
    store: RemoteSyncStore,
    reason: string,
    bootstrapLocal: boolean
  ): Promise<void> {
    await this.backend.initialize();

    const state = this.loadTargetState();
    const pushedCounts = {
      bootstrap: 0,
      incremental: 0,
      pulled: 0,
    };

    if (bootstrapLocal && !state.bootstrapComplete) {
      const bootstrapDocuments = collectBootstrapDocuments(store.db);
      await this.pushDocuments(bootstrapDocuments, 'bootstrap');
      state.bootstrapComplete = true;
      state.lastLocalPushEpoch = Math.max(
        state.lastLocalPushEpoch,
        getMaxUpdatedEpoch(bootstrapDocuments)
      );
      pushedCounts.bootstrap = bootstrapDocuments.length;
    }

    const localSince = Math.max(0, state.lastLocalPushEpoch - LOCAL_INCREMENTAL_OVERLAP_MS);
    const incrementalDocuments = collectIncrementalDocuments(store.db, localSince);
    if (incrementalDocuments.length > 0) {
      await this.pushDocuments(incrementalDocuments, 'incremental');
      state.lastLocalPushEpoch = Math.max(
        state.lastLocalPushEpoch,
        getMaxUpdatedEpoch(incrementalDocuments)
      );
      pushedCounts.incremental = incrementalDocuments.length;
    }

    const remoteSince = Math.max(0, state.lastPullEpoch - REMOTE_INCREMENTAL_OVERLAP_MS);
    const remoteDocuments = await this.backend.fetchDocumentsUpdatedSince(remoteSince);
    if (remoteDocuments.length > 0) {
      const importedCount = importRemoteDocuments(store.db, remoteDocuments);
      state.lastPullEpoch = Math.max(
        state.lastPullEpoch,
        getMaxUpdatedEpoch(remoteDocuments)
      );
      pushedCounts.pulled = importedCount;
    }

    this.saveTargetState(state);

    logger.info('REMOTE_SYNC', 'Remote memory sync complete', {
      reason,
      provider: this.backend.label,
      pushedBootstrap: pushedCounts.bootstrap,
      pushedIncremental: pushedCounts.incremental,
      pulled: pushedCounts.pulled,
    });
  }

  private async pushDocuments(documents: RemoteMemoryDocument[], phase: string): Promise<void> {
    if (documents.length === 0) {
      return;
    }

    await this.backend.upsertDocuments(documents);

    const state = this.loadTargetState();
    state.lastLocalPushEpoch = Math.max(
      state.lastLocalPushEpoch,
      getMaxUpdatedEpoch(documents)
    );
    this.saveTargetState(state);

    logger.debug('REMOTE_SYNC', 'Pushed documents to remote backend', {
      phase,
      count: documents.length,
    });
  }

  private runTask(task: () => Promise<void>): Promise<void> {
    const nextTask = this.queue.catch(() => undefined).then(task);
    this.queue = nextTask.catch(() => undefined);
    return nextTask;
  }

  private loadTargetState(): RemoteSyncTargetState {
    const fileState = loadStateFile(this.statePath);
    return fileState.targets[this.backend.targetFingerprint] ?? {
      bootstrapComplete: false,
      lastLocalPushEpoch: 0,
      lastPullEpoch: 0,
    };
  }

  private saveTargetState(targetState: RemoteSyncTargetState): void {
    const fileState = loadStateFile(this.statePath);
    fileState.targets[this.backend.targetFingerprint] = targetState;
    saveStateFile(this.statePath, fileState);
  }
}

function collectBootstrapDocuments(db: Database): RemoteMemoryDocument[] {
  const documents = new Map<string, RemoteMemoryDocument>();

  for (const session of getAllSessions(db)) {
    addDocument(documents, buildSessionDocument(session));
  }

  for (const prompt of getAllPrompts(db)) {
    for (const document of collectPromptDocuments(db, [prompt])) {
      addDocument(documents, document);
    }
  }

  for (const observation of getAllObservations(db)) {
    for (const document of collectObservationDocuments(db, [observation])) {
      addDocument(documents, document);
    }
  }

  for (const summary of getAllSummaries(db)) {
    for (const document of collectSummaryDocuments(db, [summary])) {
      addDocument(documents, document);
    }
  }

  return sortDocuments(documents);
}

function collectIncrementalDocuments(db: Database, sinceEpoch: number): RemoteMemoryDocument[] {
  const documents = new Map<string, RemoteMemoryDocument>();

  for (const prompt of getPromptsSince(db, sinceEpoch)) {
    for (const document of collectPromptDocuments(db, [prompt])) {
      addDocument(documents, document);
    }
  }

  for (const observation of getObservationsSince(db, sinceEpoch)) {
    for (const document of collectObservationDocuments(db, [observation])) {
      addDocument(documents, document);
    }
  }

  for (const summary of getSummariesSince(db, sinceEpoch)) {
    for (const document of collectSummaryDocuments(db, [summary])) {
      addDocument(documents, document);
    }
  }

  return sortDocuments(documents);
}

function collectPromptDocuments(
  db: Database,
  prompts: LocalPromptRow[]
): RemoteMemoryDocument[] {
  const documents = new Map<string, RemoteMemoryDocument>();

  for (const prompt of prompts) {
    addDocument(documents, buildPromptDocument(prompt));

    const session = getSessionByContentSessionId(db, prompt.content_session_id);
    if (session) {
      addDocument(documents, buildSessionDocument(session, prompt.created_at_epoch));
    }
  }

  return sortDocuments(documents);
}

function collectObservationDocuments(
  db: Database,
  observations: LocalObservationRow[]
): RemoteMemoryDocument[] {
  const documents = new Map<string, RemoteMemoryDocument>();

  for (const observation of observations) {
    addDocument(documents, buildObservationDocument(observation));

    const session = getSessionByMemorySessionId(db, observation.memory_session_id);
    if (session) {
      addDocument(documents, buildSessionDocument(session, observation.created_at_epoch));
    }
  }

  return sortDocuments(documents);
}

function collectSummaryDocuments(
  db: Database,
  summaries: LocalSummaryRow[]
): RemoteMemoryDocument[] {
  const documents = new Map<string, RemoteMemoryDocument>();

  for (const summary of summaries) {
    addDocument(documents, buildSummaryDocument(summary));

    const session = getSessionByMemorySessionId(db, summary.memory_session_id);
    if (session) {
      addDocument(documents, buildSessionDocument(session, summary.created_at_epoch));
    }
  }

  return sortDocuments(documents);
}

function addDocument(
  documents: Map<string, RemoteMemoryDocument>,
  document: RemoteMemoryDocument
): void {
  const existing = documents.get(document.id);
  if (!existing || existing.updatedAtEpoch <= document.updatedAtEpoch) {
    documents.set(document.id, document);
  }
}

function sortDocuments(documents: Map<string, RemoteMemoryDocument>): RemoteMemoryDocument[] {
  return [...documents.values()].sort((left, right) => {
    if (left.updatedAtEpoch !== right.updatedAtEpoch) {
      return left.updatedAtEpoch - right.updatedAtEpoch;
    }

    return left.id.localeCompare(right.id);
  });
}

function getMaxUpdatedEpoch(documents: RemoteMemoryDocument[]): number {
  return documents.reduce(
    (maxEpoch, document) => Math.max(maxEpoch, document.updatedAtEpoch),
    0
  );
}

function buildSessionDocument(
  session: LocalSessionRow,
  updatedAtEpoch?: number
): RemoteSessionDocument {
  return {
    id: `session:${session.content_session_id}`,
    kind: 'session',
    sortEpoch: session.started_at_epoch,
    updatedAtEpoch: updatedAtEpoch ?? getSessionStableUpdatedEpoch(session),
    payload: {
      content_session_id: session.content_session_id,
      memory_session_id: session.memory_session_id,
      project: session.project,
      user_prompt: session.user_prompt,
      custom_title: session.custom_title,
      started_at: session.started_at,
      started_at_epoch: session.started_at_epoch,
      completed_at: session.completed_at,
      completed_at_epoch: session.completed_at_epoch,
      status: session.status,
    },
  };
}

function buildPromptDocument(prompt: LocalPromptRow): RemotePromptDocument {
  return {
    id: `prompt:${prompt.content_session_id}:${prompt.prompt_number}`,
    kind: 'prompt',
    sortEpoch: prompt.created_at_epoch,
    updatedAtEpoch: prompt.created_at_epoch,
    payload: {
      content_session_id: prompt.content_session_id,
      project: prompt.project,
      prompt_number: prompt.prompt_number,
      prompt_text: prompt.prompt_text,
      created_at: prompt.created_at,
      created_at_epoch: prompt.created_at_epoch,
    },
  };
}

function buildObservationDocument(
  observation: LocalObservationRow
): RemoteObservationDocument {
  const payload: RemoteObservationPayload = {
    memory_session_id: observation.memory_session_id,
    project: observation.project,
    text: observation.text,
    type: observation.type,
    title: observation.title,
    subtitle: observation.subtitle,
    facts: observation.facts,
    narrative: observation.narrative,
    concepts: observation.concepts,
    files_read: observation.files_read,
    files_modified: observation.files_modified,
    prompt_number: observation.prompt_number,
    discovery_tokens: observation.discovery_tokens || 0,
    created_at: observation.created_at,
    created_at_epoch: observation.created_at_epoch,
  };

  return {
    id: buildHashDocumentId('observation', payload),
    kind: 'observation',
    sortEpoch: observation.created_at_epoch,
    updatedAtEpoch: observation.created_at_epoch,
    payload,
  };
}

function buildSummaryDocument(summary: LocalSummaryRow): RemoteSummaryDocument {
  const payload: RemoteSummaryPayload = {
    memory_session_id: summary.memory_session_id,
    project: summary.project,
    request: summary.request,
    investigated: summary.investigated,
    learned: summary.learned,
    completed: summary.completed,
    next_steps: summary.next_steps,
    files_read: summary.files_read,
    files_edited: summary.files_edited,
    notes: summary.notes,
    prompt_number: summary.prompt_number,
    discovery_tokens: summary.discovery_tokens || 0,
    created_at: summary.created_at,
    created_at_epoch: summary.created_at_epoch,
  };

  return {
    id: buildHashDocumentId('summary', payload),
    kind: 'summary',
    sortEpoch: summary.created_at_epoch,
    updatedAtEpoch: summary.created_at_epoch,
    payload,
  };
}

function buildHashDocumentId(kind: 'observation' | 'summary', payload: object): string {
  return `${kind}:${createHash('sha256').update(JSON.stringify(payload)).digest('hex')}`;
}

function importRemoteDocuments(db: Database, documents: RemoteMemoryDocument[]): number {
  const orderedDocuments = [
    ...documents.filter(document => document.kind === 'session'),
    ...documents.filter(document => document.kind === 'prompt'),
    ...documents.filter(document => document.kind === 'observation'),
    ...documents.filter(document => document.kind === 'summary'),
  ];

  const importTransaction = db.transaction(() => {
    let importedCount = 0;

    for (const document of orderedDocuments) {
      switch (document.kind) {
        case 'session':
          importedCount += upsertSession(db, document.payload) ? 1 : 0;
          break;
        case 'prompt':
          importedCount += upsertPrompt(db, document.payload) ? 1 : 0;
          break;
        case 'observation':
          importedCount += upsertObservation(db, document.payload) ? 1 : 0;
          break;
        case 'summary':
          importedCount += upsertSummary(db, document.payload) ? 1 : 0;
          break;
      }
    }

    return importedCount;
  });

  return importTransaction();
}

function upsertSession(db: Database, session: RemoteSessionPayload): boolean {
  const existing = db.prepare(`
    SELECT
      id,
      content_session_id,
      memory_session_id,
      project,
      user_prompt,
      custom_title,
      started_at,
      started_at_epoch,
      completed_at,
      completed_at_epoch,
      status
    FROM sdk_sessions
    WHERE content_session_id = ?
    LIMIT 1
  `).get(session.content_session_id) as LocalSessionRow | undefined;

  const resolvedMemorySessionId = resolveImportedMemorySessionId(db, session, existing);
  if (!existing) {
    db.prepare(`
      INSERT INTO sdk_sessions (
        content_session_id,
        memory_session_id,
        project,
        user_prompt,
        custom_title,
        started_at,
        started_at_epoch,
        completed_at,
        completed_at_epoch,
        status
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      session.content_session_id,
      resolvedMemorySessionId,
      session.project,
      session.user_prompt ?? '',
      session.custom_title,
      session.started_at,
      session.started_at_epoch,
      session.completed_at,
      session.completed_at_epoch,
      session.status
    );
    return true;
  }

  const nextValues = {
    memory_session_id: resolvedMemorySessionId,
    project: session.project || existing.project,
    user_prompt: session.user_prompt || existing.user_prompt,
    custom_title: session.custom_title ?? existing.custom_title,
    started_at: existing.started_at || session.started_at,
    started_at_epoch: existing.started_at_epoch || session.started_at_epoch,
    completed_at: session.completed_at ?? existing.completed_at,
    completed_at_epoch: session.completed_at_epoch ?? existing.completed_at_epoch,
    status: selectSessionStatus(existing.status, session.status),
  };

  const changed =
    nextValues.memory_session_id !== existing.memory_session_id ||
    nextValues.project !== existing.project ||
    nextValues.user_prompt !== existing.user_prompt ||
    nextValues.custom_title !== existing.custom_title ||
    nextValues.started_at !== existing.started_at ||
    nextValues.started_at_epoch !== existing.started_at_epoch ||
    nextValues.completed_at !== existing.completed_at ||
    nextValues.completed_at_epoch !== existing.completed_at_epoch ||
    nextValues.status !== existing.status;

  if (!changed) {
    return false;
  }

  db.prepare(`
    UPDATE sdk_sessions
    SET
      memory_session_id = ?,
      project = ?,
      user_prompt = ?,
      custom_title = ?,
      started_at = ?,
      started_at_epoch = ?,
      completed_at = ?,
      completed_at_epoch = ?,
      status = ?
    WHERE id = ?
  `).run(
    nextValues.memory_session_id,
    nextValues.project,
    nextValues.user_prompt,
    nextValues.custom_title,
    nextValues.started_at,
    nextValues.started_at_epoch,
    nextValues.completed_at,
    nextValues.completed_at_epoch,
    nextValues.status,
    existing.id
  );

  return true;
}

function upsertPrompt(db: Database, prompt: RemotePromptPayload): boolean {
  const existing = db.prepare(`
    SELECT id
    FROM user_prompts
    WHERE content_session_id = ? AND prompt_number = ?
    LIMIT 1
  `).get(prompt.content_session_id, prompt.prompt_number) as { id: number } | undefined;

  if (existing) {
    return false;
  }

  const sessionExists = db.prepare(`
    SELECT id
    FROM sdk_sessions
    WHERE content_session_id = ?
    LIMIT 1
  `).get(prompt.content_session_id) as { id: number } | undefined;

  if (!sessionExists) {
    logger.warn('REMOTE_SYNC', 'Skipping remote prompt import because session is missing locally', {
      contentSessionId: prompt.content_session_id,
      promptNumber: prompt.prompt_number,
    });
    return false;
  }

  db.prepare(`
    INSERT INTO user_prompts (
      content_session_id,
      prompt_number,
      prompt_text,
      created_at,
      created_at_epoch
    ) VALUES (?, ?, ?, ?, ?)
  `).run(
    prompt.content_session_id,
    prompt.prompt_number,
    prompt.prompt_text,
    prompt.created_at,
    prompt.created_at_epoch
  );

  return true;
}

function upsertObservation(db: Database, observation: RemoteObservationPayload): boolean {
  const sessionExists = db.prepare(`
    SELECT id
    FROM sdk_sessions
    WHERE memory_session_id = ?
    LIMIT 1
  `).get(observation.memory_session_id) as { id: number } | undefined;

  if (!sessionExists) {
    logger.warn('REMOTE_SYNC', 'Skipping remote observation import because session is missing locally', {
      memorySessionId: observation.memory_session_id,
      createdAtEpoch: observation.created_at_epoch,
    });
    return false;
  }

  const existing = db.prepare(`
    SELECT id
    FROM observations
    WHERE memory_session_id = ?
      AND created_at_epoch = ?
      AND type = ?
      AND COALESCE(title, '') = COALESCE(?, '')
      AND COALESCE(narrative, '') = COALESCE(?, '')
    LIMIT 1
  `).get(
    observation.memory_session_id,
    observation.created_at_epoch,
    observation.type,
    observation.title,
    observation.narrative
  ) as { id: number } | undefined;

  if (existing) {
    return false;
  }

  db.prepare(`
    INSERT INTO observations (
      memory_session_id,
      project,
      text,
      type,
      title,
      subtitle,
      facts,
      narrative,
      concepts,
      files_read,
      files_modified,
      prompt_number,
      discovery_tokens,
      created_at,
      created_at_epoch
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    observation.memory_session_id,
    observation.project,
    observation.text,
    observation.type,
    observation.title,
    observation.subtitle,
    observation.facts,
    observation.narrative,
    observation.concepts,
    observation.files_read,
    observation.files_modified,
    observation.prompt_number,
    observation.discovery_tokens || 0,
    observation.created_at,
    observation.created_at_epoch
  );

  return true;
}

function upsertSummary(db: Database, summary: RemoteSummaryPayload): boolean {
  const sessionExists = db.prepare(`
    SELECT id
    FROM sdk_sessions
    WHERE memory_session_id = ?
    LIMIT 1
  `).get(summary.memory_session_id) as { id: number } | undefined;

  if (!sessionExists) {
    logger.warn('REMOTE_SYNC', 'Skipping remote summary import because session is missing locally', {
      memorySessionId: summary.memory_session_id,
      createdAtEpoch: summary.created_at_epoch,
    });
    return false;
  }

  const existing = db.prepare(`
    SELECT id
    FROM session_summaries
    WHERE memory_session_id = ?
      AND created_at_epoch = ?
      AND COALESCE(prompt_number, -1) = COALESCE(?, -1)
    LIMIT 1
  `).get(
    summary.memory_session_id,
    summary.created_at_epoch,
    summary.prompt_number
  ) as { id: number } | undefined;

  if (existing) {
    return false;
  }

  db.prepare(`
    INSERT INTO session_summaries (
      memory_session_id,
      project,
      request,
      investigated,
      learned,
      completed,
      next_steps,
      files_read,
      files_edited,
      notes,
      prompt_number,
      discovery_tokens,
      created_at,
      created_at_epoch
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    summary.memory_session_id,
    summary.project,
    summary.request,
    summary.investigated,
    summary.learned,
    summary.completed,
    summary.next_steps,
    summary.files_read,
    summary.files_edited,
    summary.notes,
    summary.prompt_number,
    summary.discovery_tokens || 0,
    summary.created_at,
    summary.created_at_epoch
  );

  return true;
}

function resolveImportedMemorySessionId(
  db: Database,
  session: RemoteSessionPayload,
  existing: LocalSessionRow | undefined
): string | null {
  if (!session.memory_session_id) {
    return existing?.memory_session_id ?? null;
  }

  const conflict = db.prepare(`
    SELECT id
    FROM sdk_sessions
    WHERE memory_session_id = ? AND content_session_id != ?
    LIMIT 1
  `).get(session.memory_session_id, session.content_session_id) as { id: number } | undefined;

  if (conflict) {
    logger.warn('REMOTE_SYNC', 'Skipping conflicting remote memory session ID', {
      contentSessionId: session.content_session_id,
      memorySessionId: session.memory_session_id,
      conflictingSessionId: conflict.id,
    });
    return existing?.memory_session_id ?? null;
  }

  return session.memory_session_id;
}

function selectSessionStatus(currentStatus: string, incomingStatus: string): string {
  const priority: Record<string, number> = {
    active: 0,
    failed: 1,
    completed: 2,
  };

  return (priority[incomingStatus] ?? 0) >= (priority[currentStatus] ?? 0)
    ? incomingStatus
    : currentStatus;
}

function getSessionStableUpdatedEpoch(session: RemoteSessionPayload): number {
  return Math.max(session.started_at_epoch, session.completed_at_epoch ?? 0);
}

function getAllSessions(db: Database): LocalSessionRow[] {
  return db.prepare(`
    SELECT
      id,
      content_session_id,
      memory_session_id,
      project,
      user_prompt,
      custom_title,
      started_at,
      started_at_epoch,
      completed_at,
      completed_at_epoch,
      status
    FROM sdk_sessions
    ORDER BY started_at_epoch ASC
  `).all() as LocalSessionRow[];
}

function getAllPrompts(db: Database): LocalPromptRow[] {
  return db.prepare(`
    SELECT
      user_prompts.id,
      user_prompts.content_session_id,
      COALESCE(project, '') AS project,
      prompt_number,
      prompt_text,
      created_at,
      created_at_epoch
    FROM user_prompts
    LEFT JOIN sdk_sessions USING (content_session_id)
    ORDER BY created_at_epoch ASC
  `).all() as LocalPromptRow[];
}

function getAllObservations(db: Database): LocalObservationRow[] {
  return db.prepare(`
    SELECT
      id,
      memory_session_id,
      project,
      text,
      type,
      title,
      subtitle,
      facts,
      narrative,
      concepts,
      files_read,
      files_modified,
      prompt_number,
      discovery_tokens,
      created_at,
      created_at_epoch
    FROM observations
    ORDER BY created_at_epoch ASC
  `).all() as LocalObservationRow[];
}

function getAllSummaries(db: Database): LocalSummaryRow[] {
  return db.prepare(`
    SELECT
      id,
      memory_session_id,
      project,
      request,
      investigated,
      learned,
      completed,
      next_steps,
      files_read,
      files_edited,
      notes,
      prompt_number,
      discovery_tokens,
      created_at,
      created_at_epoch
    FROM session_summaries
    ORDER BY created_at_epoch ASC
  `).all() as LocalSummaryRow[];
}

function getPromptsSince(db: Database, sinceEpoch: number): LocalPromptRow[] {
  return db.prepare(`
    SELECT
      user_prompts.id,
      user_prompts.content_session_id,
      COALESCE(project, '') AS project,
      prompt_number,
      prompt_text,
      created_at,
      created_at_epoch
    FROM user_prompts
    LEFT JOIN sdk_sessions USING (content_session_id)
    WHERE created_at_epoch >= ?
    ORDER BY created_at_epoch ASC
  `).all(sinceEpoch) as LocalPromptRow[];
}

function getObservationsSince(db: Database, sinceEpoch: number): LocalObservationRow[] {
  return db.prepare(`
    SELECT
      id,
      memory_session_id,
      project,
      text,
      type,
      title,
      subtitle,
      facts,
      narrative,
      concepts,
      files_read,
      files_modified,
      prompt_number,
      discovery_tokens,
      created_at,
      created_at_epoch
    FROM observations
    WHERE created_at_epoch >= ?
    ORDER BY created_at_epoch ASC
  `).all(sinceEpoch) as LocalObservationRow[];
}

function getSummariesSince(db: Database, sinceEpoch: number): LocalSummaryRow[] {
  return db.prepare(`
    SELECT
      id,
      memory_session_id,
      project,
      request,
      investigated,
      learned,
      completed,
      next_steps,
      files_read,
      files_edited,
      notes,
      prompt_number,
      discovery_tokens,
      created_at,
      created_at_epoch
    FROM session_summaries
    WHERE created_at_epoch >= ?
    ORDER BY created_at_epoch ASC
  `).all(sinceEpoch) as LocalSummaryRow[];
}

function getSessionByContentSessionId(
  db: Database,
  contentSessionId: string
): LocalSessionRow | undefined {
  return db.prepare(`
    SELECT
      id,
      content_session_id,
      memory_session_id,
      project,
      user_prompt,
      custom_title,
      started_at,
      started_at_epoch,
      completed_at,
      completed_at_epoch,
      status
    FROM sdk_sessions
    WHERE content_session_id = ?
    LIMIT 1
  `).get(contentSessionId) as LocalSessionRow | undefined;
}

function getSessionByMemorySessionId(
  db: Database,
  memorySessionId: string
): LocalSessionRow | undefined {
  return db.prepare(`
    SELECT
      id,
      content_session_id,
      memory_session_id,
      project,
      user_prompt,
      custom_title,
      started_at,
      started_at_epoch,
      completed_at,
      completed_at_epoch,
      status
    FROM sdk_sessions
    WHERE memory_session_id = ?
    LIMIT 1
  `).get(memorySessionId) as LocalSessionRow | undefined;
}

function getPromptById(db: Database, promptId: number): LocalPromptRow | undefined {
  return db.prepare(`
    SELECT
      up.id,
      up.content_session_id,
      COALESCE(s.project, '') AS project,
      up.prompt_number,
      up.prompt_text,
      up.created_at,
      up.created_at_epoch
    FROM user_prompts up
    LEFT JOIN sdk_sessions s ON s.content_session_id = up.content_session_id
    WHERE up.id = ?
    LIMIT 1
  `).get(promptId) as LocalPromptRow | undefined;
}

function getObservationById(
  db: Database,
  observationId: number
): LocalObservationRow | undefined {
  return db.prepare(`
    SELECT
      id,
      memory_session_id,
      project,
      text,
      type,
      title,
      subtitle,
      facts,
      narrative,
      concepts,
      files_read,
      files_modified,
      prompt_number,
      discovery_tokens,
      created_at,
      created_at_epoch
    FROM observations
    WHERE id = ?
    LIMIT 1
  `).get(observationId) as LocalObservationRow | undefined;
}

function getSummaryById(db: Database, summaryId: number): LocalSummaryRow | undefined {
  return db.prepare(`
    SELECT
      id,
      memory_session_id,
      project,
      request,
      investigated,
      learned,
      completed,
      next_steps,
      files_read,
      files_edited,
      notes,
      prompt_number,
      discovery_tokens,
      created_at,
      created_at_epoch
    FROM session_summaries
    WHERE id = ?
    LIMIT 1
  `).get(summaryId) as LocalSummaryRow | undefined;
}

function loadStateFile(statePath: string): RemoteSyncStateFile {
  try {
    if (!existsSync(statePath)) {
      return {
        version: STATE_VERSION,
        targets: {},
      };
    }

    const raw = readFileSync(statePath, 'utf-8');
    const parsed = JSON.parse(raw) as RemoteSyncStateFile;

    if (parsed.version !== STATE_VERSION || !parsed.targets) {
      return {
        version: STATE_VERSION,
        targets: {},
      };
    }

    return parsed;
  } catch (error) {
    logger.warn('REMOTE_SYNC', 'Failed to read remote sync state, recreating it', {
      statePath,
    }, error as Error);
    return {
      version: STATE_VERSION,
      targets: {},
    };
  }
}

function saveStateFile(statePath: string, state: RemoteSyncStateFile): void {
  const parentDir = dirname(statePath);
  if (!existsSync(parentDir)) {
    mkdirSync(parentDir, { recursive: true });
  }

  writeFileSync(statePath, JSON.stringify(state, null, 2) + '\n', 'utf-8');
}

function parsePositiveInt(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value ?? '', 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}
