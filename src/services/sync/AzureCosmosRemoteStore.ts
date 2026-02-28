import { createHash } from 'crypto';
import { CosmosClient, type Container, type SqlQuerySpec } from '@azure/cosmos';
import { logger } from '../../utils/logger.js';
import type { RemoteMemoryBackend, RemoteMemoryDocument } from './RemoteMemorySync.js';

export interface AzureCosmosRemoteStoreConfig {
  connectionString?: string;
  endpoint?: string;
  key?: string;
  database: string;
  container: string;
}

export class AzureCosmosRemoteStore implements RemoteMemoryBackend {
  readonly label = 'azure-cosmos';
  readonly targetFingerprint: string;

  private readonly client: CosmosClient;
  private containerRef: Container | null = null;

  constructor(private readonly config: AzureCosmosRemoteStoreConfig) {
    this.client = createCosmosClient(config);
    this.targetFingerprint = createHash('sha256')
      .update(
        JSON.stringify({
          provider: this.label,
          endpoint: config.endpoint || '(connection-string)',
          database: config.database,
          container: config.container,
        })
      )
      .digest('hex');
  }

  async initialize(): Promise<void> {
    if (this.containerRef) {
      return;
    }

    const { database } = await this.client.databases.createIfNotExists({
      id: this.config.database,
    });

    const { container } = await database.containers.createIfNotExists({
      id: this.config.container,
      partitionKey: {
        paths: ['/kind'],
      },
    });

    this.containerRef = container;

    logger.info('REMOTE_SYNC', 'Azure Cosmos remote memory ready', {
      database: this.config.database,
      container: this.config.container,
    });
  }

  async upsertDocuments(documents: RemoteMemoryDocument[]): Promise<void> {
    if (documents.length === 0) {
      return;
    }

    await this.initialize();

    const container = this.getContainer();
    for (const document of documents) {
      await container.items.upsert(document);
    }
  }

  async fetchDocumentsUpdatedSince(sinceEpoch: number): Promise<RemoteMemoryDocument[]> {
    await this.initialize();

    const querySpec: SqlQuerySpec = {
      query: 'SELECT * FROM c WHERE c.updatedAtEpoch > @sinceEpoch',
      parameters: [{ name: '@sinceEpoch', value: sinceEpoch }],
    };

    const { resources } = await this.getContainer()
      .items.query<RemoteMemoryDocument>(querySpec)
      .fetchAll();

    return resources.sort((left, right) => left.updatedAtEpoch - right.updatedAtEpoch);
  }

  async close(): Promise<void> {
    this.containerRef = null;
  }

  private getContainer(): Container {
    if (!this.containerRef) {
      throw new Error('Azure Cosmos container not initialized');
    }
    return this.containerRef;
  }
}

function createCosmosClient(config: AzureCosmosRemoteStoreConfig): CosmosClient {
  if (config.connectionString) {
    return new CosmosClient(config.connectionString);
  }

  if (config.endpoint && config.key) {
    return new CosmosClient({
      endpoint: config.endpoint,
      key: config.key,
    });
  }

  throw new Error(
    'Remote DB is enabled but Azure Cosmos credentials are missing. Set CLAUDE_MEM_AZURE_COSMOS_CONNECTION_STRING or CLAUDE_MEM_AZURE_COSMOS_ENDPOINT and CLAUDE_MEM_AZURE_COSMOS_KEY.'
  );
}
