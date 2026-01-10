import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { Pool, PoolClient, QueryResult, QueryResultRow } from 'pg';

@Injectable()
export class PgService implements OnModuleDestroy {
  private pool: Pool;

  constructor() {
    const useUrl = process.env.DATABASE_URL;
    const ssl =
      process.env.PG_SSL === '1' || process.env.PGSSLMODE === 'require'
        ? { rejectUnauthorized: false }
        : undefined;

    this.pool = new Pool({
      connectionString: useUrl || undefined,
      host: useUrl ? undefined : process.env.PG_HOST,
      port: useUrl ? undefined : (process.env.PG_PORT ? Number(process.env.PG_PORT) : undefined),
      database: useUrl ? undefined : process.env.PG_DATABASE,
      user: useUrl ? undefined : process.env.PG_USER,
      password: useUrl ? undefined : process.env.PG_PASSWORD,
      ssl,
      max: Number(process.env.DB_POOL_MAX || 12),
      idleTimeoutMillis: Number(process.env.DB_IDLE_MS || 30_000),
      connectionTimeoutMillis: Number(process.env.DB_CONN_TIMEOUT_MS || 10_000),
    } as any);
  }

  /**
   * Run a single query with an optional per-statement timeout.
   * T is the row type (defaults to generic QueryResultRow).
   */
  async query<T extends QueryResultRow = QueryResultRow>(
    text: string,
    params?: any[],
  ): Promise<QueryResult<T>> {
    const statementTimeout = Number(process.env.DB_STATEMENT_TIMEOUT_MS || 0);

    // 1) borrow a client from the pool
    const client = await this.pool.connect();
    try {
      // 2) set a LOCAL statement timeout (only for this transaction/statement scope)
      if (statementTimeout > 0) {
        await client.query(`SET LOCAL statement_timeout = '${statementTimeout}ms'`);
      }
      // 3) run the actual query
      return await client.query<T>(text, params);
    } finally {
      // 4) always release client back to pool (even if an error occurs)
      client.release();
    }
  }

  /**
   * Use an explicit client when you need multiple queries atomically
   * (e.g., begin/commit). You provide a function that receives the client.
   */
  async withClient<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      const statementTimeout = Number(process.env.DB_STATEMENT_TIMEOUT_MS || 0);
      if (statementTimeout > 0) {
        await client.query(`SET LOCAL statement_timeout = '${statementTimeout}ms'`);
      }
      return await fn(client);
    } finally {
      client.release();
    }
  }

  /**
   * Nest calls this when the app shuts down; we close the pool cleanly.
   */
  async onModuleDestroy() {
    await this.pool.end();
  }
}
