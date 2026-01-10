import { Injectable, OnModuleDestroy } from '@nestjs/common';
import Redis from 'ioredis';

@Injectable()
export class RedisService implements OnModuleDestroy {
  private client: Redis;

  constructor() {
    // build the client from env vars
    this.client = new Redis({
      host: process.env.REDIS_HOST || '127.0.0.1',
      port: Number(process.env.REDIS_PORT || 6379),
      password: process.env.REDIS_PASSWORD || undefined,
      db: Number(process.env.REDIS_DB || 0),

      // good defaults:
      lazyConnect: false,       // connect immediately so we fail fast on startup
      enableReadyCheck: true,   // wait for "ready" before resolving ops
      maxRetriesPerRequest: 2,  // keep failures snappy instead of hanging forever
    });

    // optional: log basic connectivity (comment out in prod if too noisy)
    // this.client.on('ready', () => console.log('[redis] ready'));
    // this.client.on('error', (e) => console.error('[redis] error', e));
  }

  /** simple JSON get: returns parsed value or null */
  async get<T = unknown>(key: string): Promise<T | null> {
    const raw = await this.client.get(key);
    return raw ? (JSON.parse(raw) as T) : null;
  }

  /** simple JSON set with TTL (defaults to REDIS_DEFAULT_TTL_SEC) */
  async set(key: string, value: unknown, ttlSec?: number): Promise<void> {
    const ttl = ttlSec ?? Number(process.env.REDIS_DEFAULT_TTL_SEC || 10);
    const payload = JSON.stringify(value);
    if (ttl > 0) {
      await this.client.set(key, payload, 'EX', ttl);
    } else {
      await this.client.set(key, payload);
    }
  }

  /** handy raw access if you need native Redis commands */
  get raw(): Redis {
    return this.client;
  }

  /** graceful shutdown so Node exits cleanly */
  async onModuleDestroy() {
    try {
      await this.client.quit();
    } catch {
      await this.client.disconnect();
    }
  }
}
