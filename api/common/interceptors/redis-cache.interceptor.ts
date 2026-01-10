import {
  CallHandler,
  ExecutionContext,
  Injectable,
  NestInterceptor,
} from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { Observable, of } from 'rxjs';
import { tap } from 'rxjs/operators';
import * as crypto from 'crypto';
import { createClient, RedisClientType } from 'redis';
import { CACHE_TTL, CACHE_BYPASS } from '../decorators/cache-ttl.decorator';

/**
 * ---- Redis client (singleton) --------------------------------------------
 * Supports:
 * - REDIS_URL (redis:// or rediss://). If rediss://, TLS is auto-enabled.
 * - or REDIS_HOST / REDIS_PORT / REDIS_USER / REDIS_PASSWORD / REDIS_TLS=1
 */
let client: RedisClientType | null = null;
let redisReady = false;

(function initRedis() {
  const url = process.env.REDIS_URL; // e.g. rediss://default:pass@host:port
  const useTlsViaUrl = !!url && url.startsWith('rediss://');
  const useTlsViaEnv = process.env.REDIS_TLS === '1';

  const options: any = {};
  if (url) {
    options.url = url;
  } else {
    const host = process.env.REDIS_HOST || '127.0.0.1';
    const port = Number(process.env.REDIS_PORT || 6379);
    const username = process.env.REDIS_USER;
    const password = process.env.REDIS_PASSWORD;
    options.socket = { host, port, tls: useTlsViaEnv };
    if (username) options.username = username;
    if (password) options.password = password;
  }

  // If URL says rediss:// enforce TLS
  if (useTlsViaUrl) {
    options.socket = options.socket || {};
    options.socket.tls = true;
  }

  client = createClient(options);

  client.on('error', (err) => {
    redisReady = false;
    // Avoid noisy logs in prod; uncomment for debugging:
    // console.error('[Redis] error:', err?.message || err);
  });

  client.on('ready', () => {
    redisReady = true;
    // console.log('[Redis] ready');
  });

  client
    .connect()
    .then(() => {
      // console.log('[Redis] connected');
    })
    .catch(() => {
      // Swallow connect race during boot; interceptor will BYPASS until ready
    });
})();

/**
 * ---- Interceptor ----------------------------------------------------------
 *
 * - GET-only (idempotent)
 * - Honors @CacheTTL(), x-cache-ttl, CACHE_TTL env, capped by CACHE_TTL_MAX
 * - Bypass with ?__nocache=1 or x-cache-bypass: 1
 * - Adds X-Cache: HIT | MISS | BYPASS
 * - Jittered TTL (80%..120%) to reduce cache stampedes
 * - Skips caching if response status != 200 or body too large
 */
@Injectable()
export class RedisCacheInterceptor implements NestInterceptor {
  constructor(private readonly reflector: Reflector) {}

  // Defaults + caps
  private readonly defaultTtlSec = Math.max(
    1,
    Number(process.env.CACHE_TTL || 5),
  );
  private readonly maxTtlSec = Math.max(
    this.defaultTtlSec,
    Number(process.env.CACHE_TTL_MAX || 300),
  );
  private readonly maxCacheBodyBytes = Math.max(
    32 * 1024, // 32 KB
    Number(process.env.CACHE_MAX_BODY_BYTES || 1_500_000), // ~1.5MB default
  );

  async intercept(ctx: ExecutionContext, next: CallHandler): Promise<Observable<any>> {
    const http = ctx.switchToHttp();
    const req = http.getRequest<any>();
    const res = http.getResponse<any>();

    // Opt-out via decorator/metadata
    const bypassMeta =
      !!this.reflector.get<boolean>(CACHE_BYPASS, ctx.getHandler()) ||
      !!this.reflector.get<boolean>(CACHE_BYPASS, ctx.getClass());

    if (bypassMeta) {
      res?.setHeader?.('X-Cache', 'BYPASS');
      return next.handle();
    }

    // Only cache GET
    if (!req || String(req.method).toUpperCase() !== 'GET') {
      res?.setHeader?.('X-Cache', 'BYPASS');
      return next.handle();
    }

    // Bypass via query/header
    const bypass =
      String(req.query?.__nocache || '').toLowerCase() === '1' ||
      String(req.headers?.['x-cache-bypass'] || '').toLowerCase() === '1';
    if (bypass) {
      res?.setHeader?.('X-Cache', 'BYPASS');
      return next.handle();
    }

    // If Redis not ready, bypass
    if (!client || !redisReady) {
      res?.setHeader?.('X-Cache', 'BYPASS');
      return next.handle();
    }

    // Resolve TTL: header > decorator > env default (then cap)
    const headerTtl = Number(req.headers?.['x-cache-ttl'] || NaN);
    const metaTtl = this.reflector.get<number>(CACHE_TTL, ctx.getHandler());
    const baseTtl = Number.isFinite(headerTtl) && headerTtl > 0
      ? headerTtl
      : Number.isFinite(metaTtl) && metaTtl > 0
        ? metaTtl
        : this.defaultTtlSec;

    const ttl = Math.min(baseTtl, this.maxTtlSec);

    // Key
    const key = this.makeKey(req);

    // Try HIT
    try {
      const cached = await client!.get(key);
      if (cached) {
        res?.setHeader?.('X-Cache', 'HIT');
        // Return cached JSON. Ensure we only call JSON.parse on strings.
        if (typeof cached === 'string') {
          return of(JSON.parse(cached));
        }
        return of(cached as any);
      }
    } catch {
      // If Redis read fails, just bypass
      res?.setHeader?.('X-Cache', 'BYPASS');
      return next.handle();
    }

    // MISS -> proceed and cache response (if 200 + within size cap)
    res?.setHeader?.('X-Cache', 'MISS');

    // jitter 80%..120%
    const jitteredTtl = Math.max(
      1,
      Math.round(ttl * (0.8 + Math.random() * 0.4)),
    );

    return next.handle().pipe(
      tap(async (body) => {
        try {
          // Only cache successful JSON-ish bodies
          const statusCode = res?.statusCode ?? 200;
          if (statusCode !== 200) return;

          // Serialize and size-check
          const payload = JSON.stringify(body);
          if (!payload || payload === '{}') return;
          if (Buffer.byteLength(payload) > this.maxCacheBodyBytes) return;

          await client!.setEx(key, jitteredTtl, payload);
        } catch {
          // Swallow cache write errors
        }
      }),
    );
  }

  /** Stable cache key from method + full URL (+ optional vary header) */
  private makeKey(req: any): string {
    const method = String(req.method || 'GET').toUpperCase();
    const url =
      req.originalUrl ||
      req.url ||
      (req.path ? `${req.path}${req._parsedUrl?.search || ''}` : '');

    // Optional extra vary dimension via header (e.g., userId)
    const varyExtra = String(req.headers?.['x-cache-vary'] || '');

    const raw = `${method}:${url}${varyExtra ? `:${varyExtra}` : ''}`;
    const h = crypto.createHash('sha1').update(raw).digest('hex');
    return `api:${h}`;
  }
}
