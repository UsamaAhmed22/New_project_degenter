import Redis from 'ioredis';
import { warn, info } from './log.js';

function buildOptions() {
  if (process.env.REDIS_URL) return process.env.REDIS_URL;
  return {
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: Number(process.env.REDIS_PORT || 6379),
    password: process.env.REDIS_PASSWORD || undefined,
    db: Number(process.env.REDIS_DB || 0),
  };
}

let redisSingleton;
export function getRedis() {
  if (!redisSingleton) {
    redisSingleton = new Redis(buildOptions());
    redisSingleton.on('error', (e) => warn('[redis] error', e.message));
    redisSingleton.on('connect', () => info('[redis] connected'));
  }
  return redisSingleton;
}

export function newRedisSubscriber() {
  const sub = new Redis(buildOptions());
  sub.on('error', (e) => warn('[redis/sub] error', e.message));
  return sub;
}

export async function publishJson(channel, payload) {
  const client = getRedis();
  try {
    await client.publish(channel, JSON.stringify(payload));
  } catch (e) {
    warn('[redis publish]', channel, e.message);
  }
}

export async function xadd(stream, obj) {
  const client = getRedis();
  const fields = [];
  for (const [k, v] of Object.entries(obj)) {
    fields.push(k, v == null ? '' : String(v));
  }
  try {
    await client.xadd(stream, '*', ...fields);
  } catch (e) {
    warn('[redis xadd]', stream, e.message);
  }
}
