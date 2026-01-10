import { WebSocketServer, WebSocket } from 'ws';
import { createClient } from 'redis';
import { newRedisSubscriber } from '../lib/redis.js';

const WS_PATH = process.env.WS_PATH || '/ws';

const TRADES_CHANNEL = process.env.RT_TRADES_CHANNEL || 'rt:trades';
const CANDLES_CHANNEL = process.env.RT_CANDLES_CHANNEL || 'rt:candles';

const SEND_SNAPSHOT = (process.env.RT_WS_SEND_SNAPSHOT || '1') === '1';
const SNAPSHOT_LIMIT = Number(process.env.RT_WS_SNAPSHOT_LIMIT || 50);

function safeJsonParse(s: string) {
  try {
    return JSON.parse(s);
  } catch {
    return null;
  }
}

function wsSend(ws: WebSocket, obj: any) {
  if (!ws || ws.readyState !== ws.OPEN) return;
  try {
    ws.send(JSON.stringify(obj));
  } catch {}
}

function normalizePoolId(v: any) {
  if (v == null) return null;
  const n = typeof v === 'number' ? v : Number(String(v));
  return Number.isFinite(n) ? String(n) : null;
}

function normalizePair(v: any) {
  if (!v) return null;
  return String(v).trim();
}

function normalizeTokenId(v: any) {
  if (!v) return null;
  return String(v).trim().toLowerCase();
}

function tokenRoomVariants(denom: any) {
  if (!denom) return [];
  const raw = String(denom).trim();
  const lower = raw.toLowerCase();
  const out = new Set<string>();

  out.add(`token:${lower}`);
  out.add(`token:${raw}`);

  const cleaned = lower.replace(/^ibc\/[a-f0-9]+\//i, '');
  out.add(`token:${cleaned}`);

  return Array.from(out);
}

export function startWS(httpServer: any, { path = WS_PATH } = {}) {
  const wss = new WebSocketServer({ noServer: true });

  const rooms = new Map<string, Set<WebSocket>>();
  const socketRooms = new Map<WebSocket, Set<string>>();

  const addToRoom = (ws: WebSocket, roomKey: string | null) => {
    if (!roomKey) return;
    if (!rooms.has(roomKey)) rooms.set(roomKey, new Set());
    rooms.get(roomKey)!.add(ws);

    if (!socketRooms.has(ws)) socketRooms.set(ws, new Set());
    socketRooms.get(ws)!.add(roomKey);
  };

  const removeFromRoom = (ws: WebSocket, roomKey: string) => {
    const set = rooms.get(roomKey);
    if (set) {
      set.delete(ws);
      if (set.size === 0) rooms.delete(roomKey);
    }
    const rs = socketRooms.get(ws);
    if (rs) {
      rs.delete(roomKey);
      if (rs.size === 0) socketRooms.delete(ws);
    }
  };

  const removeFromAllRooms = (ws: WebSocket) => {
    const rs = socketRooms.get(ws);
    if (!rs) return;
    for (const k of Array.from(rs)) removeFromRoom(ws, k);
  };

  const broadcastRoom = (roomKey: string, payload: any) => {
    const set = rooms.get(roomKey);
    if (!set || set.size === 0) return;

    const msg = JSON.stringify(payload);
    for (const ws of set) {
      if (ws.readyState === ws.OPEN) {
        try {
          ws.send(msg);
        } catch {}
      }
    }
  };

  let subClient: any = null;

  async function ensureSubscriber() {
    // Try redis.js helper first
    if (typeof newRedisSubscriber === 'function') {
      try {
        subClient = await newRedisSubscriber();
        subClient.on('error', (e: any) => console.warn('[ws][redis-sub] error', e?.message || e));
        return subClient;
      } catch (e) {
        console.warn('[ws] redis subscriber (helper) failed', e?.message || e);
        subClient = null;
      }
    }

    const url = process.env.REDIS_URL || '';
    if (!url) return null;
    const c = createClient({ url });
    c.on('error', (e) => console.warn('[ws][redis-sub] error', e?.message || e));
    try {
      await c.connect();
      subClient = c;
      console.info('[ws] redis subscriber connected (manual)');
      return subClient;
    } catch (e) {
      console.warn('[ws] redis subscriber connect failed', e?.message || e);
      try { await c.quit(); } catch {}
      subClient = null;
      return null;
    }
  }

  async function startRedisFanout() {
    const c = await ensureSubscriber();
    if (!c) {
      console.warn('[ws] redis disabled; realtime WS fanout will not work');
      return;
    }

    // Handle both node-redis v4 (subscribe(channel, handler)) and ioredis (subscribe + "message" event)
    const onTrade = (message: string) => {
      const payload = safeJsonParse(message);
      if (!payload) return;

      broadcastRoom('stream:trades', payload);

      const poolId = normalizePoolId(payload.pool_id ?? payload?.data?.pool_id);
      const pair = normalizePair(payload.pair_contract ?? payload?.data?.pair_contract);

      if (poolId) broadcastRoom(`pool:${poolId}`, payload);
      if (pair) broadcastRoom(`pair:${pair}`, payload);

      const offer = payload?.data?.offer_asset_denom;
      const ask = payload?.data?.ask_asset_denom;
      for (const rk of tokenRoomVariants(offer)) broadcastRoom(rk, payload);
      for (const rk of tokenRoomVariants(ask)) broadcastRoom(rk, payload);
    };

    if (c.subscribe.length >= 2) {
      // node-redis v4 style
      await c.subscribe(TRADES_CHANNEL, onTrade);
    } else {
      // ioredis style
      await c.subscribe(TRADES_CHANNEL);
      c.on('message', (channel: string, message: string) => {
        if (channel === TRADES_CHANNEL) onTrade(message);
      });
    }
    console.info('[ws] redis fanout subscribed', { channel: TRADES_CHANNEL });

    const onCandle = (message: string) => {
      const payload = safeJsonParse(message);
      if (!payload) return;

      const tf = String(payload.tf || '1m');
      const unit = String(payload.unit || 'zig');
      const poolId = normalizePoolId(payload.pool_id ?? payload?.data?.pool_id);
      const pair = normalizePair(payload.pair_contract);

      if (poolId) broadcastRoom(`candle:pool:${poolId}:${tf}:${unit}`, payload);
      if (pair) broadcastRoom(`candle:pair:${pair}:${tf}:${unit}`, payload);
    };

    if (c.subscribe.length >= 2) {
      await c.subscribe(CANDLES_CHANNEL, onCandle);
    } else {
      await c.subscribe(CANDLES_CHANNEL);
      c.on('message', (channel: string, message: string) => {
        if (channel === CANDLES_CHANNEL) onCandle(message);
      });
    }
    console.info('[ws] redis fanout subscribed', { channel: CANDLES_CHANNEL });
  }

  httpServer.on('upgrade', (req: any, socket: any, head: any) => {
    const { url } = req;
    if (!url || !url.startsWith(path)) return;

    wss.handleUpgrade(req, socket, head, (ws) => {
      wss.emit('connection', ws, req);
    });
  });

  wss.on('connection', (ws) => {
    wsSend(ws, { type: 'hello', ok: true });

    ws.on('message', async (raw) => {
      const msg = safeJsonParse(String(raw));
      if (!msg) return;

      if (msg.type === 'ping') {
        wsSend(ws, { type: 'pong', ts: Date.now() });
        return;
      }

      const stream = String(msg.stream || '');

      if (msg.type === 'sub' && stream === 'trades') {
        const poolId = normalizePoolId(msg.pool_id);
        const pair = normalizePair(msg.pair_contract);
        const tokenId = normalizeTokenId(msg.token_id || msg.tokenId || msg.denom);

        addToRoom(ws, 'stream:trades');
        if (poolId) addToRoom(ws, `pool:${poolId}`);
        if (pair) addToRoom(ws, `pair:${pair}`);
        if (tokenId) addToRoom(ws, `token:${tokenId}`);

        wsSend(ws, {
          type: 'sub:ok',
          stream: 'trades',
          pool_id: poolId,
          pair_contract: pair,
          token_id: tokenId,
        });

        if (SEND_SNAPSHOT && poolId) {
          try {
            const r = await (typeof newRedisSubscriber === 'function' ? newRedisSubscriber() : null);
            if (r) {
              const key = `rt:cache:trades:pool:${poolId}`;
              // Cast because different redis clients expose either lRange or lrange in typings
              const rows =
                typeof (r as any).lRange === 'function'
                  ? await (r as any).lRange(
                      key,
                      0,
                      Math.max(0, SNAPSHOT_LIMIT - 1)
                    )
                  : await (r as any).lrange(
                      key,
                      0,
                      Math.max(0, SNAPSHOT_LIMIT - 1)
                    );
              const parsed = rows.map(safeJsonParse).filter(Boolean);
              if (parsed.length) {
                wsSend(ws, {
                  type: 'snapshot',
                  stream: 'trades',
                  pool_id: poolId,
                  data: parsed.reverse(),
                });
              }
            }
          } catch {}
        }

        return;
      }

      if (msg.type === 'unsub' && stream === 'trades') {
        const poolId = normalizePoolId(msg.pool_id);
        const pair = normalizePair(msg.pair_contract);
        const tokenId = normalizeTokenId(msg.token_id || msg.tokenId || msg.denom);

        if (poolId) removeFromRoom(ws, `pool:${poolId}`);
        if (pair) removeFromRoom(ws, `pair:${pair}`);
        if (tokenId) removeFromRoom(ws, `token:${tokenId}`);

        if (msg.all === true) removeFromRoom(ws, 'stream:trades');

        wsSend(ws, {
          type: 'unsub:ok',
          stream: 'trades',
          pool_id: poolId,
          pair_contract: pair,
          token_id: tokenId,
        });
        return;
      }

      if (msg.type === 'sub' && stream === 'candles') {
        const tf = String(msg.tf || '1m');
        const unit = String(msg.unit || 'zig');
        const poolId = normalizePoolId(msg.pool_id);
        const pair = normalizePair(msg.pair_contract);

        if (poolId) addToRoom(ws, `candle:pool:${poolId}:${tf}:${unit}`);
        if (pair) addToRoom(ws, `candle:pair:${pair}:${tf}:${unit}`);

        wsSend(ws, {
          type: 'sub:ok',
          stream: 'candles',
          tf,
          unit,
          pool_id: poolId,
          pair_contract: pair,
        });
        return;
      }

      if (msg.type === 'unsub' && stream === 'candles') {
        const tf = String(msg.tf || '1m');
        const unit = String(msg.unit || 'zig');
        const poolId = normalizePoolId(msg.pool_id);
        const pair = normalizePair(msg.pair_contract);

        if (poolId) removeFromRoom(ws, `candle:pool:${poolId}:${tf}:${unit}`);
        if (pair) removeFromRoom(ws, `candle:pair:${pair}:${tf}:${unit}`);

        wsSend(ws, {
          type: 'unsub:ok',
          stream: 'candles',
          tf,
          unit,
          pool_id: poolId,
          pair_contract: pair,
        });
        return;
      }
    });

    ws.on('close', () => removeFromAllRooms(ws));
    ws.on('error', () => removeFromAllRooms(ws));
  });

  startRedisFanout().catch((e) =>
    console.warn('[ws] startRedisFanout failed', e?.message || e)
  );

  return wss;
}
