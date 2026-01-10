import { Injectable } from '@nestjs/common';
import { PgService } from '../db/pg.service';
import { QueryResultRow } from 'pg';

/** NOTE: This service ports your Express /trades logic nearly verbatim.
 *  - Same helpers (minutesForTf, buildWindow, buildWhereBase, builders)
 *  - Same shaping and combineRouter (shallow+deep)
 *  - Same endpoints behavior/params and meta
 */

type Dir = 'buy'|'sell'|'provide'|'withdraw'|null;
const VALID_DIR = new Set(['buy','sell','provide','withdraw']);
const VALID_CLASS = new Set(['shrimp','shark','whale']);

@Injectable()
export class TradesService {
  constructor(private readonly pg: PgService) {}

  // ---------------- helpers (ported 1:1) ----------------
  private normDir(d: any): Dir {
    const x = String(d || '').toLowerCase();
    return VALID_DIR.has(x) ? (x as Dir) : null;
  }
  private parseLimit(q: any) {
    const valid = new Set([100, 500, 1000]);
    const n = Number(q);
    return valid.has(n) ? n : 100;
  }
  private parsePage(q: any) {
    const n = Number(q);
    return Number.isFinite(n) && n >= 1 ? Math.floor(n) : 1;
  }
  private minutesForTf(tf?: string) {
    const m = String(tf || '').toLowerCase();
    const d = m.match(/^(\d+)d$/);
    if (d) return Number(d[1]) * 1440;
    const map: Record<string, number> = {
      '30m':30, '1h':60, '2h':120, '4h':240, '8h':480, '12h':720,
      '24h':1440, '1d':1440, '3d':4320, '5d':7200, '7d':10080, '14d':20160, '30d':43200, '60d':86400
    };
    return map[m] || 1440;
  }
  private buildWindow({ tf, from, to, days }: any, params: any[], alias = 't') {
    const clauses: string[] = [];
    if (from && to) {
      clauses.push(`${alias}.created_at >= $${params.length + 1}::timestamptz`); params.push(from);
      clauses.push(`${alias}.created_at <  $${params.length + 1}::timestamptz`); params.push(to);
      return { clause: clauses.join(' AND ') };
    }
    if (days) {
      clauses.push(`${alias}.created_at >= now() - ($${params.length + 1} || ' days')::interval`);
      params.push(String(days));
      return { clause: clauses.join(' AND ') };
    }
    const mins = this.minutesForTf(tf);
    clauses.push(`${alias}.created_at >= now() - INTERVAL '${mins} minutes'`);
    return { clause: clauses.join(' AND ') };
  }
  private tradesFromJoin(alias = 't') {
    return `
      FROM trades ${alias}
      JOIN pools  p ON p.pool_id = ${alias}.pool_id
      JOIN tokens q ON q.token_id = p.quote_token_id
      JOIN tokens b ON b.token_id = p.base_token_id
      LEFT JOIN tokens toff ON toff.denom = ${alias}.offer_asset_denom
      LEFT JOIN tokens task ON task.denom = ${alias}.ask_asset_denom
    `;
  }
  private buildWhereBase({ scope, scopeValue, direction, includeLiquidity }: any, params: any[], alias='t') {
    const where: string[] = [];
    if (includeLiquidity) where.push(`${alias}.action IN ('swap','provide','withdraw')`);
    else where.push(`${alias}.action = 'swap'`);
    if (direction) { where.push(`${alias}.direction = $${params.length + 1}`); params.push(direction); }
    if (scope === 'token') { where.push(`b.token_id = $${params.length + 1}`); params.push(scopeValue); }
    else if (scope === 'wallet') { where.push(`${alias}.signer = $${params.length + 1}`); params.push(scopeValue); }
    else if (scope === 'pool') {
      if (scopeValue.poolId) { where.push(`p.pool_id = $${params.length + 1}`); params.push(scopeValue.poolId); }
      else if (scopeValue.pairContract) { where.push(`p.pair_contract = $${params.length + 1}`); params.push(scopeValue.pairContract); }
    }
    return where;
  }
  private async zigUsd() {
    const r = await this.pg.query(`SELECT zig_usd FROM exchange_rates ORDER BY ts DESC LIMIT 1`);
    return r.rows[0]?.zig_usd ? Number(r.rows[0].zig_usd) : 0;
  }
  private scale(base: any, exp: any, fallback=6) {
    if (base == null) return null;
    const e = (exp == null ? fallback : Number(exp));
    return Number(base) / 10 ** e;
  }
  private shapeRow(r: any, unit: 'usd'|'zig', zigUsd: number) {
    const fx = r.fx_zig_usd != null ? Number(r.fx_zig_usd) : zigUsd;

    const offerScaled = this.scale(r.offer_amount_base, (r.offer_asset_denom === 'uzig') ? 6 : (r.offer_exp ?? 6), 6);
    const askScaled   = this.scale(r.ask_amount_base,   (r.ask_asset_denom   === 'uzig') ? 6 : (r.ask_exp   ?? 6), 6);

    const returnAsQuote = this.scale(r.return_amount_base, r.qexp ?? 6, 6);
    const returnAsBase  = this.scale(r.return_amount_base, r.bexp ?? 6, 6);

    let valueZig: number|null = null;
    if (r.is_uzig_quote) {
      valueZig = (r.direction === 'buy')
        ? this.scale(r.offer_amount_base, r.qexp ?? 6, 6)
        : this.scale(r.return_amount_base, r.qexp ?? 6, 6);
    } else if (r.pq_price_in_zig != null) {
      const rawQuote = (r.direction === 'buy')
        ? this.scale(r.offer_amount_base, r.qexp ?? 6, 6)
        : this.scale(r.return_amount_base, r.qexp ?? 6, 6);
      if (rawQuote != null) valueZig = rawQuote * Number(r.pq_price_in_zig);
    }
    const valueUsd = valueZig != null ? valueZig * fx : null;

    let quoteAmtZig: number|null = null;
    if (r.is_uzig_quote) {
      quoteAmtZig = (r.direction === 'buy')
        ? this.scale(r.offer_amount_base, r.qexp ?? 6, 6)
        : this.scale(r.return_amount_base, r.qexp ?? 6, 6);
    } else if (r.pq_price_in_zig != null) {
      const rawQuote = (r.direction === 'buy')
        ? this.scale(r.offer_amount_base, r.qexp ?? 6, 6)
        : this.scale(r.return_amount_base, r.qexp ?? 6, 6);
      if (rawQuote != null) quoteAmtZig = rawQuote * Number(r.pq_price_in_zig);
    }
    const baseAmt = (r.direction === 'buy')
      ? returnAsBase
      : (r.direction === 'sell')
        ? this.scale(r.offer_amount_base, r.bexp ?? 6, 6)
        : null;

    const priceNative = (quoteAmtZig != null && baseAmt != null && baseAmt !== 0) ? (quoteAmtZig / baseAmt) : null;
    const priceUsd    = priceNative != null ? priceNative * fx : null;

    const zigLegAmount =
      (r.offer_asset_denom === 'uzig' && offerScaled != null) ? offerScaled :
      (r.ask_asset_denom   === 'uzig' && askScaled   != null) ? askScaled   :
      null;

    return {
      time: r.created_at,
      txHash: r.tx_hash,
      pairContract: r.pair_contract,
      signer: r.signer,
      direction: r.direction,
      is_router: r.is_router === true,

      offerDenom: r.offer_asset_denom,
      offerAmountBase: r.offer_amount_base,
      offerAmount: offerScaled,

      askDenom: r.ask_asset_denom,
      askAmountBase: r.ask_amount_base,
      askAmount: askScaled,

      returnAmountBase: r.return_amount_base,
      returnAmount: (r.direction === 'buy') ? returnAsBase : returnAsQuote,

      priceNative,
      priceUsd,

      valueNative: valueZig,
      valueUsd,

      zigLegAmount,
      zig_usd_at_trade: fx
    } as any;
  }
  private worthForClass(item: any, unit: 'usd'|'zig', zigUsd: number) {
    const zigBasis = (item.zigLegAmount != null) ? item.zigLegAmount : item.valueNative;
    if (zigBasis == null) return null;
    const fx = item.zig_usd_at_trade != null ? Number(item.zig_usd_at_trade) : zigUsd;
    return unit === 'usd' ? zigBasis * fx : zigBasis;
  }
  private classifyByThreshold(x: number) {
    if (x < 1000) return 'shrimp';
    if (x <= 10000) return 'shark';
    return 'whale';
  }

  // ---- SQL builder for paged worth/class (same idea as Express) ----
  private buildWorthPagedSQL(opts: any, params: any[]) {
    const { scope, scopeValue, direction, includeLiquidity, windowOpts,
            page, limit, unit, klass, minValue, maxValue, extraWhere=[] } = opts;

    const baseWhere = this.buildWhereBase({ scope, scopeValue, direction, includeLiquidity }, params, 't');
    if (Array.isArray(extraWhere) && extraWhere.length) baseWhere.push(...extraWhere);

    const { clause: timeClause } = this.buildWindow(windowOpts, params, 't');
    baseWhere.push(timeClause);

    const fromJoin = this.tradesFromJoin('t');

    const worthZig = `
      COALESCE(
        CASE WHEN base.offer_asset_denom='uzig'
             THEN base.offer_amount_base / POWER(10, COALESCE(base.offer_exp,6))
             WHEN base.ask_asset_denom='uzig'
             THEN base.ask_amount_base   / POWER(10, COALESCE(base.ask_exp,6))
        END,
        CASE WHEN base.is_uzig_quote THEN
               CASE WHEN base.direction='buy'
                    THEN base.offer_amount_base  / POWER(10, COALESCE(base.qexp,6))
                    ELSE base.return_amount_base / POWER(10, COALESCE(base.qexp,6))
               END
             ELSE
               (CASE WHEN base.direction='buy'
                     THEN base.offer_amount_base  / POWER(10, COALESCE(base.qexp,6))
                     ELSE base.return_amount_base / POWER(10, COALESCE(base.qexp,6))
                END) * base.pq_price_in_zig
        END
      )
    `;

    const zigUsdIdx = params.length + 1;
    params.push(0); // placeholder; caller fills with zigUsd
    const worthUsd = `(${worthZig}) * COALESCE(base.fx_zig_usd, $${zigUsdIdx})`;

    const filters: string[] = [];
    const k = String(klass || '').toLowerCase();
    if (VALID_CLASS.has(k)) {
      if (k === 'shrimp') filters.push(unit === 'zig' ? `${worthZig} < 1000` : `${worthUsd} < 1000`);
      if (k === 'shark')  filters.push(unit === 'zig' ? `${worthZig} >= 1000 AND ${worthZig} <= 10000`
                                                      : `${worthUsd} >= 1000 AND ${worthUsd} <= 10000`);
      if (k === 'whale')  filters.push(unit === 'zig' ? `${worthZig} > 10000` : `${worthUsd} > 10000`);
    }
    if (minValue != null) filters.push(unit === 'zig' ? `${worthZig} >= ${Number(minValue)}` : `${worthUsd} >= ${Number(minValue)}`);
    if (maxValue != null) filters.push(unit === 'zig' ? `${worthZig} <= ${Number(maxValue)}` : `${worthUsd} <= ${Number(maxValue)}`);

    const offset = (page - 1) * limit;

    const sql = `
      WITH base AS (
        SELECT
          t.*,
          p.pair_contract,
          p.is_uzig_quote,
          q.exponent AS qexp,
          b.exponent AS bexp,
          b.denom    AS base_denom,
          toff.exponent AS offer_exp,
          task.exponent AS ask_exp,
          fx.zig_usd_at_trade AS fx_zig_usd,
          (SELECT price_in_zig FROM prices WHERE token_id = p.quote_token_id ORDER BY updated_at DESC LIMIT 1) AS pq_price_in_zig
        ${fromJoin}
        LEFT JOIN LATERAL (
          SELECT zig_usd AS zig_usd_at_trade
          FROM exchange_rates
          WHERE ts <= t.created_at
          ORDER BY ts DESC
          LIMIT 1
        ) fx ON TRUE
        WHERE ${baseWhere.join(' AND ')}
      ),
      ranked AS (
        SELECT base.*,
               ${worthZig} AS worth_zig,
               ${worthUsd} AS worth_usd,
               COUNT(*) OVER() AS total
        FROM base
        ${filters.length ? `WHERE ${filters.join(' AND ')}` : ''}
      )
      SELECT * FROM ranked
      ORDER BY created_at DESC
      LIMIT ${limit} OFFSET ${offset}
    `;
    return { sql, params, zigUsdIdx };
  }

  // ---------------- endpoints ----------------

  async getAll(q: any) {
    const unit = (q.unit || 'usd').toLowerCase();
    const limit = this.parseLimit(q.limit);
    const page  = this.parsePage(q.page);
    const dir   = this.normDir(q.direction);
    const includeLiquidity = q.includeLiquidity === '1' || q.includeLiquidity === 'true';
    const klass = q.class || undefined;
    const minV  = q.minValue != null ? Number(q.minValue) : null;
    const maxV  = q.maxValue != null ? Number(q.maxValue) : null;
    const zigUsd = await this.zigUsd();
    const windowOpts = { tf:q.tf, from:q.from, to:q.to, days:q.days };

    const params: any[] = [];
    const { sql, params: p2, zigUsdIdx } = this.buildWorthPagedSQL({
      scope:'all', scopeValue:null, direction:dir, includeLiquidity,
      windowOpts, page, limit, unit, klass, minValue:minV, maxValue:maxV, extraWhere:[]
    }, params);
    p2[zigUsdIdx - 1] = zigUsd;

    const { rows } = await this.pg.query(sql, p2);
    const total = rows[0]?.total ? Number(rows[0].total) : 0;
    const pages = Math.max(1, Math.ceil(total / limit));
    const shaped = rows.map(r => {
      const s = this.shapeRow(r, unit, zigUsd);
      const w = this.worthForClass(s, unit, zigUsd);
      (s as any).class = w != null ? this.classifyByThreshold(w) : null;
      return s;
    });

    return { success:true, data: shaped, meta:{ unit, tf:q.tf || '24h', limit, page, pages, total } };
  }

  async getByToken(id: string, q: any) {
    const tok = await this.resolveTokenId(id);
    if (!tok) return { success:false, error:'token not found' };

    const unit = (q.unit || 'usd').toLowerCase();
    const limit = this.parseLimit(q.limit);
    const page  = this.parsePage(q.page);
    const dir   = this.normDir(q.direction);
    const includeLiquidity = q.includeLiquidity === '1' || q.includeLiquidity === 'true';
    const klass = q.class || undefined;
    const minV  = q.minValue != null ? Number(q.minValue) : null;
    const maxV  = q.maxValue != null ? Number(q.maxValue) : null;
    const zigUsd = await this.zigUsd();
    const windowOpts = { tf:q.tf, from:q.from, to:q.to, days:q.days };

    const params: any[] = [];
    const { sql, params: p2, zigUsdIdx } = this.buildWorthPagedSQL({
      scope:'token', scopeValue: tok.token_id, direction:dir, includeLiquidity,
      windowOpts, page, limit, unit, klass, minValue:minV, maxValue:maxV, extraWhere:[]
    }, params);
    p2[zigUsdIdx - 1] = zigUsd;

    const { rows } = await this.pg.query(sql, p2);
    const total = rows[0]?.total ? Number(rows[0].total) : 0;
    const pages = Math.max(1, Math.ceil(total / limit));
    const shaped = rows.map(r => {
      const s = this.shapeRow(r, unit, zigUsd);
      const w = this.worthForClass(s, unit, zigUsd);
      (s as any).class = w != null ? this.classifyByThreshold(w) : null;
      return s;
    });

    return { success:true, data: shaped, meta:{ unit, tf:q.tf || '24h', limit, page, pages, total } };
  }

  async getByPool(ref: string, q: any) {
    const row = await this.pg.query(`SELECT pool_id FROM pools WHERE pair_contract=$1 OR pool_id::text=$1 LIMIT 1`, [ref]);
    if (!row.rows.length) return { success:false, error:'pool not found' };
    const poolId = row.rows[0].pool_id;

    const unit = (q.unit || 'usd').toLowerCase();
    const limit = this.parseLimit(q.limit);
    const page  = this.parsePage(q.page);
    const dir   = this.normDir(q.direction);
    const includeLiquidity = q.includeLiquidity === '1' || q.includeLiquidity === 'true';
    const klass = q.class || undefined;
    const minV  = q.minValue != null ? Number(q.minValue) : null;
    const maxV  = q.maxValue != null ? Number(q.maxValue) : null;
    const zigUsd = await this.zigUsd();
    const windowOpts = { tf:q.tf, from:q.from, to:q.to, days:q.days };

    const params: any[] = [];
    const { sql, params: p2, zigUsdIdx } = this.buildWorthPagedSQL({
      scope:'pool', scopeValue: { poolId }, direction:dir, includeLiquidity,
      windowOpts, page, limit, unit, klass, minValue:minV, maxValue:maxV, extraWhere:[]
    }, params);
    p2[zigUsdIdx - 1] = zigUsd;

    const { rows } = await this.pg.query(sql, p2);
    const total = rows[0]?.total ? Number(rows[0].total) : 0;
    const pages = Math.max(1, Math.ceil(total / limit));
    const shaped = rows.map(r => {
      const s = this.shapeRow(r, unit, zigUsd);
      const w = this.worthForClass(s, unit, zigUsd);
      (s as any).class = w != null ? this.classifyByThreshold(w) : null;
      return s;
    });

    return { success:true, data: shaped, meta:{ unit, tf:q.tf || '24h', limit, page, pages, total } };
  }

  async getByWallet(address: string, q: any) {
    const unit = (q.unit || 'usd').toLowerCase();
    const limit = this.parseLimit(q.limit);
    const page  = this.parsePage(q.page);
    const dir   = this.normDir(q.direction);
    const includeLiquidity = q.includeLiquidity === '1' || q.includeLiquidity === 'true';
    const klass = q.class || undefined;
    const minV  = q.minValue != null ? Number(q.minValue) : null;
    const maxV  = q.maxValue != null ? Number(q.maxValue) : null;
    const zigUsd = await this.zigUsd();
    const windowOpts = { tf:q.tf, from:q.from, to:q.to, days:q.days };

    const extraWhere: string[] = [];
    if (q.tokenId) {
      const tok = await this.resolveTokenId(q.tokenId);
      if (tok) extraWhere.push(`b.token_id = ${tok.token_id}`);
    }
    if (q.pair) {
      extraWhere.push(`p.pair_contract = '${String(q.pair).replace(/'/g,"''")}'`);
    } else if (q.poolId) {
      extraWhere.push(`p.pool_id = ${Number(q.poolId)}`);
    }

    const params: any[] = [];
    const { sql, params: p2, zigUsdIdx } = this.buildWorthPagedSQL({
      scope:'wallet', scopeValue: address, direction:dir, includeLiquidity,
      windowOpts, page, limit, unit, klass, minValue:minV, maxValue:maxV, extraWhere
    }, params);
    p2[zigUsdIdx - 1] = zigUsd;

    const { rows } = await this.pg.query(sql, p2);
    const total = rows[0]?.total ? Number(rows[0].total) : 0;
    const pages = Math.max(1, Math.ceil(total / limit));
    const shaped = rows.map(r => {
      const s = this.shapeRow(r, unit, zigUsd);
      const w = this.worthForClass(s, unit, zigUsd);
      (s as any).class = w != null ? this.classifyByThreshold(w) : null;
      return s;
    });

    return { success:true, data: shaped, meta:{ unit, tf:q.tf || '24h', limit, page, pages, total } };
  }

  async getLarge(q: any) {
    // Kept simple: DB pick from large_trades → hydrate → worth/class → page (same as Express)
    const bucket = (q.bucket || '24h').toLowerCase();
    const unit = (q.unit || 'zig').toLowerCase();
    const limit = this.parseLimit(q.limit);
    const page  = this.parsePage(q.page);
    const dir   = this.normDir(q.direction);
    const klass = q.class || undefined;
    const minV  = q.minValue != null ? Number(q.minValue) : null;
    const maxV  = q.maxValue != null ? Number(q.maxValue) : null;

    const zigUsd = await this.zigUsd();
    const params: any[] = [bucket];
    let dirClause = '';
    if (dir) { params.push(dir); dirClause = `AND lt.direction = $${params.length}`; }

    const offset = (page - 1) * limit;
    const zigUsdIdx = params.length + 1; params.push(zigUsd);

    const worthZig = `
      COALESCE(
        CASE WHEN t.offer_asset_denom='uzig'
             THEN t.offer_amount_base / POWER(10, COALESCE(toff.exponent,6))
             WHEN t.ask_asset_denom='uzig'
             THEN t.ask_amount_base   / POWER(10, COALESCE(task.exponent,6))
        END,
        CASE WHEN p.is_uzig_quote THEN
               CASE WHEN t.direction='buy'
                    THEN t.offer_amount_base  / POWER(10, COALESCE(q.exponent,6))
                    ELSE t.return_amount_base / POWER(10, COALESCE(q.exponent,6))
               END
             ELSE
               (CASE WHEN t.direction='buy'
                     THEN t.offer_amount_base  / POWER(10, COALESCE(q.exponent,6))
                     ELSE t.return_amount_base / POWER(10, COALESCE(q.exponent,6))
                END) * (SELECT price_in_zig FROM prices WHERE token_id=p.quote_token_id ORDER BY updated_at DESC LIMIT 1)
        END
      )
    `;
    const worthUsd = `(${worthZig}) * $${zigUsdIdx}`;

    const filters: string[] = [];
    if (VALID_CLASS.has(klass)) {
      if (klass === 'shrimp') filters.push(unit === 'zig' ? `${worthZig} < 1000` : `${worthUsd} < 1000`);
      if (klass === 'shark')  filters.push(unit === 'zig' ? `${worthZig} >= 1000 AND ${worthZig} <= 10000` : `${worthUsd} >= 1000 AND ${worthUsd} <= 10000`);
      if (klass === 'whale')  filters.push(unit === 'zig' ? `${worthZig} > 10000` : `${worthUsd} > 10000`);
    }
    if (minV != null) filters.push(unit === 'zig' ? `${worthZig} >= ${Number(minV)}` : `${worthUsd} >= ${Number(minV)}`);
    if (maxV != null) filters.push(unit === 'zig' ? `${worthZig} <= ${Number(maxV)}` : `${worthUsd} <= ${Number(maxV)}`);

    const sql = `
      WITH pick AS (
        SELECT DISTINCT ON (lt.tx_hash, lt.pool_id, lt.direction)
               lt.tx_hash, lt.pool_id, lt.direction, lt.value_zig, lt.created_at
        FROM large_trades lt
        WHERE lt.bucket = $1 ${dir ? dirClause : ''}
        ORDER BY lt.tx_hash, lt.pool_id, lt.direction, lt.created_at DESC
      ),
      base AS (
        SELECT
          t.*,
          p.pair_contract,
          p.is_uzig_quote,
          q.exponent AS qexp,
          b.exponent AS bexp,
          b.denom    AS base_denom,
          toff.exponent AS offer_exp,
          task.exponent AS ask_exp,
          (SELECT price_in_zig FROM prices WHERE token_id = p.quote_token_id ORDER BY updated_at DESC LIMIT 1) AS pq_price_in_zig
        FROM trades t
        JOIN pick k ON k.tx_hash = t.tx_hash AND k.pool_id = t.pool_id AND k.direction = t.direction
        JOIN pools  p ON p.pool_id = t.pool_id
        JOIN tokens q ON q.token_id = p.quote_token_id
        JOIN tokens b ON b.token_id = p.base_token_id
        LEFT JOIN tokens toff ON toff.denom = t.offer_asset_denom
        LEFT JOIN tokens task ON task.denom = t.ask_asset_denom
      ),
      ranked AS (
        SELECT base.*,
               ${worthZig.split('t.').join('base.')} AS worth_zig,
               (${worthZig.split('t.').join('base.')}) * $${zigUsdIdx} AS worth_usd,
               COUNT(*) OVER() AS total
        FROM base
        ${filters.length ? `WHERE ${filters.map(f => f.split('t.').join('base.')).join(' AND ')}` : ''}
        ORDER BY base.created_at DESC
        LIMIT ${limit} OFFSET ${offset}
      )
      SELECT * FROM ranked
    `;
    const { rows } = await this.pg.query(sql, params);
    const total = rows[0]?.total ? Number(rows[0].total) : 0;
    const pages = Math.max(1, Math.ceil(total / limit));
    const shaped = rows.map(r => {
      const s = this.shapeRow(r, unit, zigUsd);
      const w = this.worthForClass(s, unit, zigUsd);
      (s as any).class = w != null ? this.classifyByThreshold(w) : null;
      return s;
    });
    return { success:true, data: shaped, meta:{ unit, tf: bucket, limit, page, pages, total } };
  }

  async getRecent(q: any) {
    // identical to getAll but allows tokenId/pair/poolId extraWhere
    const unit = (q.unit || 'usd').toLowerCase();
    const limit = this.parseLimit(q.limit);
    const page  = this.parsePage(q.page);
    const dir   = this.normDir(q.direction);
    const includeLiquidity = q.includeLiquidity === '1' || q.includeLiquidity === 'true';
    const klass = q.class || undefined;
    const minV  = q.minValue != null ? Number(q.minValue) : null;
    const maxV  = q.maxValue != null ? Number(q.maxValue) : null;
    const zigUsd = await this.zigUsd();
    const windowOpts = { tf:q.tf, from:q.from, to:q.to, days:q.days };

    const extraWhere: string[] = [];
    if (q.tokenId) {
      const tok = await this.resolveTokenId(q.tokenId);
      if (tok) extraWhere.push(`b.token_id = ${tok.token_id}`);
    }
    if (q.pair) extraWhere.push(`p.pair_contract = '${String(q.pair).replace(/'/g,"''")}'`);
    else if (q.poolId) extraWhere.push(`p.pool_id = ${Number(q.poolId)}`);

    const params: any[] = [];
    const { sql, params: p2, zigUsdIdx } = this.buildWorthPagedSQL({
      scope:'all', scopeValue:null, direction:dir, includeLiquidity,
      windowOpts, page, limit, unit, klass, minValue:minV, maxValue:maxV, extraWhere
    }, params);
    p2[zigUsdIdx - 1] = zigUsd;

    const { rows } = await this.pg.query(sql, p2);
    const total = rows[0]?.total ? Number(rows[0].total) : 0;
    const pages = Math.max(1, Math.ceil(total / limit));
    const shaped = rows.map(r => {
      const s = this.shapeRow(r, unit, zigUsd);
      const w = this.worthForClass(s, unit, zigUsd);
      (s as any).class = w != null ? this.classifyByThreshold(w) : null;
      return s;
    });
    return { success:true, data: shaped, meta:{ unit, limit, page, pages, total, tf: q.tf || '24h', minValue:minV ?? undefined, maxValue:maxV ?? undefined } };
  }

  // ------- tiny helpers shared with Swap -------
  private async resolveTokenId(idOrDenomOrSymbol: string): Promise<{ token_id:number; denom:string; symbol:string|null; exponent:number|null }|null> {
    const sql = `
      WITH inp AS (SELECT $1::text AS q)
      SELECT token_id, denom, symbol, exponent
      FROM tokens t
      WHERE t.denom = (SELECT q FROM inp)
         OR t.symbol = (SELECT q FROM inp)
         OR lower(t.symbol) = lower((SELECT q FROM inp))
         OR t.token_id::text = (SELECT q FROM inp)
      ORDER BY
        CASE WHEN t.denom = (SELECT q FROM inp) THEN 0 ELSE 1 END,
        CASE WHEN lower(t.symbol) = lower((SELECT q FROM inp)) THEN 0 ELSE 1 END,
        t.token_id DESC
      LIMIT 1`;
    const r = await this.pg.query<{token_id:number;denom:string;symbol:string|null;exponent:number|null}>(sql, [idOrDenomOrSymbol]);
    return r.rows[0] ?? null;
  }
}
