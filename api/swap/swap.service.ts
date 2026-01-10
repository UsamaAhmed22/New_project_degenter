import { Injectable } from '@nestjs/common';
import { PgService } from '../db/pg.service';

const UZIG_ALIASES = new Set(['uzig','zig','uzig','uZIG','UZIG']);

@Injectable()
export class SwapService {
  constructor(private readonly pg: PgService) {}

  private isUzigRef(s: any) { return !!s && UZIG_ALIASES.has(String(s).trim().toLowerCase()); }

  private async resolveTokenId(idOrDenomOrSymbol: string) {
    const sql = `
      WITH inp AS (SELECT $1::text AS q)
      SELECT token_id, denom, symbol, exponent, name
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
    const r = await this.pg.query(sql, [idOrDenomOrSymbol]);
    return r.rows[0] ?? null;
  }

  private async resolveRef(ref: any) {
    if (this.isUzigRef(ref)) return { type: 'uzig' as const };
    const tok = await this.resolveTokenId(ref);
    if (!tok) return null;
    return { type: 'token' as const, token: tok };
  }

  private async zigUsd() {
    const r = await this.pg.query(`SELECT zig_usd FROM exchange_rates ORDER BY ts DESC LIMIT 1`);
    return r.rows[0]?.zig_usd ? Number(r.rows[0].zig_usd) : 0;
  }

  private pairFee(pairType?: string|null) {
    if (!pairType) return 0.003;
    const t = String(pairType).toLowerCase();
    if (t === 'xyk') return 0.0001;
    if (t === 'concentrated') return 0.01;
    const m = t.match(/xyk[_-](\d+)/);
    if (m) {
      const bps = Number(m[1]);
      if (Number.isFinite(bps)) return bps / 10_000;
    }
    return 0.003;
  }

  private simulateXYK(opts: { fromIsZig: boolean; amountIn: number; Rz: number; Rt: number; fee: number }) {
    const { fromIsZig, amountIn, Rz, Rt, fee } = opts;
    if (!(Rz > 0 && Rt > 0) || !(amountIn > 0)) return { out: 0, price: 0, impact: 0 };
    const mid = Rz / Rt;
    const xin = amountIn * (1 - fee);
    if (fromIsZig) {
      const outToken = (xin * Rt) / (Rz + xin);
      const effZigPerToken = amountIn / Math.max(outToken, 1e-18);
      const impact = mid > 0 ? (effZigPerToken / mid) - 1 : 0;
      return { out: outToken, price: effZigPerToken, impact };
    } else {
      const outZig = (xin * Rz) / (Rt + xin);
      const effZigPerToken = outZig / amountIn;
      const impact = mid > 0 ? (mid / Math.max(effZigPerToken, 1e-18)) - 1 : 0;
      return { out: outZig, price: effZigPerToken, impact };
    }
  }

  private async loadUzigPoolsForToken(tokenId: number, { minTvlZig = 0 } = {}) {
    const { rows } = await this.pg.query(`
      SELECT
        p.pool_id,
        p.pair_contract,
        p.pair_type,
        pr.price_in_zig,
        ps.reserve_base_base   AS res_base_base,
        ps.reserve_quote_base  AS res_quote_base,
        tb.exponent            AS base_exp,
        tq.exponent            AS quote_exp,
        COALESCE(pm.tvl_zig,0) AS tvl_zig
      FROM pools p
      JOIN prices pr           ON pr.pool_id = p.pool_id AND pr.token_id = $1
      LEFT JOIN pool_state ps  ON ps.pool_id = p.pool_id
      JOIN tokens tb           ON tb.token_id = p.base_token_id
      JOIN tokens tq           ON tq.token_id = p.quote_token_id
      LEFT JOIN pool_matrix pm ON pm.pool_id = p.pool_id AND pm.bucket = '24h'
      WHERE p.is_uzig_quote = TRUE
    `, [tokenId]);

    return rows.map(r => {
      const Rt = Number(r.res_base_base  || 0) / Math.pow(10, Number(r.base_exp  || 0));
      const Rz = Number(r.res_quote_base || 0) / Math.pow(10, Number(r.quote_exp || 0));
      return {
        poolId: String(r.pool_id),
        pairContract: r.pair_contract as string,
        pairType: r.pair_type as string,
        priceInZig: Number(r.price_in_zig || 0),
        tokenReserve: Rt,
        zigReserve: Rz,
        tvlZig: Number(r.tvl_zig || 0),
      };
    }).filter(p => p.tvlZig >= minTvlZig);
  }

  private pickBySimulation(pools: any[], side: 'buy'|'sell', { fromIsZig, amountIn }: { fromIsZig:boolean; amountIn:number }) {
    let best: any = null;
    for (const p of pools) {
      const fee = this.pairFee(p.pairType);
      const hasRes = p.zigReserve > 0 && p.tokenReserve > 0;
      const sim = hasRes ? this.simulateXYK({ fromIsZig, amountIn, Rz: p.zigReserve, Rt: p.tokenReserve, fee }) : null;
      const score = sim ? sim.out : 0;
      const cand = { ...p, fee, sim, score };
      if (!best || cand.score > best.score) best = cand;
    }
    return best;
  }

  private defaultAmount(side: 'buy'|'sell', { zigUsd, pools }: { zigUsd: number; pools: any[] }) {
    const targetUsd = 100;
    const zigAmt = targetUsd / Math.max(zigUsd, 1e-9);
    if (side === 'buy') return zigAmt;
    const avgMid = pools.length ? pools.reduce((s, p) => s + (p.priceInZig || 0), 0) / pools.length : 1;
    return zigAmt / Math.max(avgMid, 1e-12);
  }

  private makePairBlock({ side, pool, sim, fee, zigUsd, amountIn }: any) {
    const price_native_exec = sim ? sim.price : null;
    const price_usd_exec    = price_native_exec != null ? price_native_exec * zigUsd : null;
    const price_native_mid  = pool.priceInZig;
    const price_usd_mid     = price_native_mid * zigUsd;

    return {
      poolId: pool.poolId,
      pairContract: pool.pairContract,
      pairType: pool.pairType,
      side,
      price_native_exec,
      price_usd_exec,
      price_native_mid,
      price_usd_mid,
      amount_in: amountIn ?? null,
      amount_out: sim ? sim.out : null,
      price_impact: sim ? sim.impact : null,
      fee
    };
  }

  private async bestBuyPool(tokenId: number, { amountIn, minTvlZig, zigUsd }: any) {
    const pools = await this.loadUzigPoolsForToken(tokenId, { minTvlZig });
    if (!pools.length) return null;
    const amt = Number.isFinite(amountIn) ? Number(amountIn) : this.defaultAmount('buy', { zigUsd, pools });
    const pick = this.pickBySimulation(pools, 'buy', { fromIsZig: true, amountIn: amt });
    if (!pick) return null;
    return { ...pick, amtUsed: amt };
  }
  private async bestSellPool(tokenId: number, { amountIn, minTvlZig, zigUsd }: any) {
    const pools = await this.loadUzigPoolsForToken(tokenId, { minTvlZig });
    if (!pools.length) return null;
    const amt = Number.isFinite(amountIn) ? Number(amountIn) : this.defaultAmount('sell', { zigUsd, pools });
    const pick = this.pickBySimulation(pools, 'sell', { fromIsZig: false, amountIn: amt });
    if (!pick) return null;
    return { ...pick, amtUsed: amt };
  }

  async route(q: any) {
    const fromRef = q.from;
    const toRef   = q.to;
    if (!fromRef || !toRef) return { success:false, error:'missing from/to' };

    const zigUsd    = await this.zigUsd();
    const amt       = q.amt ? Number(q.amt) : undefined;
    const minTvlZig = q.minTvl ? Number(q.minTvl) : 0;

    const from = await this.resolveRef(fromRef);
    const to   = await this.resolveRef(toRef);
    if (!from) return { success:false, error:'from token not found' };
    if (!to)   return { success:false, error:'to token not found' };

    // ZIG -> TOKEN
    if (from.type === 'uzig' && to.type === 'token') {
      const buy = await this.bestBuyPool(to.token.token_id, { amountIn: amt, minTvlZig, zigUsd });
      if (!buy) {
        return { success:true, data:{
          route:['uzig', to.token.denom || to.token.symbol],
          pairs:[], price_native:null, price_usd:null,
          cross:{ zig_per_from:1, usd_per_from:zigUsd },
          usd_baseline:{ from_usd: zigUsd, to_usd: null }, source:'direct_uzig'
        }};
      }
      const pairBlock = this.makePairBlock({ side:'buy', pool: buy, sim: buy.sim, fee: buy.fee, zigUsd, amountIn: buy.amtUsed });
      const price_native = pairBlock.price_native_exec;
      const price_usd    = pairBlock.price_usd_exec;
      const from_usd = zigUsd;
      const to_usd   = pairBlock.price_native_mid * zigUsd;
      return { success:true, data:{
        route:['uzig', to.token.denom || to.token.symbol || String(to.token.token_id)],
        pairs:[pairBlock],
        price_native, price_usd,
        cross:{ zig_per_from:1, usd_per_from:zigUsd },
        usd_baseline:{ from_usd, to_usd },
        source:'direct_uzig'
      }};
    }

    // TOKEN -> ZIG
    if (from.type === 'token' && to.type === 'uzig') {
      const sell = await this.bestSellPool(from.token.token_id, { amountIn: amt, minTvlZig, zigUsd });
      if (!sell) {
        return { success:true, data:{
          route:[from.token.denom || from.token.symbol, 'uzig'],
          pairs:[], price_native:null, price_usd:null,
          cross:{ zig_per_from:null, usd_per_from:null },
          usd_baseline:{ from_usd: null, to_usd: zigUsd }, source:'direct_uzig'
        }};
      }
      const pairBlock = this.makePairBlock({ side:'sell', pool: sell, sim: sell.sim, fee: sell.fee, zigUsd, amountIn: sell.amtUsed });
      const price_native = pairBlock.price_native_exec;
      const price_usd    = price_native != null ? price_native * zigUsd : null;
      const from_usd = sell.priceInZig * zigUsd;
      const to_usd   = zigUsd;
      return { success:true, data:{
        route:[from.token.denom || from.token.symbol || String(from.token.token_id), 'uzig'],
        pairs:[pairBlock],
        price_native, price_usd,
        cross:{ zig_per_from: price_native, usd_per_from: from_usd },
        usd_baseline:{ from_usd, to_usd }, source:'direct_uzig'
      }};
    }

    // TOKEN -> TOKEN (via UZIG)
    if (from.type === 'token' && to.type === 'token') {
      const sellA = await this.bestSellPool(from.token.token_id, { amountIn: amt, minTvlZig, zigUsd });
      const zigOut = sellA?.sim ? sellA.sim.out : undefined;
      const buyB  = await this.bestBuyPool(to.token.token_id,   { amountIn: zigOut, minTvlZig, zigUsd });

      if (!sellA || !buyB) {
        return { success:true, data:{
          route:[from.token.denom || from.token.symbol || String(from.token.token_id),'uzig',to.token.denom || to.token.symbol || String(to.token.token_id)],
          pairs:[], price_native:null, price_usd:null,
          cross:{ zig_per_from:null, usd_per_from:null },
          usd_baseline:{ from_usd:null, to_usd:null }, source:'via_uzig'
        }};
      }
      const sellBlock = this.makePairBlock({ side:'sell', pool: sellA, sim: sellA.sim, fee: sellA.fee, zigUsd, amountIn: sellA.amtUsed });
      const buyBlock  = this.makePairBlock({ side:'buy',  pool: buyB,  sim: buyB.sim,  fee: buyB.fee,  zigUsd, amountIn: buyB.amtUsed });
      const bPerA = sellA.priceInZig / Math.max(buyB.priceInZig, 1e-18);
      const from_usd = sellA.priceInZig * zigUsd;
      const to_usd   = buyB.priceInZig  * zigUsd;
      return { success:true, data:{
        route:[from.token.denom || from.token.symbol || String(from.token.token_id),'uzig',to.token.denom || to.token.symbol || String(to.token.token_id)],
        pairs:[sellBlock, buyBlock],
        price_native: bPerA, price_usd: null,
        cross:{ zig_per_from: sellA.priceInZig, usd_per_from: from_usd },
        usd_baseline:{ from_usd, to_usd }, source:'via_uzig'
      }};
    }

    return { success:false, error:'unsupported route (check from/to)' };
  }
}
