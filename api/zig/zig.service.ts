import { Injectable } from '@nestjs/common';
import { PgService } from '../db/pg.service';

@Injectable()
export class ZigService {
  constructor(private readonly pg: PgService) {}

  private async zigUsd(): Promise<number> {
    const r = await this.pg.query(`SELECT zig_usd FROM exchange_rates ORDER BY ts DESC LIMIT 1`);
    return r.rows[0]?.zig_usd ? Number(r.rows[0].zig_usd) : 0;
  }

  private async zigChangeFromFx() {
    const buckets = [30, 60, 240, 1440];
    const latestQ = await this.pg.query(
      `SELECT zig_usd, ts FROM exchange_rates ORDER BY ts DESC LIMIT 1`
    );
    const latest = latestQ.rows[0]?.zig_usd != null ? Number(latestQ.rows[0].zig_usd) : null;
    const out: Record<number, number|null> = {};
    if (!latest) {
      for (const b of buckets) out[b] = null;
      return { latest: null, change: out };
    }
    const latestVal = Number(latest);
    const { rows } = await this.pg.query(`
      WITH vals AS (
        SELECT bucket,
               (SELECT zig_usd
                  FROM exchange_rates
                  WHERE ts <= now() - (bucket || ' minutes')::interval
                  ORDER BY ts DESC
                  LIMIT 1) AS zig_usd
        FROM (VALUES (30),(60),(240),(1440)) v(bucket)
      )
      SELECT bucket, zig_usd FROM vals
    `);
    const map = new Map(rows.map((r: any) => [Number(r.bucket), r.zig_usd != null ? Number(r.zig_usd) : null]));
    for (const b of buckets) {
      const prev = map.get(b);
      const prevNum = prev == null ? null : Number(prev);
      if (prevNum == null || prevNum === 0) out[b] = null;
      else out[b] = ((latestVal - prevNum) / prevNum) * 100;
    }
    return { latest: latestVal, change: out };
  }

  private async resolveZigToken() {
    const { rows } = await this.pg.query(
      `
        SELECT token_id, denom, symbol, name, exponent, image_uri, created_at
        FROM tokens
        WHERE denom = ANY($1)
           OR lower(symbol) = 'zig'
        ORDER BY token_id ASC
        LIMIT 1
      `,
      [['uzig', 'zig', 'uZIG', 'UZIG']]
    );
    return rows[0] || null;
  }

  async overview(limit: number) {
    const zig = await this.resolveZigToken();
    if (!zig) return { success: false, error: 'ZIG token not found' };

    const fx = await this.zigChangeFromFx();
    const zigUsd = fx.latest != null ? fx.latest : await this.zigUsd();

    const supplyRow = await this.pg.query(
      `SELECT total_supply_base, max_supply_base, exponent FROM tokens WHERE token_id=$1`,
      [zig.token_id]
    );
    const s = supplyRow.rows[0] || {};
    const exp = s.exponent != null ? Number(s.exponent) : Number(zig.exponent ?? 6);
    const circ = s.total_supply_base != null ? Number(s.total_supply_base) / 10 ** exp : null;
    const max = s.max_supply_base != null ? Number(s.max_supply_base) / 10 ** exp : null;

    const agg = await this.pg.query(
      `
        SELECT
          COALESCE(SUM(pm.tvl_zig),0)                          AS tvl_zig,
          COALESCE(SUM(pm.vol_buy_zig),0)                      AS vol_buy_zig,
          COALESCE(SUM(pm.vol_sell_zig),0)                     AS vol_sell_zig,
          COALESCE(SUM(pm.tx_buy + pm.tx_sell),0)              AS tx,
          COALESCE(SUM(pm.unique_traders),0)                   AS traders
        FROM pool_matrix pm
        JOIN pools p ON p.pool_id = pm.pool_id
        WHERE pm.bucket = '24h'
          AND p.is_uzig_quote = TRUE
      `
    );
    const A = agg.rows[0] || {};
    const tvlZig = Number(A.tvl_zig || 0);
    const volBuyZig = Number(A.vol_buy_zig || 0);
    const volSellZig = Number(A.vol_sell_zig || 0);
    const volZig = volBuyZig + volSellZig;
    const tx = Number(A.tx || 0);
    const traders = Number(A.traders || 0);

    const holdersRow = await this.pg.query(
      `SELECT holders_count FROM token_holders_stats WHERE token_id=$1`,
      [zig.token_id]
    );
    const holders = holdersRow.rows[0]?.holders_count != null ? Number(holdersRow.rows[0].holders_count) : null;

    const bestPoolRow = await this.pg.query(
      `
        SELECT
          p.pool_id,
          p.pair_contract,
          p.pair_type,
          COALESCE(pm.tvl_zig,0) AS tvl_zig,
          COALESCE(pm.vol_buy_zig,0) + COALESCE(pm.vol_sell_zig,0) AS vol_zig,
          (SELECT price_in_zig FROM prices pr
             WHERE pr.pool_id = p.pool_id AND pr.token_id = p.base_token_id
             ORDER BY pr.updated_at DESC LIMIT 1) AS price_in_zig
        FROM pools p
        LEFT JOIN pool_matrix pm ON pm.pool_id = p.pool_id AND pm.bucket='24h'
        WHERE p.is_uzig_quote = TRUE
        ORDER BY COALESCE(pm.tvl_zig,0) DESC NULLS LAST
        LIMIT 1
      `
    );
    const bp = bestPoolRow.rows[0] || null;

    const topPools = await this.pg.query(
      `
        SELECT
          p.pool_id,
          p.pair_contract,
          p.pair_type,
          b.token_id   AS base_token_id,
          b.symbol     AS base_symbol,
          b.denom      AS base_denom,
          q.token_id   AS quote_token_id,
          q.symbol     AS quote_symbol,
          q.denom      AS quote_denom,
          COALESCE(pm.tvl_zig,0) AS tvl_zig,
          COALESCE(pm.vol_buy_zig,0) + COALESCE(pm.vol_sell_zig,0) AS vol_zig,
          COALESCE(pm.tx_buy,0) + COALESCE(pm.tx_sell,0) AS tx,
          (SELECT price_in_zig FROM prices pr
             WHERE pr.pool_id = p.pool_id AND pr.token_id = p.base_token_id
             ORDER BY pr.updated_at DESC LIMIT 1) AS price_in_zig
        FROM pools p
        JOIN tokens b ON b.token_id = p.base_token_id
        JOIN tokens q ON q.token_id = p.quote_token_id
        LEFT JOIN pool_matrix pm ON pm.pool_id = p.pool_id AND pm.bucket='24h'
        WHERE p.is_uzig_quote = TRUE
        ORDER BY COALESCE(pm.vol_buy_zig,0) + COALESCE(pm.vol_sell_zig,0) DESC NULLS LAST
        LIMIT $1
      `,
      [limit]
    );

    const priceNative = 1;
    const priceUsd = zigUsd || null;
    const mcNative = circ != null ? circ * priceNative : null;
    const fdvNative = max != null ? max * priceNative : null;

    return {
      success: true,
      data: {
        token: {
          tokenId: String(zig.token_id),
          denom: zig.denom,
          symbol: zig.symbol,
          name: zig.name,
          exponent: exp,
          imageUri: zig.image_uri,
          creationTime: zig.created_at
        },
        price: {
          native: priceNative,
          usd: priceUsd,
          changePct: {
            '30m': fx.change[30] ?? null,
            '1h': fx.change[60] ?? null,
            '4h': fx.change[240] ?? null,
            '24h': fx.change[1440] ?? null
          },
          source: 'exchange_rates'
        },
        supply: { circulating: circ, max },
        mcap: { native: mcNative, usd: mcNative != null && priceUsd != null ? mcNative * priceUsd : null },
        fdv: { native: fdvNative, usd: fdvNative != null && priceUsd != null ? fdvNative * priceUsd : null },
        liquidity: { native: tvlZig, usd: tvlZig * (priceUsd || 0) },
        volume24h: {
          native: volZig,
          usd: volZig * (priceUsd || 0),
          buyNative: volBuyZig,
          sellNative: volSellZig
        },
        tx24h: tx,
        traders24h: traders,
        holders,
        bestPool: bp
          ? {
              poolId: bp.pool_id,
              pairContract: bp.pair_contract,
              pairType: bp.pair_type,
              priceNativeMid: bp.price_in_zig != null ? Number(bp.price_in_zig) : null,
              tvlNative: Number(bp.tvl_zig || 0),
              volNative: Number(bp.vol_zig || 0)
            }
          : null,
        topPools: topPools.rows.map((r: any) => ({
          poolId: r.pool_id,
          pairContract: r.pair_contract,
          pairType: r.pair_type,
          base: { tokenId: r.base_token_id, symbol: r.base_symbol, denom: r.base_denom },
          quote: { tokenId: r.quote_token_id, symbol: r.quote_symbol, denom: r.quote_denom },
          priceNativeMid: r.price_in_zig != null ? Number(r.price_in_zig) : null,
          tvlNative: Number(r.tvl_zig || 0),
          tvlUsd: Number(r.tvl_zig || 0) * (priceUsd || 0),
          volNative: Number(r.vol_zig || 0),
          volUsd: Number(r.vol_zig || 0) * (priceUsd || 0),
          tx: Number(r.tx || 0)
        }))
      }
    };
  }
}
