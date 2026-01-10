import { Injectable } from '@nestjs/common';
import { PgService } from '../db/pg.service';

type Win = '24h' | '7d' | '10d' | '30d';
type SortSpec = { expr: string; dir: 'ASC' | 'DESC'; type: 'numeric' | 'timestamptz' };

@Injectable()
export class WalletsService {
  constructor(private readonly pg: PgService) {}

  private normalizeWin(win?: string): Win {
    const w = String(win || '').toLowerCase();
    if (w === '7d' || w === '10d' || w === '30d') return w as Win;
    return '24h';
  }

  private winInterval(win: Win): string {
    switch (win) {
      case '7d':
        return '7 days';
      case '10d':
        return '10 days';
      case '30d':
        return '30 days';
      default:
        return '24 hours';
    }
  }

  private winMinutes(win: Win): number {
    switch (win) {
      case '7d':
        return 7 * 1440;
      case '10d':
        return 10 * 1440;
      case '30d':
        return 30 * 1440;
      default:
        return 24 * 60;
    }
  }

  private parseTfMinutes(tf?: string): number {
    const raw = String(tf || '').toLowerCase();
    const m = raw.match(/^(\d+)(m|h|d)$/);
    if (!m) return 1440;
    const n = Number(m[1]);
    if (!Number.isFinite(n) || n <= 0) return 1440;
    if (m[2] === 'm') return n;
    if (m[2] === 'h') return n * 60;
    return n * 1440;
  }

  private parseLimit(val: any, def: number, max: number) {
    const n = Number(val);
    if (!Number.isFinite(n) || n <= 0) return def;
    return Math.min(Math.floor(n), max);
  }

  private parseTop(val: any, def = 10, max = 50) {
    const n = Number(val);
    if (!Number.isFinite(n) || n <= 0) return def;
    return Math.min(Math.floor(n), max);
  }

  private safeNum(v: any, fallback = 0) {
    const n = Number(v);
    return Number.isFinite(n) ? n : fallback;
  }

  private parseActivityCursor(cursor?: string) {
    if (!cursor) return null;
    const raw = String(cursor);
    const idx = raw.lastIndexOf('|');
    if (idx < 0) return null;
    const ts = raw.slice(0, idx);
    const id = Number(raw.slice(idx + 1));
    const d = new Date(ts);
    if (!Number.isFinite(id) || Number.isNaN(d.getTime())) return null;
    return { ts: d.toISOString(), id: Math.floor(id) };
  }

  private parseSortCursor(cursor?: string) {
    if (!cursor) return null;
    const raw = String(cursor);
    const idx = raw.lastIndexOf('|');
    if (idx < 0) return null;
    const value = raw.slice(0, idx);
    const tokenId = Number(raw.slice(idx + 1));
    if (!Number.isFinite(tokenId)) return null;
    return { value, tokenId: Math.floor(tokenId) };
  }

  private csvEscape(value: any) {
    if (value == null) return '';
    const s = String(value);
    if (s.includes('"') || s.includes(',') || s.includes('\n')) {
      return `"${s.replace(/"/g, '""')}"`;
    }
    return s;
  }

  private normalizeSort(sort?: string): SortSpec {
    const key = String(sort || '').toLowerCase();
    const map: Record<string, SortSpec> = {
      total_pnl_desc: { expr: 'total_pnl_usd', dir: 'DESC', type: 'numeric' },
      total_pnl_asc: { expr: 'total_pnl_usd', dir: 'ASC', type: 'numeric' },
      realized_pnl_desc: { expr: 'realized_pnl_usd', dir: 'DESC', type: 'numeric' },
      realized_pnl_asc: { expr: 'realized_pnl_usd', dir: 'ASC', type: 'numeric' },
      unrealized_pnl_desc: { expr: 'unrealized_pnl_usd', dir: 'DESC', type: 'numeric' },
      unrealized_pnl_asc: { expr: 'unrealized_pnl_usd', dir: 'ASC', type: 'numeric' },
      volume_desc: { expr: 'volume_usd', dir: 'DESC', type: 'numeric' },
      volume_asc: { expr: 'volume_usd', dir: 'ASC', type: 'numeric' },
      value_desc: { expr: 'position_value_usd', dir: 'DESC', type: 'numeric' },
      value_asc: { expr: 'position_value_usd', dir: 'ASC', type: 'numeric' },
      last_active_desc: { expr: 'last_active', dir: 'DESC', type: 'timestamptz' },
      last_active_asc: { expr: 'last_active', dir: 'ASC', type: 'timestamptz' },
      net_desc: { expr: 'net_usd', dir: 'DESC', type: 'numeric' },
      net_asc: { expr: 'net_usd', dir: 'ASC', type: 'numeric' },
    };
    return map[key] || map.total_pnl_desc;
  }

  private async getWalletRow(address: string) {
    const { rows } = await this.pg.query(
      `
      SELECT
        w.wallet_id,
        w.address,
        w.display_name,
        wp.tags,
        wp.twitter,
        wp.telegram,
        wp.website
      FROM wallets w
      LEFT JOIN wallet_profiles wp ON wp.wallet_id = w.wallet_id
      WHERE lower(w.address) = lower($1)
      LIMIT 1
      `,
      [address],
    );
    return rows[0] || null;
  }

  private async getWalletId(address: string) {
    const { rows } = await this.pg.query(
      `SELECT wallet_id FROM wallets WHERE lower(address) = lower($1) LIMIT 1`,
      [address],
    );
    return rows[0]?.wallet_id ?? null;
  }

  private async getPortfolioTotals(walletId: number) {
    const { rows } = await this.pg.query(
      `
      WITH rate AS (
        SELECT zig_usd FROM exchange_rates ORDER BY ts DESC LIMIT 1
      ),
      latest_price AS (
        SELECT DISTINCT ON (token_id) token_id, price_in_zig
        FROM prices
        ORDER BY token_id, updated_at DESC
      )
      SELECT
        COALESCE(SUM(
          (wtp.amount_base / POWER(10::numeric, COALESCE(t.exponent,6)))
          * COALESCE(lp.price_in_zig, 0)
          * COALESCE((SELECT zig_usd FROM rate), 0)
        ), 0) AS value_usd,
        COALESCE(SUM(wtp.cost_basis_usd), 0) AS cost_basis_usd
      FROM wallet_token_positions wtp
      JOIN tokens t ON t.token_id = wtp.token_id
      LEFT JOIN latest_price lp ON lp.token_id = wtp.token_id
      WHERE wtp.wallet_id = $1 AND wtp.amount_base > 0
      `,
      [walletId],
    );
    const row = rows[0] || {};
    return {
      value_usd: this.safeNum(row.value_usd),
      cost_basis_usd: this.safeNum(row.cost_basis_usd),
    };
  }

  private async getSnapshotValue(walletId: number, interval: string) {
    const { rows } = await this.pg.query(
      `
      SELECT value_usd
      FROM wallet_portfolio_snapshots
      WHERE wallet_id = $1
        AND ts <= now() - ($2::interval)
      ORDER BY ts DESC
      LIMIT 1
      `,
      [walletId, interval],
    );
    return rows[0]?.value_usd != null ? Number(rows[0].value_usd) : null;
  }

  private computeMddPct(points: Array<{ value_usd: number }>) {
    let peak = 0;
    let maxDd = 0;
    for (const p of points) {
      const v = this.safeNum(p.value_usd);
      if (v > peak) {
        peak = v;
        continue;
      }
      if (peak > 0) {
        const dd = (peak - v) / peak;
        if (dd > maxDd) maxDd = dd;
      }
    }
    return peak > 0 ? maxDd * 100 : 0;
  }

  private async getHoldingsRows(walletId: number) {
    const { rows } = await this.pg.query(
      `
      WITH rate AS (
        SELECT zig_usd FROM exchange_rates ORDER BY ts DESC LIMIT 1
      ),
      latest_price AS (
        SELECT DISTINCT ON (token_id) token_id, price_in_zig
        FROM prices
        ORDER BY token_id, updated_at DESC
      ),
      base AS (
        SELECT
          wtp.wallet_id,
          wtp.token_id,
          wtp.amount_base,
          wtp.cost_basis_usd,
          wtp.updated_at,
          t.symbol,
          t.denom,
          t.image_uri,
          t.exponent,
          COALESCE(lp.price_in_zig, 0) AS price_in_zig,
          COALESCE((SELECT zig_usd FROM rate), 0) AS zig_usd
        FROM wallet_token_positions wtp
        JOIN tokens t ON t.token_id = wtp.token_id
        LEFT JOIN latest_price lp ON lp.token_id = wtp.token_id
        WHERE wtp.wallet_id = $1 AND wtp.amount_base > 0
      ),
      calc AS (
        SELECT
          *,
          (amount_base / POWER(10::numeric, COALESCE(exponent,6))) AS balance,
          (price_in_zig * zig_usd) AS price_usd,
          (amount_base / POWER(10::numeric, COALESCE(exponent,6))) * price_in_zig * zig_usd AS value_usd
        FROM base
      )
      SELECT
        *,
        CASE
          WHEN amount_base > 0 THEN cost_basis_usd / NULLIF((amount_base / POWER(10::numeric, COALESCE(exponent,6))), 0)
          ELSE 0
        END AS avg_entry_usd,
        (value_usd - cost_basis_usd) AS unrealized_pnl_usd,
        CASE
          WHEN cost_basis_usd > 0 THEN ((value_usd - cost_basis_usd) / cost_basis_usd) * 100
          ELSE 0
        END AS unrealized_pnl_pct,
        MAX(updated_at) OVER() AS max_updated_at,
        SUM(value_usd) OVER() AS total_value_usd,
        SUM(cost_basis_usd) OVER() AS total_cost_basis_usd
      FROM calc
      ORDER BY value_usd DESC NULLS LAST
      `,
      [walletId],
    );
    return rows;
  }

  private buildFlatSeries(valueUsd: number, win: Win, tf: string) {
    const tfMinutes = this.parseTfMinutes(tf);
    const winMinutes = this.winMinutes(win);
    const now = new Date();
    const startMs = now.getTime() - winMinutes * 60 * 1000;
    const stepMs = tfMinutes * 60 * 1000;
    const points: Array<{ t: string; value_usd: number }> = [];

    if (!Number.isFinite(stepMs) || stepMs <= 0) {
      return [{ t: now.toISOString(), value_usd: valueUsd }];
    }

    let count = Math.floor(winMinutes / tfMinutes);
    const maxPoints = 500;
    if (count > maxPoints) count = maxPoints;

    for (let i = 0; i <= count; i += 1) {
      const t = new Date(startMs + i * stepMs);
      if (t.getTime() > now.getTime()) break;
      points.push({ t: t.toISOString(), value_usd: valueUsd });
    }

    if (!points.length || new Date(points[points.length - 1].t).getTime() < now.getTime()) {
      points.push({ t: now.toISOString(), value_usd: valueUsd });
    }

    return points;
  }

  async summary(address: string, winParam?: string) {
    const wallet = await this.getWalletRow(address);
    if (!wallet) return { success: false, error: 'wallet not found' };

    const win = this.normalizeWin(winParam);
    const statsQ = await this.pg.query(
      `
      SELECT
        win,
        volume_usd,
        tx_count,
        bought_usd,
        sold_usd,
        realized_pnl_usd,
        win_rate,
        avg_hold_seconds
      FROM wallet_stats_window
      WHERE wallet_id = $1 AND win = $2::wallet_window
      `,
      [wallet.wallet_id, win],
    );
    const stats = statsQ.rows[0] || {};

    const totals = await this.getPortfolioTotals(wallet.wallet_id);
    const interval = this.winInterval(win);
    const pastValue = await this.getSnapshotValue(wallet.wallet_id, interval);
    let changePct = 0;
    if (pastValue != null && pastValue > 0) {
      changePct = ((totals.value_usd - pastValue) / pastValue) * 100;
    } else if (totals.cost_basis_usd > 0) {
      changePct = ((totals.value_usd - totals.cost_basis_usd) / totals.cost_basis_usd) * 100;
    }

    return {
      address: wallet.address,
      wallet_id: wallet.wallet_id,
      labels: {
        name: wallet.display_name ?? null,
        tags: Array.isArray(wallet.tags) ? wallet.tags : [],
      },
      portfolio_value_usd: totals.value_usd,
      portfolio_change_pct: changePct,
      social: {
        twitter: wallet.twitter ?? null,
        telegram: wallet.telegram ?? null,
        website: wallet.website ?? null,
      },
      stats: {
        win,
        volume_usd: this.safeNum(stats.volume_usd),
        tx_count: this.safeNum(stats.tx_count),
        bought_usd: this.safeNum(stats.bought_usd),
        sold_usd: this.safeNum(stats.sold_usd),
        realized_pnl_usd: this.safeNum(stats.realized_pnl_usd),
        win_rate: this.safeNum(stats.win_rate),
        avg_hold_seconds: this.safeNum(stats.avg_hold_seconds),
      },
    };
  }

  async portfolioValueSeries(address: string, q: any) {
    const walletId = await this.getWalletId(address);
    if (!walletId) return { success: false, error: 'wallet not found' };

    const win = this.normalizeWin(q?.win || '30d');
    const tf = String(q?.tf || '1d').toLowerCase();

    const tfMinutes = this.parseTfMinutes(tf);
    const bucketSec = Math.max(60, tfMinutes * 60);
    const interval = this.winInterval(win);

    const { rows } = await this.pg.query(
      `
      WITH base AS (
        SELECT
          to_timestamp(FLOOR(EXTRACT(EPOCH FROM ts) / $2) * $2) AS bucket,
          value_usd,
          ts
        FROM wallet_portfolio_snapshots
        WHERE wallet_id = $1
          AND ts >= now() - ($3::interval)
      ),
      ranked AS (
        SELECT
          bucket,
          value_usd,
          ROW_NUMBER() OVER (PARTITION BY bucket ORDER BY ts DESC) AS rn
        FROM base
      )
      SELECT bucket, value_usd
      FROM ranked
      WHERE rn = 1
      ORDER BY bucket ASC
      `,
      [walletId, bucketSec, interval],
    );

    if (!rows.length) {
      const totals = await this.getPortfolioTotals(walletId);
      const flat = this.buildFlatSeries(totals.value_usd, win, tf);
      return { tf, points: flat, mdd_pct: 0, source: 'flat' };
    }

    const points = rows.map((r: any) => ({
      t: new Date(r.bucket).toISOString(),
      value_usd: this.safeNum(r.value_usd),
    }));
    const mddPct = this.computeMddPct(points);

    return { tf, points, mdd_pct: mddPct, source: 'snapshots' };
  }

  async activities(address: string, q: any) {
    const walletId = await this.getWalletId(address);
    if (!walletId) return { success: false, error: 'wallet not found' };

    const win = this.normalizeWin(q?.win);
    const limit = this.parseLimit(q?.limit, 50, 200);
    const cursor = this.parseActivityCursor(q?.cursor);
    const interval = this.winInterval(win);

    const params: any[] = [walletId, interval];
    const where: string[] = ['a.wallet_id = $1', 'a.ts >= now() - ($2::interval)'];

    if (cursor) {
      params.push(cursor.ts, cursor.id);
      const tsIdx = params.length - 1;
      const idIdx = params.length;
      where.push(`(a.ts, a.activity_id) < ($${tsIdx}::timestamptz, $${idIdx}::bigint)`);
    }

    const sql = `
      SELECT
        a.activity_id,
        a.ts,
        a.tx_hash,
        a.msg_index,
        a.pool_id,
        a.action,
        a.direction,
        a.token_in_id,
        a.token_out_id,
        a.value_usd,
        a.value_zig,
        (a.amount_in_base / POWER(10::numeric, COALESCE(tin.exponent,6))) AS amount_in,
        (a.amount_out_base / POWER(10::numeric, COALESCE(tout.exponent,6))) AS amount_out,
        tin.denom AS token_in_denom,
        tin.symbol AS token_in_symbol,
        tout.denom AS token_out_denom,
        tout.symbol AS token_out_symbol
      FROM wallet_activities a
      LEFT JOIN tokens tin ON tin.token_id = a.token_in_id
      LEFT JOIN tokens tout ON tout.token_id = a.token_out_id
      WHERE ${where.join(' AND ')}
      ORDER BY a.ts DESC, a.activity_id DESC
      LIMIT ${limit + 1}
    `;

    const { rows } = await this.pg.query(sql, params);
    const hasMore = rows.length > limit;
    const slice = hasMore ? rows.slice(0, limit) : rows;
    const last = slice[slice.length - 1];
    const nextCursor = hasMore && last
      ? `${new Date(last.ts).toISOString()}|${last.activity_id}`
      : null;

    return {
      win,
      next_cursor: nextCursor,
      items: slice.map((r: any) => ({
        ts: r.ts,
        tx_hash: r.tx_hash,
        msg_index: r.msg_index,
        pool_id: r.pool_id,
        action: r.action,
        direction: r.direction,
        token_in: r.token_in_id
          ? { token_id: r.token_in_id, denom: r.token_in_denom, symbol: r.token_in_symbol }
          : null,
        token_out: r.token_out_id
          ? { token_id: r.token_out_id, denom: r.token_out_denom, symbol: r.token_out_symbol }
          : null,
        amount_in: r.amount_in,
        amount_out: r.amount_out,
        value_usd: r.value_usd,
        value_zig: r.value_zig,
      })),
    };
  }

  async activitiesExport(address: string, q: any) {
    const walletId = await this.getWalletId(address);
    if (!walletId) return { ok: false, error: 'wallet not found' };

    const format = String(q?.format || 'csv').toLowerCase();
    if (format !== 'csv') return { ok: false, error: 'format not supported' };

    const win = this.normalizeWin(q?.win || '30d');
    const interval = this.winInterval(win);
    const limit = q?.limit != null ? this.parseLimit(q.limit, 10000, 100000) : null;

    const params: any[] = [walletId, interval];
    const limitClause = limit != null ? `LIMIT ${limit}` : '';

    const sql = `
      SELECT
        a.ts,
        a.tx_hash,
        a.msg_index,
        a.pool_id,
        a.action,
        a.direction,
        a.value_usd,
        a.value_zig,
        (a.amount_in_base / POWER(10::numeric, COALESCE(tin.exponent,6))) AS amount_in,
        (a.amount_out_base / POWER(10::numeric, COALESCE(tout.exponent,6))) AS amount_out,
        tin.symbol AS token_in_symbol,
        tout.symbol AS token_out_symbol
      FROM wallet_activities a
      LEFT JOIN tokens tin ON tin.token_id = a.token_in_id
      LEFT JOIN tokens tout ON tout.token_id = a.token_out_id
      WHERE a.wallet_id = $1 AND a.ts >= now() - ($2::interval)
      ORDER BY a.ts DESC, a.activity_id DESC
      ${limitClause}
    `;

    const { rows } = await this.pg.query(sql, params);

    const header = [
      'ts',
      'tx_hash',
      'msg_index',
      'pool_id',
      'action',
      'direction',
      'token_in',
      'token_out',
      'amount_in',
      'amount_out',
      'value_usd',
      'value_zig',
    ];

    const lines = rows.map((r: any) => [
      this.csvEscape(r.ts),
      this.csvEscape(r.tx_hash),
      this.csvEscape(r.msg_index),
      this.csvEscape(r.pool_id),
      this.csvEscape(r.action),
      this.csvEscape(r.direction),
      this.csvEscape(r.token_in_symbol),
      this.csvEscape(r.token_out_symbol),
      this.csvEscape(r.amount_in),
      this.csvEscape(r.amount_out),
      this.csvEscape(r.value_usd),
      this.csvEscape(r.value_zig),
    ].join(','));

    const csv = [header.join(','), ...lines].join('\n');
    const safeAddr = String(address).replace(/[^a-zA-Z0-9_-]/g, '');
    const filename = `wallet-activities-${safeAddr || 'wallet'}-${win}.csv`;

    return { ok: true, csv, filename };
  }

  async holdings(address: string) {
    const walletId = await this.getWalletId(address);
    if (!walletId) return { success: false, error: 'wallet not found' };

    const rows = await this.getHoldingsRows(walletId);
    if (!rows.length) {
      return {
        as_of: new Date().toISOString(),
        items: [],
        totals: { value_usd: 0 },
      };
    }

    const totalValue = this.safeNum(rows[0]?.total_value_usd);
    const asOf = rows[0]?.max_updated_at || new Date().toISOString();

    return {
      as_of: asOf,
      items: rows.map((r: any) => ({
        token: {
          token_id: r.token_id,
          symbol: r.symbol,
          denom: r.denom,
          image: r.image_uri,
        },
        balance: r.balance,
        price_usd: r.price_usd,
        value_usd: r.value_usd,
        avg_entry_usd: r.avg_entry_usd,
        cost_basis_usd: r.cost_basis_usd,
        unrealized_pnl_usd: r.unrealized_pnl_usd,
        unrealized_pnl_pct: r.unrealized_pnl_pct,
      })),
      totals: { value_usd: totalValue },
    };
  }

  async allocation(address: string, topParam?: string) {
    const walletId = await this.getWalletId(address);
    if (!walletId) return { success: false, error: 'wallet not found' };

    const rows = await this.getHoldingsRows(walletId);
    const totalValue = rows.length ? this.safeNum(rows[0]?.total_value_usd) : 0;
    const top = this.parseTop(topParam, 10, 50);

    const holdings = rows.map((r: any) => ({
      token_id: r.token_id,
      symbol: r.symbol,
      value_usd: this.safeNum(r.value_usd),
    }));

    holdings.sort((a, b) => b.value_usd - a.value_usd);
    const topItems = holdings.slice(0, top);
    const topSum = topItems.reduce((acc, item) => acc + item.value_usd, 0);
    const othersValue = Math.max(0, totalValue - topSum);

    const items = topItems.map((item) => ({
      token_id: item.token_id,
      symbol: item.symbol,
      value_usd: item.value_usd,
      pct: totalValue > 0 ? (item.value_usd / totalValue) * 100 : 0,
    }));

    if (othersValue > 0) {
      items.push({
        token_id: 0,
        symbol: 'Others',
        value_usd: othersValue,
        pct: totalValue > 0 ? (othersValue / totalValue) * 100 : 0,
      });
    }

    return { total_value_usd: totalValue, items };
  }

  async pnlOverview(address: string, winParam?: string) {
    const walletId = await this.getWalletId(address);
    if (!walletId) return { success: false, error: 'wallet not found' };

    const win = this.normalizeWin(winParam);
    const interval = this.winInterval(win);

    const statsQ = await this.pg.query(
      `
      SELECT
        volume_usd,
        tx_count,
        bought_usd,
        sold_usd,
        realized_pnl_usd,
        win_rate,
        avg_hold_seconds
      FROM wallet_stats_window
      WHERE wallet_id = $1 AND win = $2::wallet_window
      `,
      [walletId, win],
    );
    const stats = statsQ.rows[0] || {};

    const totals = await this.getPortfolioTotals(walletId);
    const unrealized = totals.value_usd - totals.cost_basis_usd;

    const avgQ = await this.pg.query(
      `
      SELECT
        COUNT(*) FILTER (WHERE direction='buy') AS txs_buy,
        COUNT(*) FILTER (WHERE direction='sell') AS txs_sell,
        COALESCE(AVG(value_usd) FILTER (WHERE direction='buy'),0) AS avg_buy_usd,
        COALESCE(AVG(value_usd) FILTER (WHERE direction='sell'),0) AS avg_sell_usd,
        COALESCE(AVG(value_usd) FILTER (WHERE direction='buy'),0) AS avg_cost_usd,
        COALESCE(AVG(value_usd) FILTER (WHERE direction='sell' AND realized_pnl_usd > 0),0) AS avg_win_cost_usd,
        COALESCE(AVG(value_usd) FILTER (WHERE direction='sell' AND realized_pnl_usd < 0),0) AS avg_loss_cost_usd
      FROM wallet_activities
      WHERE wallet_id = $1
        AND ts >= now() - ($2::interval)
        AND action = 'swap'
        AND direction IN ('buy','sell')
      `,
      [walletId, interval],
    );
    const avgRow = avgQ.rows[0] || {};

    const suspQ = await this.pg.query(
      `
      SELECT
        COUNT(*) FILTER (WHERE sold_usd > bought_usd AND sold_usd > 0) AS sold_more,
        COUNT(*) FILTER (WHERE sold_usd > 0 AND bought_usd = 0) AS didnt_buy,
        COUNT(*) FILTER (WHERE sold_usd > 0 AND hold_duration_sec > 0 AND hold_duration_sec <= 60) AS instant_sell,
        COUNT(*) FILTER (WHERE sold_usd > 0 AND realized_pnl_usd < 0) AS scam_rug,
        COUNT(*) FILTER (WHERE sold_usd > 0 OR bought_usd > 0) AS total
      FROM wallet_token_stats_window
      WHERE wallet_id = $1 AND win = $2::wallet_window
      `,
      [walletId, win],
    );
    const susp = suspQ.rows[0] || {};
    const totalTokens = this.safeNum(susp.total);
    const pct = (n: any) => (totalTokens > 0 ? (this.safeNum(n) / totalTokens) * 100 : 0);

    const realized = this.safeNum(stats.realized_pnl_usd);
    const totalPnl = realized + unrealized;

    return {
      win,
      realized_pnl_usd: realized,
      unrealized_pnl_usd: unrealized,
      total_pnl_usd: totalPnl,
      win_rate: this.safeNum(stats.win_rate),
      analytics: {
        volume_usd: this.safeNum(stats.volume_usd),
        tx_count: this.safeNum(stats.tx_count),
        txs_buy: this.safeNum(avgRow.txs_buy),
        txs_sell: this.safeNum(avgRow.txs_sell),
        avg_hold_seconds: this.safeNum(stats.avg_hold_seconds),
        bought_usd: this.safeNum(stats.bought_usd),
        sold_usd: this.safeNum(stats.sold_usd),
        avg_buy_usd: this.safeNum(avgRow.avg_buy_usd),
        avg_sell_usd: this.safeNum(avgRow.avg_sell_usd),
        avg_cost_usd: this.safeNum(avgRow.avg_cost_usd),
        avg_win_cost_usd: this.safeNum(avgRow.avg_win_cost_usd),
        avg_loss_cost_usd: this.safeNum(avgRow.avg_loss_cost_usd),
      },
      suspicious: {
        sold_more_than_bought_pct: pct(susp.sold_more),
        didnt_buy_pct: pct(susp.didnt_buy),
        instant_sell_pct: pct(susp.instant_sell),
        scam_rug_pct: pct(susp.scam_rug),
      },
    };
  }

  async pnlDistribution(address: string, winParam?: string) {
    const walletId = await this.getWalletId(address);
    if (!walletId) return { success: false, error: 'wallet not found' };

    const win = this.normalizeWin(winParam || '30d');

    const distQ = await this.pg.query(
      `
      WITH base AS (
        SELECT
          CASE
            WHEN bought_usd > 0 THEN (realized_pnl_usd / bought_usd) * 100
            ELSE NULL
          END AS pnl_pct
        FROM wallet_token_stats_window
        WHERE wallet_id = $1 AND win = $2::wallet_window
      )
      SELECT
        COUNT(*) FILTER (WHERE pnl_pct > 500) AS gt500,
        COUNT(*) FILTER (WHERE pnl_pct > 100 AND pnl_pct <= 500) AS gt100,
        COUNT(*) FILTER (WHERE pnl_pct > 0 AND pnl_pct <= 100) AS gt0,
        COUNT(*) FILTER (WHERE pnl_pct <= -50 AND pnl_pct > -100) AS lt50,
        COUNT(*) FILTER (WHERE pnl_pct <= -100) AS lt100
      FROM base
      `,
      [walletId, win],
    );
    const row = distQ.rows[0] || {};

    return {
      win,
      buckets: [
        { label: '>500%', count: this.safeNum(row.gt500) },
        { label: '>100%', count: this.safeNum(row.gt100) },
        { label: '>0%', count: this.safeNum(row.gt0) },
        { label: '<-50%', count: this.safeNum(row.lt50) },
        { label: '<-100%', count: this.safeNum(row.lt100) },
      ],
    };
  }

  async pnlTokens(address: string, q: any) {
    const walletId = await this.getWalletId(address);
    if (!walletId) return { success: false, error: 'wallet not found' };

    const win = this.normalizeWin(q?.win || '30d');
    const interval = this.winInterval(win);
    const limit = this.parseLimit(q?.limit, 100, 200);
    const sort = this.normalizeSort(q?.sort || 'total_pnl_desc');
    const cursor = this.parseSortCursor(q?.cursor);

    const params: any[] = [walletId, interval, win];
    let cursorClause = '';
    let cursorValue: any = null;

    if (cursor) {
      if (sort.type === 'timestamptz') {
        const d = new Date(cursor.value);
        if (!Number.isNaN(d.getTime())) cursorValue = d.toISOString();
      } else {
        const n = Number(cursor.value);
        if (Number.isFinite(n)) cursorValue = n;
      }

      if (cursorValue != null) {
        params.push(cursorValue, cursor.tokenId);
        const valIdx = params.length - 1;
        const idIdx = params.length;
        const op = sort.dir === 'DESC' ? '<' : '>';
        const cast = sort.type === 'timestamptz' ? '::timestamptz' : '::numeric';
        cursorClause = `WHERE (sort_value, token_id) ${op} ($${valIdx}${cast}, $${idIdx}::bigint)`;
      }
    }

    const sortExpr =
      sort.type === 'timestamptz'
        ? `COALESCE(${sort.expr}, '1970-01-01'::timestamptz)`
        : `COALESCE(${sort.expr}, 0)`;

    const sql = `
      WITH rate AS (
        SELECT zig_usd FROM exchange_rates ORDER BY ts DESC LIMIT 1
      ),
      latest_price AS (
        SELECT DISTINCT ON (token_id) token_id, price_in_zig
        FROM prices
        ORDER BY token_id, updated_at DESC
      ),
      txs AS (
        SELECT
          CASE WHEN direction='buy' THEN token_out_id ELSE token_in_id END AS token_id,
          COUNT(*) FILTER (WHERE direction='buy') AS txs_buy,
          COUNT(*) FILTER (WHERE direction='sell') AS txs_sell
        FROM wallet_activities
        WHERE wallet_id = $1
          AND ts >= now() - ($2::interval)
          AND action = 'swap'
          AND direction IN ('buy','sell')
        GROUP BY 1
      ),
      base AS (
        SELECT
          s.wallet_id,
          s.token_id,
          s.as_of,
          s.tx_count,
          s.volume_usd,
          s.bought_usd,
          s.sold_usd,
          s.bought_amount_base,
          s.sold_amount_base,
          s.realized_pnl_usd,
          s.avg_cost_usd,
          s.hold_duration_sec,
          t.symbol,
          t.image_uri,
          t.denom,
          t.exponent,
          COALESCE(wtp.amount_base, 0) AS amount_base,
          COALESCE(wtp.cost_basis_usd, 0) AS cost_basis_usd,
          wtp.last_trade_at,
          COALESCE(lp.price_in_zig, 0) AS price_in_zig,
          COALESCE((SELECT zig_usd FROM rate), 0) AS zig_usd,
          COALESCE(txs.txs_buy, 0) AS txs_buy,
          COALESCE(txs.txs_sell, 0) AS txs_sell
        FROM wallet_token_stats_window s
        JOIN tokens t ON t.token_id = s.token_id
        LEFT JOIN wallet_token_positions wtp ON wtp.wallet_id = s.wallet_id AND wtp.token_id = s.token_id
        LEFT JOIN latest_price lp ON lp.token_id = s.token_id
        LEFT JOIN txs ON txs.token_id = s.token_id
        WHERE s.wallet_id = $1 AND s.win = $3::wallet_window
      ),
      calc AS (
        SELECT
          *,
          (amount_base / POWER(10::numeric, COALESCE(exponent,6))) AS token_balance,
          (price_in_zig * zig_usd) AS price_usd,
          (amount_base / POWER(10::numeric, COALESCE(exponent,6))) * price_in_zig * zig_usd AS position_value_usd,
          ((amount_base / POWER(10::numeric, COALESCE(exponent,6))) * price_in_zig * zig_usd) - cost_basis_usd AS unrealized_pnl_usd,
          COALESCE(realized_pnl_usd,0) + (((amount_base / POWER(10::numeric, COALESCE(exponent,6))) * price_in_zig * zig_usd) - cost_basis_usd) AS total_pnl_usd,
          COALESCE(sold_usd,0) - COALESCE(bought_usd,0) AS net_usd,
          CASE
            WHEN sold_amount_base > 0 THEN sold_usd / NULLIF((sold_amount_base / POWER(10::numeric, COALESCE(exponent,6))), 0)
            ELSE 0
          END AS avg_sell_price_usd,
          COALESCE(last_trade_at, as_of) AS last_active
        FROM base
      ),
      ranked AS (
        SELECT *, ${sortExpr} AS sort_value
        FROM calc
      )
      SELECT *
      FROM ranked
      ${cursorClause}
      ORDER BY sort_value ${sort.dir}, token_id ${sort.dir}
      LIMIT ${limit + 1}
    `;

    const { rows } = await this.pg.query(sql, params);
    const hasMore = rows.length > limit;
    const slice = hasMore ? rows.slice(0, limit) : rows;
    const last = slice[slice.length - 1];
    const nextCursor = hasMore && last
      ? `${last.sort_value}${'|'}${last.token_id}`
      : null;

    return {
      win,
      next_cursor: nextCursor,
      items: slice.map((r: any) => ({
        token: { token_id: r.token_id, symbol: r.symbol, image: r.image_uri },
        last_active: r.last_active,
        token_balance: r.token_balance,
        position_value_usd: r.position_value_usd,
        realized_pnl_usd: r.realized_pnl_usd,
        unrealized_pnl_usd: r.unrealized_pnl_usd,
        total_pnl_usd: r.total_pnl_usd,
        bought_usd: r.bought_usd,
        sold_usd: r.sold_usd,
        net_usd: r.net_usd,
        avg_buy_price_usd: r.avg_cost_usd,
        avg_sell_price_usd: r.avg_sell_price_usd,
        txs_buy: this.safeNum(r.txs_buy),
        txs_sell: this.safeNum(r.txs_sell),
        hold_duration_sec: this.safeNum(r.hold_duration_sec),
      })),
    };
  }
}
