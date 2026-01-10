import { Injectable } from '@nestjs/common';
import { PgService } from '../db/pg.service';

@Injectable()
export class WatchlistService {
  constructor(private readonly pg: PgService) {}

  async list(walletId: number) {
    const { rows } = await this.pg.query(
      `
      SELECT w.id, w.token_id, t.denom, t.symbol, w.pool_id, w.note, w.created_at
      FROM watchlist w
      LEFT JOIN tokens t ON t.token_id=w.token_id
      WHERE w.wallet_id=$1
      ORDER BY w.created_at DESC
      `,
      [walletId]
    );
    return { success: true, data: rows };
  }

  async add(body: { walletId: number; token?: string; poolId?: number; note?: string }) {
    const { walletId, token, poolId, note } = body;
    if (!walletId || (!token && !poolId)) return { success: false, error: 'walletId and token or poolId required' };
    let tokenId: number | null = null;
    if (token) {
      const r = await this.pg.query(
        `SELECT token_id
           FROM tokens
          WHERE denom=$1
             OR lower(denom)=lower($1)
             OR symbol=$1
             OR lower(symbol)=lower($1)
             OR name ILIKE $2
             OR token_id::text=$1
          ORDER BY CASE WHEN denom=$1 THEN 0 ELSE 1 END,
                   CASE WHEN lower(denom)=lower($1) THEN 0 ELSE 1 END,
                   CASE WHEN lower(symbol)=lower($1) THEN 0 ELSE 1 END,
                   token_id DESC
          LIMIT 1`,
        [token, `%${token}%`]
      );
      if (!r.rows.length) return { success: false, error: 'token not found' };
      tokenId = Number(r.rows[0].token_id);
    }
    const { rows } = await this.pg.query(
      `
      INSERT INTO watchlist(wallet_id, token_id, pool_id, note)
      VALUES ($1,$2,$3,$4)
      ON CONFLICT (wallet_id, token_id) DO NOTHING
      RETURNING *
      `,
      [walletId, tokenId, poolId ?? null, note ?? null]
    );
    return { success: true, data: rows[0] || null };
  }

  async remove(id: number) {
    await this.pg.query(`DELETE FROM watchlist WHERE id=$1`, [id]);
    return { success: true };
  }
}
