import { Injectable } from '@nestjs/common';
import { PgService } from '../db/pg.service';

@Injectable()
export class AlertsService {
  constructor(private readonly pg: PgService) {}

  async list(walletId: number) {
    const { rows } = await this.pg.query(
      `
      SELECT alert_id, alert_type, params, is_active, throttle_sec, last_triggered, created_at
      FROM alerts
      WHERE wallet_id=$1
      ORDER BY created_at DESC
      `,
      [walletId]
    );
    return { success: true, data: rows };
  }

  async add(body: { walletId: number; type: string; params: any; throttleSec?: number }) {
    const { walletId, type, params, throttleSec } = body;
    if (!walletId || !type || !params) return { success: false, error: 'walletId, type, params required' };
    const { rows } = await this.pg.query(
      `
      INSERT INTO alerts(wallet_id, alert_type, params, throttle_sec, is_active)
      VALUES ($1,$2,$3,$4,TRUE)
      RETURNING *
      `,
      [walletId, type, params, throttleSec || 300]
    );
    return { success: true, data: rows[0] };
  }

  async patch(id: number, body: { is_active?: boolean; params?: any; throttle_sec?: number }) {
    const { is_active, params, throttle_sec } = body;
    const { rows } = await this.pg.query(
      `
      UPDATE alerts
      SET is_active = COALESCE($2, is_active),
          params = COALESCE($3, params),
          throttle_sec = COALESCE($4, throttle_sec)
      WHERE alert_id=$1
      RETURNING *
      `,
      [id, is_active, params, throttle_sec]
    );
    return { success: true, data: rows[0] || null };
  }

  async remove(id: number) {
    await this.pg.query(`DELETE FROM alerts WHERE alert_id=$1`, [id]);
    return { success: true };
  }
}
