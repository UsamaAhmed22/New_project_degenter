import { Injectable } from '@nestjs/common';
import { HealthIndicator, HealthIndicatorResult } from '@nestjs/terminus';
import { PgService } from './pg.service';

@Injectable()
export class PgHealthIndicator extends HealthIndicator {
  constructor(private readonly pg: PgService) { super(); }

  async isHealthy(key = 'postgres'): Promise<HealthIndicatorResult> {
    try {
      await this.pg.query('SELECT 1');
      return this.getStatus(key, true);
    } catch (e) {
      return this.getStatus(key, false, { message: (e as Error).message });
    }
  }
}
