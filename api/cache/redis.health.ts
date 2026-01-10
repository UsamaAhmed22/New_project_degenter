import { Injectable } from '@nestjs/common';
import { HealthIndicator, HealthIndicatorResult } from '@nestjs/terminus';
import { RedisService } from './redis.service';

@Injectable()
export class RedisHealthIndicator extends HealthIndicator {
  constructor(private readonly redis: RedisService) { super(); }

  async isHealthy(key = 'redis'): Promise<HealthIndicatorResult> {
    try {
      await this.redis.raw.ping();
      return this.getStatus(key, true);
    } catch (e) {
      return this.getStatus(key, false, { message: (e as Error).message });
    }
  }
}
