import { Controller, Get } from '@nestjs/common';
import { HealthCheck, HealthCheckService } from '@nestjs/terminus';
import { PgHealthIndicator } from '../db/pg.health';
import { RedisHealthIndicator } from '../cache/redis.health';

@Controller()
export class HealthController {
  constructor(
    private readonly health: HealthCheckService,
    private readonly pg: PgHealthIndicator,
    private readonly redis: RedisHealthIndicator,
  ) {}

  // Legacy health endpoint for compatibility
  @Get('/health')
  @HealthCheck()
  healthLegacy() {
    return this.health.check([
      () => this.pg.isHealthy(),
      () => this.redis.isHealthy(),
    ]);
  }

  // Liveness: light and instant (doesn't touch DB/Redis)
  @Get('/healthz')
  healthz() {
    return { ok: true };
  }

  // Readiness: only "ok" if external deps are ok (DB + Redis)
  @Get('/readyz')
  @HealthCheck()
  readiness() {
    return this.health.check([
      () => this.pg.isHealthy(),
      () => this.redis.isHealthy(),
    ]);
  }
}
