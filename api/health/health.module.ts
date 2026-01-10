import { Module } from '@nestjs/common';
import { TerminusModule } from '@nestjs/terminus';
import { HealthController } from './health.controller';
import { PgHealthIndicator } from '../db/pg.health';
import { RedisHealthIndicator } from '../cache/redis.health';

@Module({
  imports: [TerminusModule],
  controllers: [HealthController],
  providers: [PgHealthIndicator, RedisHealthIndicator],
})
export class HealthModule {}
