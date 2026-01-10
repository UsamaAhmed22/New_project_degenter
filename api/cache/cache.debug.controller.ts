import { Controller, Get, Query } from '@nestjs/common';
import { RedisService } from './redis.service';

@Controller('/_cache') // debug-only routes
export class CacheDebugController {
  constructor(private readonly redis: RedisService) {}

  @Get('/keys')
  async keys(@Query('pattern') pattern = 'httpcache:*', @Query('limit') limit = '10') {
    const max = Number(limit) || 10;
    // KEYS is fine for quick debug; for prod tooling, use SCAN
    const keys = await this.redis.raw.keys(pattern);
    return { count: keys.length, sample: keys.slice(0, max) };
  }

  @Get('/get')
  async get(@Query('key') key: string) {
    if (!key) return { error: 'Provide ?key=' };
    const val = await this.redis.get<any>(key);
    return { key, value: val };
  }
}
