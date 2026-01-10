import { Controller, Get } from '@nestjs/common';
import { RedisService } from './redis.service';

@Controller()
export class RediszController {
  constructor(private readonly redis: RedisService) {}

  @Get('/redisz')
  async redisz() {
    try {
      // quick health: PING
      const pong = await this.redis.raw.ping();
      // quick r/w check: set+get a temp key (auto-expires in 1s)
      const key = 'health:ping';
      await this.redis.set(key, { ok: true, ts: Date.now() }, 1);
      const val = await this.redis.get<typeof Object>(key);

      return { ok: pong === 'PONG', pong, cache: Boolean(val) };
    } catch (e: any) {
      return { ok: false, error: String(e?.message || e) };
    }
  }
}
