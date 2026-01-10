import { Controller, Get, Query, UseInterceptors } from '@nestjs/common';
import { SwapService } from './swap.service';
import { RedisCacheInterceptor } from '../common/interceptors/redis-cache.interceptor';
import { CacheTTL } from '../common/decorators/cache-ttl.decorator';

@Controller('/swap')
@UseInterceptors(RedisCacheInterceptor) // optional; enable cache for swap too
export class SwapController {
  constructor(private readonly svc: SwapService) {}

  @Get()
  @CacheTTL(5)
  route(@Query() q: any) { return this.svc.route(q); }
}
