import { Controller, Get, Param, Query, UseInterceptors } from '@nestjs/common';
import { TradesService } from './trades.service';
// If you already have it:
import { RedisCacheInterceptor } from '../common/interceptors/redis-cache.interceptor';
import { CacheTTL } from '../common/decorators/cache-ttl.decorator';

@Controller('/trades')
@UseInterceptors(RedisCacheInterceptor) // <-- enable cache for all /trades (you can remove if you’ll add later)
export class TradesController {
  constructor(private readonly svc: TradesService) {}

  @Get()
  @CacheTTL(3)   
  all(@Query() q: any) { return this.svc.getAll(q); }

  @Get('/token/:id')
  @CacheTTL(3) 
  token(@Param('id') id: string, @Query() q: any) { return this.svc.getByToken(id, q); }

  @Get('/pool/:ref')
  @CacheTTL(5) 
  pool(@Param('ref') ref: string, @Query() q: any) { return this.svc.getByPool(ref, q); }

  @Get('/wallet/:address')
  @CacheTTL(5) 
  wallet(@Param('address') address: string, @Query() q: any) { return this.svc.getByWallet(address, q); }

  @Get('/large')
  @CacheTTL(5) 
  large(@Query() q: any) { return this.svc.getLarge(q); }

  @Get('/recent')
  @CacheTTL(5) 
  recent(@Query() q: any) { return this.svc.getRecent(q); }
}
