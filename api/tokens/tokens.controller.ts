import { Controller, Get, Param, Query, UseInterceptors } from '@nestjs/common';
import { TokensService } from './tokens.service';
import { GetTokensDto } from './dto/get-tokens.dto';
import { GetSwapListDto } from './dto/get-swap-list.dto';
import { GetPoolsDto } from './dto/get-pools.dto';
import { GetHoldersDto } from './dto/get-holders.dto';
import { GetGainLosersDto } from './dto/get-gainlosers.dto';
import { GetOhlcvAdvDto } from './dto/get-ohlcv-adv.dto';
import { RedisCacheInterceptor } from '../common/interceptors/redis-cache.interceptor';
import { CacheTTL } from '../common/decorators/cache-ttl.decorator';
import { GetBestPoolDto } from './dto/get-best-pool.dto';

@Controller('/tokens')
@UseInterceptors(RedisCacheInterceptor)
export class TokensController {
  constructor(private readonly svc: TokensService) {}

  @Get()
  @CacheTTL(5)
  list(@Query() q: GetTokensDto) {
    const includeChange =
      q.includeChange === 'true' || q.includeChange === '1';
    const includeBest =
      q.includeBest === 'true' || q.includeBest === '1';
    const minBestTvl = q.minBestTvl ? Number(q.minBestTvl) : 0;
    const amtParam = q.amt ? Number(q.amt) : undefined;
    return this.svc.listTokens({
      search: q.search,
      sort: q.sort || 'mcap',
      dir: (q.dir || 'desc'),
      priceSource: q.priceSource || 'best',
      bucket: q.bucket || '24h',
      includeChange,
      includeBest,
      minBestTvl,
      amt: amtParam,
      limit: q.limit ?? 100,
      offset: q.offset ?? 0,
    });
  }

  @Get('/gainers')
  @CacheTTL(10)
  gainers(@Query() q: GetGainLosersDto) {
    return this.svc.gainersOrLosers('gainers', q.priceSource || 'best', q.bucket || '24h', q.limit ?? 100, q.offset ?? 0, q.amt, q.minBestTvl);
  }

  @Get('/losers')
  @CacheTTL(10)
  losers(@Query() q: GetGainLosersDto) {
    return this.svc.gainersOrLosers('losers', q.priceSource || 'best', q.bucket || '24h', q.limit ?? 100, q.offset ?? 0, q.amt, q.minBestTvl);
  }

  @Get('/swap-list')
  @CacheTTL(15)
  swapList(@Query() q: GetSwapListDto) {
    return this.svc.swapList({ bucket: q.bucket || '24h', limit: q.limit ?? 200, offset: q.offset ?? 0 });
  }

  @Get('/:id')
  @CacheTTL(10)
  getOne(@Param('id') id: string, @Query('priceSource') priceSource?: string, @Query('poolId') poolId?: string) {
    return this.svc.getOne(id, { priceSource: (priceSource as any) ?? 'best', poolId });
  }

  @Get('/:id/pools')
  @CacheTTL(15)
  pools(@Param('id') id: string, @Query() q: GetPoolsDto) {
    return this.svc.poolsForToken(id, q.bucket || '24h', q.limit ?? 100, q.offset ?? 0, q.includeCaps === '1');
  }

  @Get('/:id/holders')
  @CacheTTL(60)
  holders(@Param('id') id: string, @Query() q: GetHoldersDto) {
    return this.svc.holders(id, q.limit ?? 200, q.offset ?? 0);
  }

  @Get('/:id/security')
  @CacheTTL(300)
  security(@Param('id') id: string) {
    return this.svc.security(id);
  }

  @Get('/:id/ohlcv')
  @CacheTTL(5)
  ohlcv(@Param('id') id: string, @Query() q: GetOhlcvAdvDto) {
    return this.svc.ohlcvAdvanced(id, q);
  }

  @Get('/:id/best-pool')
  @CacheTTL(5)
  bestPool(@Param('id') id: string, @Query() q: GetBestPoolDto) {
    return this.svc.getBestPool(id, { amt: q.amt, minBestTvl: q.minBestTvl ?? 0 });
  }
}
