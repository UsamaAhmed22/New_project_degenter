import { Controller, Get, Post, Delete, Param, Body } from '@nestjs/common';
import { WatchlistService } from './watchlist.service';
import { NoCache } from '../common/decorators/cache-ttl.decorator';

@Controller('/watchlist')
@NoCache()
export class WatchlistController {
  constructor(private readonly svc: WatchlistService) {}

  @Get('/:walletId')
  list(@Param('walletId') walletId: string) {
    return this.svc.list(Number(walletId));
  }

  @Post()
  add(@Body() body: any) {
    return this.svc.add(body);
  }

  @Delete('/:id')
  remove(@Param('id') id: string) {
    return this.svc.remove(Number(id));
  }
}
