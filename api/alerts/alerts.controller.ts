import { Controller, Get, Post, Patch, Delete, Param, Body } from '@nestjs/common';
import { AlertsService } from './alerts.service';
import { NoCache } from '../common/decorators/cache-ttl.decorator';

@Controller('/alerts')
@NoCache()
export class AlertsController {
  constructor(private readonly svc: AlertsService) {}

  @Get('/:walletId')
  list(@Param('walletId') walletId: string) {
    return this.svc.list(Number(walletId));
  }

  @Post()
  add(@Body() body: any) {
    return this.svc.add(body);
  }

  @Patch('/:id')
  patch(@Param('id') id: string, @Body() body: any) {
    return this.svc.patch(Number(id), body);
  }

  @Delete('/:id')
  remove(@Param('id') id: string) {
    return this.svc.remove(Number(id));
  }
}
