import { Controller, Get, Param, Query, Res } from '@nestjs/common';
import { Response } from 'express';
import { WalletsService } from './wallets.service';
import { NoCache } from '../common/decorators/cache-ttl.decorator';

@Controller('/wallets')
export class WalletsController {
  constructor(private readonly svc: WalletsService) {}

  @Get('/:address/summary')
  summary(@Param('address') address: string, @Query('win') win?: string) {
    return this.svc.summary(address, win);
  }

  @Get('/:address/portfolio/value-series')
  valueSeries(@Param('address') address: string, @Query() q: any) {
    return this.svc.portfolioValueSeries(address, q);
  }

  @Get('/:address/activities')
  @NoCache()
  activities(@Param('address') address: string, @Query() q: any) {
    return this.svc.activities(address, q);
  }

  @Get('/:address/activities/export')
  @NoCache()
  async activitiesExport(
    @Param('address') address: string,
    @Query() q: any,
    @Res() res: Response,
  ) {
    const out = await this.svc.activitiesExport(address, q);
    if (!out.ok) {
      return res.status(400).json({ success: false, error: out.error || 'export failed' });
    }
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader(
      'Content-Disposition',
      `attachment; filename="${out.filename || 'wallet-activities.csv'}"`,
    );
    return res.send(out.csv || '');
  }

  @Get('/:address/portfolio/holdings')
  holdings(@Param('address') address: string) {
    return this.svc.holdings(address);
  }

  @Get('/:address/portfolio/allocation')
  allocation(@Param('address') address: string, @Query('top') top?: string) {
    return this.svc.allocation(address, top);
  }

  @Get('/:address/pnl/overview')
  pnlOverview(@Param('address') address: string, @Query('win') win?: string) {
    return this.svc.pnlOverview(address, win);
  }

  @Get('/:address/pnl/distribution')
  pnlDistribution(@Param('address') address: string, @Query('win') win?: string) {
    return this.svc.pnlDistribution(address, win);
  }

  @Get('/:address/pnl/tokens')
  pnlTokens(@Param('address') address: string, @Query() q: any) {
    return this.svc.pnlTokens(address, q);
  }
}
