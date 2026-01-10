import { Controller, Get, Query } from '@nestjs/common';
import { ZigService } from './zig.service';

@Controller('/zig')
export class ZigController {
  constructor(private readonly svc: ZigService) {}

  @Get('/overview')
  overview(@Query('limit') limit?: string) {
    const lim = Math.max(1, Math.min(parseInt(limit || '10', 10), 50));
    return this.svc.overview(lim);
  }
}
