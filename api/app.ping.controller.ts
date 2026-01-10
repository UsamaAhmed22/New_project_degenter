import { Controller, Get } from '@nestjs/common';

@Controller()
export class AppPingController {
  @Get('/ping')
  ping() {
    return { ok: true, ts: Date.now() };
  }
}
