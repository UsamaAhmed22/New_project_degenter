import { Controller, Get } from '@nestjs/common';
import { PgService } from './pg.service';

@Controller()
export class DbzController {
  constructor(private readonly pg: PgService) {}

  @Get('/dbz')
  async dbz() {
    // run a trivial query to prove connectivity
    const res = await this.pg.query<{ one: number }>('SELECT 1 AS one');
    return { ok: true, one: res.rows[0].one };
  }
}
