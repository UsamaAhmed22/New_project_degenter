import { Global, Module } from '@nestjs/common';
import { PgService } from './pg.service';

@Global()
@Module({
  providers: [PgService],
  exports: [PgService],
})
export class PgModule {}
