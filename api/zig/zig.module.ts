import { Module } from '@nestjs/common';
import { PgModule } from '../db/pg.module';
import { ZigService } from './zig.service';
import { ZigController } from './zig.controller';

@Module({
  imports: [PgModule],
  controllers: [ZigController],
  providers: [ZigService],
})
export class ZigModule {}
