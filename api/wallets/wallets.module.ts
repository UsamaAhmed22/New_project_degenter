import { Module } from '@nestjs/common';
import { PgModule } from '../db/pg.module';
import { WalletsController } from './wallets.controller';
import { WalletsService } from './wallets.service';

@Module({
  imports: [PgModule],
  controllers: [WalletsController],
  providers: [WalletsService],
})
export class WalletsModule {}
