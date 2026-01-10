import { Module } from '@nestjs/common';
import { TokensController } from './tokens.controller';
import { TokensService } from './tokens.service';
import { PgModule } from '../db/pg.module';

@Module({
  imports: [PgModule],
  controllers: [TokensController],
  providers: [TokensService],
})
export class TokensModule {}
