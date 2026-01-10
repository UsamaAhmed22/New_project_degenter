import { Module } from '@nestjs/common';
import { PgModule } from '../db/pg.module';
import { AlertsService } from './alerts.service';
import { AlertsController } from './alerts.controller';

@Module({
  imports: [PgModule],
  controllers: [AlertsController],
  providers: [AlertsService],
})
export class AlertsModule {}
