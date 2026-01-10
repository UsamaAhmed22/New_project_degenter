import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { AppPingController } from './app.ping.controller';
import { ConfigModule } from './config/config.module';
import { PgModule } from './db/pg.module';
import { DbzController } from './db/dbz.controller';
import { RedisModule } from './cache/redis.module';
import { RediszController } from './cache/redisz.controller';
import { TokensModule } from './tokens/tokens.module';
import { CacheDebugController } from './cache/cache.debug.controller';
import { HealthModule } from './health/health.module';
import { TradesModule } from './trades/trades.module';
import { SwapModule } from './swap/swap.module';
import { ZigModule } from './zig/zig.module';
import { WatchlistModule } from './watchlist/watchlist.module';
import { AlertsModule } from './alerts/alerts.module';
import { WalletsModule } from './wallets/wallets.module';

@Module({
  imports: [ConfigModule, PgModule, RedisModule, TokensModule, HealthModule, TradesModule, SwapModule, ZigModule, WatchlistModule, AlertsModule, WalletsModule],
  // ⬇️ REMOVE HealthController here. HealthModule already declares it.
  controllers: [AppController, AppPingController, DbzController, RediszController, CacheDebugController],
  providers: [AppService],
})
export class AppModule {}
