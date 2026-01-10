import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { RedisCacheInterceptor } from './common/interceptors/redis-cache.interceptor';
import { Reflector } from '@nestjs/core';
import { startWS } from './ws';
import morgan from 'morgan';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.use(morgan('dev'));
  app.enableCors();
  app.useGlobalInterceptors(new RedisCacheInterceptor(app.get(Reflector)));
  startWS(app.getHttpServer(), { path: '/ws' });
  const port = Number(process.env.API_PORT || process.env.PORT || 8003);
  await app.listen(port);
}
bootstrap();
