import { SetMetadata } from '@nestjs/common';
export const CACHE_TTL = 'CACHE_TTL';
export const CacheTTL = (seconds: number) => SetMetadata(CACHE_TTL, seconds);
export const CACHE_BYPASS = 'CACHE_BYPASS';
export const NoCache = () => SetMetadata(CACHE_BYPASS, true);
