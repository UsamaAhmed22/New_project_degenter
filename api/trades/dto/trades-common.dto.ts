import { IsIn, IsInt, IsOptional, IsString, Max, Min } from 'class-validator';

export class TradesCommonQueryDto {
  @IsOptional() @IsIn(['buy','sell','provide','withdraw'])
  direction?: 'buy'|'sell'|'provide'|'withdraw';

  @IsOptional() @IsIn(['usd','zig'])
  unit?: 'usd'|'zig' = 'usd';

  @IsOptional() @IsString()
  tf?: string; // '24h' | '30m' | '7d' | '60d' | from/to/days also allowed

  @IsOptional() @IsString()
  from?: string;

  @IsOptional() @IsString()
  to?: string;

  @IsOptional() @IsInt() @Min(1) @Max(365)
  days?: number;

  @IsOptional() @IsIn(['true','false','1','0'])
  includeLiquidity?: 'true'|'false'|'1'|'0' = 'false';

  @IsOptional() @IsIn(['true','false','1','0','deep'])
  combineRouter?: 'true'|'false'|'1'|'0'|'deep' = 'false';

  @IsOptional() @IsIn(['shrimp','shark','whale'])
  class?: 'shrimp'|'shark'|'whale';

  @IsOptional() @IsInt()
  minValue?: number;

  @IsOptional() @IsInt()
  maxValue?: number;

  @IsOptional() @IsInt() @Min(1) @Max(1000)
  limit?: number = 100;

  @IsOptional() @IsInt() @Min(1)
  page?: number = 1;

  // optional scopers for wallet/recent routes
  @IsOptional() @IsString()
  tokenId?: string;

  @IsOptional() @IsString()
  pair?: string;

  @IsOptional() @IsString()
  poolId?: string;
}
