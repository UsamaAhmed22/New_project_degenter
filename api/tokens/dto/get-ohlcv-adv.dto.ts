import { IsIn, IsInt, IsOptional, IsString, Max, Min } from 'class-validator';

export class GetOhlcvAdvDto {
  // timeframe like '1m','5m','15m','1h','1d','1w','1M','3M' etc.
  @IsOptional() @IsString()
  tf?: string = '1m';

  // 'price' | 'mcap'
  @IsOptional() @IsIn(['price','mcap'])
  mode?: 'price'|'mcap' = 'price';

  // 'native' | 'usd'
  @IsOptional() @IsIn(['native','usd'])
  unit?: 'native'|'usd' = 'native';

  // 'best' | 'uzig' | 'pool' | 'all'
  @IsOptional() @IsIn(['best','uzig','pool','all'])
  priceSource?: 'best'|'uzig'|'pool'|'all' = 'best';

  // if priceSource='pool', you can pass either poolId or pair
  @IsOptional() @IsString()
  poolId?: string;

  @IsOptional() @IsString()
  pair?: string;

  // fill strategy: 'prev'|'zero'|'none'
  @IsOptional() @IsIn(['prev','zero','none'])
  fill?: 'prev'|'zero'|'none' = 'none';

  // time window controls (ISO or relative)
  @IsOptional() @IsString()
  from?: string;

  @IsOptional() @IsString()
  to?: string;

  // alternative to from/to: span like '24h', '7d', '1M', etc.
  @IsOptional() @IsString()
  span?: string;

  // or number of bars
  @IsOptional() @IsInt() @Min(1) @Max(5000)
  window?: number;
}
