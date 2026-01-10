import { IsIn, IsInt, IsOptional, Max, Min } from 'class-validator';

export class GetGainLosersDto {
  @IsOptional() @IsIn(['best','uzig','pool','all'])
  priceSource?: 'best'|'uzig'|'pool'|'all' = 'best';

  @IsOptional() @IsIn(['30m','1h','4h','24h'])
  bucket?: '30m'|'1h'|'4h'|'24h' = '24h';

  @IsOptional() @IsInt() @Min(0)
  amt?: number;

  @IsOptional() @IsInt() @Min(0)
  minBestTvl?: number;

  @IsOptional() @IsInt() @Min(0)
  offset?: number = 0;

  @IsOptional() @IsInt() @Min(1) @Max(200)
  limit?: number = 100;
}
