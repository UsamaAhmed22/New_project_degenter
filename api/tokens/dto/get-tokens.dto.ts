import { IsBooleanString, IsIn, IsInt, IsOptional, IsString, Max, Min } from 'class-validator';

export class GetTokensDto {
  @IsOptional() @IsString()
  search?: string;

  @IsOptional() @IsIn(['mcap','price','fdv','vol','tx','created'])
  sort?: 'mcap'|'price'|'fdv'|'vol'|'tx'|'created' = 'mcap';

  @IsOptional() @IsIn(['asc','desc'])
  dir?: 'asc'|'desc' = 'desc';

  @IsOptional() @IsIn(['best','uzig','pool','all'])
  priceSource?: 'best'|'uzig'|'pool'|'all' = 'best';

  @IsOptional() @IsIn(['30m','1h','4h','24h'])
  bucket?: '30m'|'1h'|'4h'|'24h' = '24h';

  // to match old API semantics: '1' -> true
  @IsOptional() @IsBooleanString()
  includeChange?: 'true'|'false'|'1'|'0' = 'false';

  @IsOptional() @IsBooleanString()
  includeBest?: 'true'|'false'|'1'|'0' = 'false';

  @IsOptional() @IsInt() @Min(0)
  minBestTvl?: number;

  @IsOptional() @IsInt() @Min(0)
  amt?: number;

  @IsOptional() @IsInt() @Min(0)
  offset?: number = 0;

  @IsOptional() @IsInt() @Min(1) @Max(500)
  limit?: number = 100;
}
