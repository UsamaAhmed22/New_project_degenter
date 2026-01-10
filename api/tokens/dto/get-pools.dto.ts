import { IsIn, IsInt, IsOptional, Max, Min } from 'class-validator';

export class GetPoolsDto {
  @IsOptional() @IsIn(['30m','1h','4h','24h'])
  bucket?: '30m'|'1h'|'4h'|'24h' = '24h';

  @IsOptional() @IsInt() @Min(0)
  offset?: number = 0;

  @IsOptional() @IsInt() @Min(1) @Max(500)
  limit?: number = 100;

  // to mirror express optional caps include
  @IsOptional() @IsIn(['0','1'])
  includeCaps?: '0'|'1' = '0';
}
