import { IsIn, IsInt, IsOptional, Max, Min } from 'class-validator';

export class GetSwapListDto {
  @IsOptional() @IsIn(['30m','1h','4h','24h'])
  bucket?: '30m'|'1h'|'4h'|'24h' = '24h';

  @IsOptional() @IsInt() @Min(0)
  offset?: number = 0;

  @IsOptional() @IsInt() @Min(1) @Max(500)
  limit?: number = 200;
}
