import { IsInt, IsOptional, Min } from 'class-validator';

export class GetBestPoolDto {
  @IsOptional() @IsInt() @Min(0)
  amt?: number;

  @IsOptional() @IsInt() @Min(0)
  minBestTvl?: number;
}
