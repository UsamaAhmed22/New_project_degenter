import { IsInt, IsOptional, Max, Min } from 'class-validator';

export class GetHoldersDto {
  @IsOptional() @IsInt() @Min(0)
  offset?: number = 0;

  @IsOptional() @IsInt() @Min(1) @Max(500)
  limit?: number = 200;
}
