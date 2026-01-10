import { Injectable } from '@nestjs/common';

@Injectable()
export class ConfigService {
  number(name: string, fallback?: number): number {
    const v = process.env[name];
    return v !== undefined ? Number(v) : (fallback as number);
  }
  string(name: string, fallback?: string): string {
    const v = process.env[name];
    return v !== undefined ? v : (fallback as string);
  }
  bool(name: string, fallback = false): boolean {
    const v = process.env[name];
    if (v === undefined) return fallback;
    return ['1', 'true', 'TRUE', 'yes', 'YES'].includes(v);
  }
}
