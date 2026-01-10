// lib/log.js
import process from 'node:process';

const VERBOSE = (process.env.VERBOSE || '1') !== '0';

function ts() {
  return new Date().toISOString();
}

export function debug(...a) {
  if (VERBOSE) console.log('[debug]', ts(), ...a);
}
export function info(...a) {
  console.log('[info ]', ts(), ...a);
}
export function warn(...a) {
  console.warn('[warn ]', ts(), ...a);
}
export function err(...a) {
  console.error('[error]', ts(), ...a);
}

export default { debug, info, warn, err };
