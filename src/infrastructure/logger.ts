import { AsyncLocalStorage } from 'node:async_hooks';

export const correlationContext = new AsyncLocalStorage<string>();

export function structuredLog(level: 'info' | 'warn' | 'error', event: string, fields: Record<string, string | number | boolean | undefined>): void {
  if (process.env.NODE_ENV === 'test') return;
  process.stdout.write(JSON.stringify({ level, event, timestamp: new Date().toISOString(), ...fields, ...(correlationContext.getStore() ? { correlationId: correlationContext.getStore() } : {}) }) + '\n');
}
