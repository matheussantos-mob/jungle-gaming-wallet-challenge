import { metrics } from '../infrastructure/metrics';

export function errorCode(error: unknown): string | undefined {
  if (!error || typeof error !== 'object') return undefined;
  const object = error as { code?: unknown; cause?: unknown; driverException?: unknown; originalError?: unknown };
  if (typeof object.code === 'string') return object.code;
  return errorCode(object.cause) ?? errorCode(object.driverException) ?? errorCode(object.originalError);
}

export async function retryTransient<T>(operation: () => Promise<T>, maxAttempts = 3): Promise<T> {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try { return await operation(); }
    catch (error) {
      const code = errorCode(error);
      if (code === '40P01' || code === '40001') metrics.lockConflicts++;
      if (!['40P01','40001','08000','08003','08006','57P01'].includes(code ?? '') || attempt === maxAttempts) throw error;
      metrics.retries++;
      await Bun.sleep(10 * 2 ** (attempt - 1) + Math.floor(Math.random() * 10));
    }
  }
  throw new Error('RETRY_EXHAUSTED');
}
