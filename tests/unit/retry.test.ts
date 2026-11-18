import { expect, test } from 'bun:test';
import { retryTransient } from '../../src/application/retry';
import { metrics } from '../../src/infrastructure/metrics';

test('repete deadlock um número limitado de vezes', async () => {
  let attempts = 0;
  const previousConflicts = metrics.lockConflicts;
  const result = await retryTransient(async () => { attempts++; if (attempts < 3) throw { code: '40P01' }; return 'ok'; });
  expect(result).toBe('ok');
  expect(attempts).toBe(3);
  expect(metrics.lockConflicts - previousConflicts).toBe(2);
});

test('não repete erro de negócio', async () => {
  let attempts = 0;
  await expect(retryTransient(async () => { attempts++; throw new Error('INSUFFICIENT_BALANCE'); })).rejects.toThrow('INSUFFICIENT_BALANCE');
  expect(attempts).toBe(1);
});
