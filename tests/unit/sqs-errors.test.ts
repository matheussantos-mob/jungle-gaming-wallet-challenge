import { expect, test } from 'bun:test';
import { classifyMessageError } from '../../src/workers/sqs-consumer';

test('classifica falhas permanentes e transitórias sem descartar erros desconhecidos', () => {
  expect(classifyMessageError(new SyntaxError('json'))).toBe('permanent');
  expect(classifyMessageError(new Error('IDEMPOTENCY_CONFLICT'))).toBe('permanent');
  expect(classifyMessageError(new Error('INVALID_AMOUNT'))).toBe('permanent');
  expect(classifyMessageError(Object.assign(new Error('offline'), { code: '08006' }))).toBe('transient');
  expect(classifyMessageError(new Error('unexpected'))).toBe('unknown');
});
