import { expect, test } from 'bun:test';
import { canonicalHash } from '../../src/application/canonical-hash';

test('hash canônico independe da ordem das chaves em objetos aninhados', () => {
  const a = { providerId: 'p', money: { amount: '1.00', currency: 'BRL' } };
  const b = { money: { currency: 'BRL', amount: '1.00' }, providerId: 'p' };
  expect(canonicalHash(a)).toBe(canonicalHash(b));
  expect(canonicalHash(a)).not.toBe(canonicalHash({ ...a, money: { amount: '2.00', currency: 'BRL' } }));
});
