import { expect, test } from 'bun:test';
import { Money } from '../../src/domain/money';
import { WalletBalanceChanged, WagerTransactionProcessed } from '../../src/domain/events';

test('eventos serializam envelope versionado e MoneyProps', () => {
  const event = WalletBalanceChanged.from({ walletId: 'w', transactionId: 't', direction: 'DEBIT', money: Money.from({ amount: '1.00', currency: 'BRL' }), balanceBefore: Money.from({ amount: '2.00', currency: 'BRL' }), balanceAfter: Money.from({ amount: '1.00', currency: 'BRL' }), walletVersion: 2 }, { correlationId: 'c' });
  expect(event.toJSON().eventType).toBe('WalletBalanceChanged');
  expect(event.toJSON().version).toBe(1);
  expect(event.toJSON().data.money).toEqual({ amount: '1.00', currency: 'BRL' });
  expect(typeof event.toJSON().occurredAt).toBe('string');
  const processed = WagerTransactionProcessed.from({ walletId: 'w', transactionId: 't', status: 'PROCESSED' }, { correlationId: 'c' });
  expect(processed.toJSON().eventType).toBe('WagerTransactionProcessed');
});
