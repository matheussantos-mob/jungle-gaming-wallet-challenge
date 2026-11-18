import { describe, expect, test } from 'bun:test';
import { Money } from '../../src/domain/money';
import { WagerTransaction } from '../../src/domain/wager';

const base = {
  id: 't1', providerId: 'provider', externalTransactionId: 'external', idempotencyKey: 'key',
  payloadHash: 'hash', walletId: 'wallet', playerId: 'player', roundId: 'round', gameId: 'game',
  money: Money.from({ amount: '25.00', currency: 'BRL' }),
};

describe('WagerTransaction', () => {
  test('começa pendente, processa e impede transição terminal', () => {
    const tx = WagerTransaction.create({ ...base, kind: 'BET' });
    expect(tx.status).toBe('PENDING');
    tx.markProcessed(undefined, new Date());
    expect(tx.status).toBe('PROCESSED');
    expect(() => tx.reject('INSUFFICIENT_BALANCE')).toThrow('INVALID_TRANSACTION_STATE');
  });

  test('reversões exigem referência e podem aguardar', () => {
    expect(() => WagerTransaction.create({ ...base, kind: 'REFUND' })).toThrow('REFERENCE_REQUIRED');
    const tx = WagerTransaction.create({ ...base, kind: 'REFUND', referenceExternalTransactionId: 'bet1' });
    tx.markPendingReference();
    expect(tx.status).toBe('PENDING_REFERENCE');
    tx.markProcessed('original-id', new Date());
    expect(tx.referenceTransactionId).toBe('original-id');
  });

  test('LOSS não afeta saldo; rehydrate preserva terminal', () => {
    const tx = WagerTransaction.rehydrate({ ...base, kind: 'LOSS', status: 'REJECTED', failureCode: 'RULE' });
    expect(tx.affectsBalance()).toBe(false);
    expect(tx.failureCode).toBe('RULE');
    expect(() => tx.markPendingReference()).toThrow('INVALID_TRANSACTION_STATE');
  });
});
