import { describe, expect, test } from 'bun:test';
import { Money } from '../../src/domain/money';
import { Wallet } from '../../src/domain/wallet';

const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });

describe('Wallet', () => {
  test('abre com versão 1 e debita gerando movimento balanceado', () => {
    const wallet = Wallet.open({ id: 'w', playerId: 'p', initialBalance: brl('100.00') });
    const entry = wallet.debit('t1', brl('80.00'));
    expect(wallet.balance.toString()).toBe('20.00');
    expect(wallet.version).toBe(2);
    expect(entry.balanceBefore.toString()).toBe('100.00');
    expect(entry.balanceAfter.toString()).toBe('20.00');
    expect(entry.isBalanced()).toBe(true);
  });

  test('saldo insuficiente preserva estado', () => {
    const wallet = Wallet.open({ id: 'w', playerId: 'p', initialBalance: brl('20.00') });
    expect(() => wallet.debit('t1', brl('80.00'))).toThrow('INSUFFICIENT_BALANCE');
    expect(wallet.balance.toString()).toBe('20.00');
    expect(wallet.version).toBe(1);
  });

  test('moeda incompatível falha e rehydrate preserva versão', () => {
    const wallet = Wallet.rehydrate({ id: 'w', playerId: 'p', currency: 'BRL', balance: brl('10.00'), version: 7, createdAt: new Date(), updatedAt: new Date() });
    expect(() => wallet.credit('t1', Money.from({ amount: '1.00', currency: 'USD' }))).toThrow('CURRENCY_MISMATCH');
    expect(wallet.version).toBe(7);
  });
});
