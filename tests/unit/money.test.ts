import { describe, expect, test } from 'bun:test';
import { Money } from '../../src/domain/money';

describe('Money', () => {
  test('normaliza duas casas e calcula sem perda de precisão', () => {
    const a = Money.from({ amount: '0.10', currency: 'BRL' });
    const b = Money.from({ amount: '0.20', currency: 'BRL' });
    expect(a.add(b).toJSON()).toEqual({ amount: '0.30', currency: 'BRL' });
    expect(a.toJSON()).toEqual({ amount: '0.10', currency: 'BRL' });
  });

  test('rejeita entradas inválidas sem arredondar', () => {
    for (const amount of ['NaN', 'Infinity', '1e2', '1.001', '', ' 1.00', '1,00', '9999999999999999999.00']) {
      expect(() => Money.from({ amount, currency: 'BRL' })).toThrow();
    }
  });

  test('não mistura moedas', () => {
    expect(() => Money.from({ amount: '1.00', currency: 'BRL' }).add(Money.from({ amount: '1.00', currency: 'USD' }))).toThrow();
    expect(() => Money.from({ amount: '1.00', currency: 'AAA' })).toThrow('INVALID_CURRENCY');
  });

  test('suporta grandes valores sem passar por number', () => {
    expect(Money.from({ amount: '9007199254740993.01', currency: 'BRL' }).add(Money.from({ amount: '0.01', currency: 'BRL' })).toString()).toBe('9007199254740993.02');
    expect(() => Money.from({ amount: '999999999999999999.99', currency: 'BRL' }).add(Money.from({ amount: '0.01', currency: 'BRL' }))).toThrow('AMOUNT_OUT_OF_RANGE');
  });
});
