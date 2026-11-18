import { WalletLedgerEntry } from './ledger';
import { Money } from './money';

export interface WalletState {
  id: string;
  playerId: string;
  currency: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export class Wallet {
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    private _balance: Money,
    private _version: number,
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  static open(props: { id: string; playerId: string; initialBalance: Money }): Wallet {
    if (props.initialBalance.isNegative()) throw new Error('NEGATIVE_BALANCE');
    const now = new Date();
    return new Wallet(props.id, props.playerId, props.initialBalance.currency, props.initialBalance, 1, now, now);
  }

  static rehydrate(state: WalletState): Wallet {
    return new Wallet(state.id, state.playerId, state.currency, state.balance, state.version, state.createdAt, state.updatedAt);
  }

  get balance(): Money { return this._balance; }
  get version(): number { return this._version; }
  get updatedAt(): Date { return this._updatedAt; }

  debit(transactionId: string, money: Money): WalletLedgerEntry { return this.move(transactionId, money, 'DEBIT'); }
  credit(transactionId: string, money: Money): WalletLedgerEntry { return this.move(transactionId, money, 'CREDIT'); }

  private move(transactionId: string, money: Money, direction: 'DEBIT' | 'CREDIT'): WalletLedgerEntry {
    if (money.currency !== this.currency) throw new Error('CURRENCY_MISMATCH');
    if (!money.isPositive()) throw new Error('INVALID_AMOUNT');
    const next = direction === 'CREDIT' ? this._balance.add(money) : this._balance.subtract(money);
    if (next.isNegative()) throw new Error('INSUFFICIENT_BALANCE');
    const entry = WalletLedgerEntry.create({ walletId: this.id, transactionId, direction, money, balanceBefore: this._balance, balanceAfter: next });
    this._balance = next;
    this._version++;
    this._updatedAt = new Date();
    return entry;
  }
}
