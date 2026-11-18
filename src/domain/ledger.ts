import { Money } from './money';

export type LedgerDirection = 'DEBIT' | 'CREDIT';

export class WalletLedgerEntry {
  private constructor(
    public readonly walletId: string,
    public readonly transactionId: string,
    public readonly direction: LedgerDirection,
    public readonly money: Money,
    public readonly balanceBefore: Money,
    public readonly balanceAfter: Money,
    public readonly createdAt: Date,
  ) {}

  static create(props: { walletId: string; transactionId: string; direction: LedgerDirection; money: Money; balanceBefore: Money; balanceAfter: Money; createdAt?: Date }): WalletLedgerEntry {
    const entry = new WalletLedgerEntry(props.walletId, props.transactionId, props.direction, props.money, props.balanceBefore, props.balanceAfter, props.createdAt ?? new Date());
    if (!entry.isBalanced() || !props.money.isPositive() || props.balanceAfter.isNegative()) throw new Error('INVALID_LEDGER_ENTRY');
    return entry;
  }

  static rehydrate(state: { walletId: string; transactionId: string; direction: LedgerDirection; money: Money; balanceBefore: Money; balanceAfter: Money; createdAt: Date }): WalletLedgerEntry {
    return new WalletLedgerEntry(state.walletId, state.transactionId, state.direction, state.money, state.balanceBefore, state.balanceAfter, state.createdAt);
  }

  isBalanced(): boolean {
    return (this.direction === 'CREDIT' ? this.balanceBefore.add(this.money) : this.balanceBefore.subtract(this.money)).equals(this.balanceAfter);
  }
}
