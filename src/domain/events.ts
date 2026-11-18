import { randomUUID } from 'node:crypto';
import type { LedgerDirection } from './ledger';
import type { Money, MoneyProps } from './money';

export interface EventContext { correlationId: string; causationId?: string }
export interface EventProps<T> extends EventContext { eventId: string; aggregateId: string; occurredAt: Date; data: T }

export abstract class IntegrationEvent<T> {
  abstract readonly eventType: string;
  abstract readonly version: number;
  readonly eventId: string;
  readonly aggregateId: string;
  readonly correlationId: string;
  readonly causationId?: string;
  readonly occurredAt: Date;
  readonly data: Readonly<T>;

  protected constructor(props: EventProps<T>) {
    this.eventId = props.eventId;
    this.aggregateId = props.aggregateId;
    this.correlationId = props.correlationId;
    this.causationId = props.causationId;
    this.occurredAt = props.occurredAt;
    this.data = Object.freeze(props.data);
  }

  toJSON(): { eventId: string; eventType: string; aggregateId: string; correlationId: string; causationId?: string; occurredAt: string; version: number; data: T } {
    return { eventId: this.eventId, eventType: this.eventType, aggregateId: this.aggregateId, correlationId: this.correlationId,
      ...(this.causationId ? { causationId: this.causationId } : {}), occurredAt: this.occurredAt.toISOString(), version: this.version, data: this.data as T };
  }
}

interface TransactionData { walletId: string; transactionId: string; status: string; failureCode?: string }

export class WagerTransactionProcessed extends IntegrationEvent<TransactionData> {
  readonly eventType = 'WagerTransactionProcessed'; readonly version = 1;
  static from(data: TransactionData, ctx: EventContext): WagerTransactionProcessed {
    return new WagerTransactionProcessed({ eventId: randomUUID(), aggregateId: data.walletId, occurredAt: new Date(), data, ...ctx });
  }
}
export class WagerTransactionRejected extends IntegrationEvent<TransactionData> {
  readonly eventType = 'WagerTransactionRejected'; readonly version = 1;
  static from(data: TransactionData, ctx: EventContext): WagerTransactionRejected {
    return new WagerTransactionRejected({ eventId: randomUUID(), aggregateId: data.walletId, occurredAt: new Date(), data, ...ctx });
  }
}
export class WagerTransactionPendingReference extends IntegrationEvent<TransactionData> {
  readonly eventType = 'WagerTransactionPendingReference'; readonly version = 1;
  static from(data: TransactionData, ctx: EventContext): WagerTransactionPendingReference {
    return new WagerTransactionPendingReference({ eventId: randomUUID(), aggregateId: data.walletId, occurredAt: new Date(), data, ...ctx });
  }
}

export interface WalletBalanceChangedData {
  walletId: string; transactionId: string; direction: LedgerDirection;
  money: MoneyProps; balanceBefore: MoneyProps; balanceAfter: MoneyProps; walletVersion: number;
}
export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  readonly eventType = 'WalletBalanceChanged'; readonly version = 1;
  static from(values: { walletId: string; transactionId: string; direction: LedgerDirection; money: Money; balanceBefore: Money; balanceAfter: Money; walletVersion: number }, ctx: EventContext): WalletBalanceChanged {
    return new WalletBalanceChanged({ eventId: randomUUID(), aggregateId: values.walletId, occurredAt: new Date(), data: {
      walletId: values.walletId, transactionId: values.transactionId, direction: values.direction, money: values.money.toJSON(),
      balanceBefore: values.balanceBefore.toJSON(), balanceAfter: values.balanceAfter.toJSON(), walletVersion: values.walletVersion,
    }, ...ctx });
  }
}
