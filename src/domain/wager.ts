import { Money } from './money';

export type WagerKind = 'OPENING' | 'BET' | 'WIN' | 'LOSS' | 'REFUND' | 'ROLLBACK';
export type WagerStatus = 'PENDING' | 'PENDING_REFERENCE' | 'PROCESSED' | 'REJECTED' | 'FAILED';

export interface WagerState {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerKind;
  money: Money;
  referenceExternalTransactionId?: string;
  status: WagerStatus;
  referenceTransactionId?: string;
  failureCode?: string;
  processedAt?: Date;
}

export class WagerTransaction {
  private constructor(
    public readonly id: string,
    public readonly providerId: string,
    public readonly externalTransactionId: string,
    public readonly idempotencyKey: string,
    public readonly payloadHash: string,
    public readonly walletId: string,
    public readonly playerId: string,
    public readonly roundId: string,
    public readonly gameId: string,
    public readonly kind: WagerKind,
    public readonly money: Money,
    public readonly referenceExternalTransactionId: string | undefined,
    private _status: WagerStatus,
    private _referenceTransactionId?: string,
    private _failureCode?: string,
    private _processedAt?: Date,
  ) {}

  static create(props: Omit<WagerState, 'status' | 'referenceTransactionId' | 'failureCode' | 'processedAt'>): WagerTransaction {
    if ((props.kind === 'REFUND' || props.kind === 'ROLLBACK') && !props.referenceExternalTransactionId) throw new Error('REFERENCE_REQUIRED');
    if (!props.money.isPositive()) throw new Error('INVALID_AMOUNT');
    return WagerTransaction.rehydrate({ ...props, status: 'PENDING' });
  }

  static rehydrate(state: WagerState): WagerTransaction {
    return new WagerTransaction(state.id, state.providerId, state.externalTransactionId, state.idempotencyKey, state.payloadHash, state.walletId, state.playerId, state.roundId, state.gameId, state.kind, state.money, state.referenceExternalTransactionId, state.status, state.referenceTransactionId, state.failureCode, state.processedAt);
  }

  get status(): WagerStatus { return this._status; }
  get referenceTransactionId(): string | undefined { return this._referenceTransactionId; }
  get failureCode(): string | undefined { return this._failureCode; }
  get processedAt(): Date | undefined { return this._processedAt; }
  isTerminal(): boolean { return this._status === 'PROCESSED' || this._status === 'REJECTED' || this._status === 'FAILED'; }
  affectsBalance(): boolean { return this.kind !== 'LOSS'; }
  requiresReference(): boolean { return this.kind === 'REFUND' || this.kind === 'ROLLBACK'; }
  matchesPayload(hash: string): boolean { return this.payloadHash === hash; }

  markProcessed(referenceId: string | undefined, at: Date): void {
    this.assertPending();
    this._status = 'PROCESSED';
    this._referenceTransactionId = referenceId;
    this._processedAt = at;
  }
  markPendingReference(): void { this.assertPending(); if (!this.requiresReference() && !(this.kind === 'WIN' && this.referenceExternalTransactionId)) throw new Error('REFERENCE_NOT_APPLICABLE'); this._status = 'PENDING_REFERENCE'; }
  reject(code: string): void { this.assertPending(); this._status = 'REJECTED'; this._failureCode = code; this._processedAt = new Date(); }
  fail(code: string): void { this.assertPending(); this._status = 'FAILED'; this._failureCode = code; this._processedAt = new Date(); }
  private assertPending(): void { if (this.isTerminal()) throw new Error('INVALID_TRANSACTION_STATE'); }
}
