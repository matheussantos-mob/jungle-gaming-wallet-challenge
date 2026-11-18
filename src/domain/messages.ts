import { IntegrationEvent } from './events';

export interface InboxState { consumerName: string; messageId: string; payloadHash: string; receivedAt: Date; processedAt?: Date }

export class InboxMessage {
  private constructor(
    public readonly consumerName: string,
    public readonly messageId: string,
    public readonly payloadHash: string,
    public readonly receivedAt: Date,
    private _processedAt?: Date,
  ) {}
  static receive(props: { consumerName: string; messageId: string; payloadHash: string }): InboxMessage {
    return new InboxMessage(props.consumerName, props.messageId, props.payloadHash, new Date());
  }
  static rehydrate(state: InboxState): InboxMessage {
    return new InboxMessage(state.consumerName, state.messageId, state.payloadHash, state.receivedAt, state.processedAt);
  }
  get processedAt(): Date | undefined { return this._processedAt; }
  isProcessed(): boolean { return this._processedAt !== undefined; }
  markProcessed(at: Date): void {
    if (this.isProcessed()) throw new Error('INBOX_ALREADY_PROCESSED');
    this._processedAt = at;
  }
}

export interface OutboxState {
  id: string; aggregateId: string; eventType: string; payload: Readonly<Record<string, unknown>>;
  occurredAt: Date; attempts: number; nextAttemptAt?: Date; publishedAt?: Date;
}

export class OutboxMessage {
  private constructor(
    public readonly id: string,
    public readonly aggregateId: string,
    public readonly eventType: string,
    public readonly payload: Readonly<Record<string, unknown>>,
    public readonly occurredAt: Date,
    private _attempts: number,
    private _nextAttemptAt?: Date,
    private _publishedAt?: Date,
  ) {}
  static enqueue(event: IntegrationEvent<unknown>): OutboxMessage {
    return new OutboxMessage(event.eventId, event.aggregateId, event.eventType, Object.freeze(event.toJSON()), event.occurredAt, 0);
  }
  static rehydrate(state: OutboxState): OutboxMessage {
    return new OutboxMessage(state.id, state.aggregateId, state.eventType, state.payload, state.occurredAt, state.attempts, state.nextAttemptAt, state.publishedAt);
  }
  get attempts(): number { return this._attempts; }
  get nextAttemptAt(): Date | undefined { return this._nextAttemptAt; }
  get publishedAt(): Date | undefined { return this._publishedAt; }
  isPending(): boolean { return this._publishedAt === undefined; }
  isDue(now: Date): boolean { return this.isPending() && (!this._nextAttemptAt || this._nextAttemptAt <= now); }
  markPublished(at: Date): void {
    if (!this.isPending()) throw new Error('OUTBOX_ALREADY_PUBLISHED');
    this._publishedAt = at;
  }
  scheduleRetry(now: Date): void {
    if (!this.isPending()) throw new Error('OUTBOX_ALREADY_PUBLISHED');
    this._attempts++;
    this._nextAttemptAt = new Date(now.getTime() + Math.min(60, 2 ** Math.min(this._attempts, 5)) * 1000);
  }
}
