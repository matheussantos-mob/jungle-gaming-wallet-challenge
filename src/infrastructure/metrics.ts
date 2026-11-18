import type { WagerStatus } from '../domain/wager';

export class Metrics {
  readonly transactionStatuses: Record<Exclude<WagerStatus, 'PENDING' | 'FAILED'>, number> = { PENDING_REFERENCE: 0, PROCESSED: 0, REJECTED: 0 };
  duplicates = 0;
  retries = 0;
  dlq = 0;
  lockConflicts = 0;
  reconciliationDivergences = 0;
  outboxLagMs = 0;
  processingLatencyMs = { count: 0, sum: 0, max: 0 };

  recordStatus(status: WagerStatus, latencyMs: number, duplicate: boolean): void {
    if (status !== 'PENDING' && status !== 'FAILED') this.transactionStatuses[status]++;
    if (duplicate) this.duplicates++;
    this.processingLatencyMs.count++;
    this.processingLatencyMs.sum += latencyMs;
    this.processingLatencyMs.max = Math.max(this.processingLatencyMs.max, latencyMs);
  }

  snapshot() {
    return { transactionStatuses: { ...this.transactionStatuses }, duplicates: this.duplicates, retries: this.retries,
      dlq: this.dlq, lockConflicts: this.lockConflicts, reconciliationDivergences: this.reconciliationDivergences,
      outboxLagMs: this.outboxLagMs, processingLatencyMs: { ...this.processingLatencyMs } };
  }
}

export const metrics = new Metrics();
