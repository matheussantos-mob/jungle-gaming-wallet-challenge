import { SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import type { MikroORM } from '@mikro-orm/core';
import type { PostgreSqlDriver } from '@mikro-orm/postgresql';
import { metrics } from '../infrastructure/metrics';
import { structuredLog } from '../infrastructure/logger';

interface PendingEvent { id: string; aggregate_id: string; payload: Record<string, unknown>; attempts: number; occurred_at: Date }

export class OutboxPublisher {
  private stopping = false;
  constructor(private readonly orm: MikroORM<PostgreSqlDriver>, private readonly client: SQSClient, private readonly queueUrl: string) {}

  stop(): void { this.stopping = true; }

  async run(): Promise<void> {
    let failures = 0;
    while (!this.stopping) {
      try {
        const published = await this.publishOnce();
        failures = 0;
        if (!published) await Bun.sleep(500);
      } catch (error) {
        failures++;
        metrics.retries++;
        structuredLog('error', 'outbox_unavailable', { errorType: error instanceof Error ? error.name : 'UNKNOWN', attempt: failures });
        if (!this.stopping) await Bun.sleep(Math.min(5000, 100 * 2 ** Math.min(failures, 5)));
      }
    }
  }

  async publishOnce(): Promise<boolean> {
    return this.orm.em.fork().transactional(async em => {
      const db = em.getConnection();
      const ctx = em.getTransactionContext();
      const rows = await db.execute<PendingEvent[]>(`SELECT id,aggregate_id,payload,attempts,occurred_at FROM outbox_messages
        WHERE published_at IS NULL AND next_attempt_at <= now()
        ORDER BY occurred_at,id FOR UPDATE SKIP LOCKED LIMIT 1`, [], 'all', ctx);
      const event = rows[0];
      if (!event) return false;
      metrics.outboxLagMs = Math.max(0, Date.now() - new Date(event.occurred_at).getTime());
      try {
        await this.client.send(new SendMessageCommand({ QueueUrl: this.queueUrl, MessageBody: JSON.stringify(event.payload), MessageGroupId: event.aggregate_id, MessageDeduplicationId: event.id }));
        await db.execute('UPDATE outbox_messages SET published_at=now(),attempts=attempts+1 WHERE id=?', [event.id], 'run', ctx);
        structuredLog('info', 'outbox_published', { eventId: event.id, aggregateId: event.aggregate_id });
        return true;
      } catch (error) {
        metrics.retries++;
        const delaySeconds = Math.min(60, 2 ** Math.min(event.attempts + 1, 5));
        await db.execute(`UPDATE outbox_messages SET attempts=attempts+1,next_attempt_at=now()+(? * interval '1 second') WHERE id=?`, [delaySeconds,event.id], 'run', ctx);
        structuredLog('warn', 'outbox_retry', { eventId: event.id, errorType: error instanceof Error ? error.name : 'UNKNOWN' });
        return true;
      }
    });
  }
}
