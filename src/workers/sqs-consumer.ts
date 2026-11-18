import {
  ChangeMessageVisibilityCommand, CreateQueueCommand, DeleteMessageCommand,
  GetQueueAttributesCommand, ReceiveMessageCommand, SQSClient,
} from '@aws-sdk/client-sqs';
import { WagerService, type SubmitCommand } from '../application/wager-service';
import { metrics } from '../infrastructure/metrics';
import { structuredLog } from '../infrastructure/logger';
import { errorCode } from '../application/retry';

export function classifyMessageError(error: unknown): 'permanent' | 'transient' | 'unknown' {
  const message = error instanceof Error ? error.message : '';
  if (error instanceof SyntaxError || ['INVALID_MESSAGE', 'INVALID_PAYLOAD', 'INVALID_AMOUNT', 'AMOUNT_OUT_OF_RANGE', 'INVALID_CURRENCY', 'REFERENCE_REQUIRED', 'REFERENCE_NOT_ALLOWED', 'IDEMPOTENCY_KEY_REQUIRED', 'IDEMPOTENCY_CONFLICT', 'INBOX_PAYLOAD_CONFLICT', 'CURRENCY_MISMATCH', 'PLAYER_MISMATCH', 'WALLET_NOT_FOUND'].includes(message)) return 'permanent';
  const code = errorCode(error) ?? '';
  if (code.startsWith('08') || ['40P01', '40001', '57P01', '57P02', '57P03', 'ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT'].includes(code)) return 'transient';
  return 'unknown';
}

export function createSqsClient(endpoint?: string): SQSClient {
  return new SQSClient({
    region: process.env.AWS_REGION ?? 'us-east-1',
    endpoint: endpoint ?? process.env.SQS_ENDPOINT ?? 'http://localhost:4566',
    credentials: { accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test', secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test' },
  });
}

export async function ensureQueues(client: SQSClient): Promise<{ main: string; dlq: string }> {
  const dlq = await client.send(new CreateQueueCommand({ QueueName: 'wager-transactions-dlq.fifo', Attributes: { FifoQueue: 'true' } }));
  if (!dlq.QueueUrl) throw new Error('DLQ_URL_MISSING');
  const attrs = await client.send(new GetQueueAttributesCommand({ QueueUrl: dlq.QueueUrl, AttributeNames: ['QueueArn'] }));
  const arn = attrs.Attributes?.QueueArn;
  if (!arn) throw new Error('DLQ_ARN_MISSING');
  const main = await client.send(new CreateQueueCommand({ QueueName: 'wager-transactions.fifo', Attributes: {
    FifoQueue: 'true', VisibilityTimeout: '30', RedrivePolicy: JSON.stringify({ deadLetterTargetArn: arn, maxReceiveCount: 5 }),
  } }));
  if (!main.QueueUrl) throw new Error('MAIN_QUEUE_URL_MISSING');
  return { main: main.QueueUrl, dlq: dlq.QueueUrl };
}

interface RequestedMessage {
  messageId: string;
  type: 'WagerTransactionRequested';
  occurredAt: string;
  data: SubmitCommand & { idempotencyKey: string };
}

export class WagerConsumer {
  private stopping = false;
  constructor(private readonly client: SQSClient, private readonly queueUrl: string, private readonly service: WagerService, private readonly afterCommit?: () => Promise<void>) {}

  stop(): void { this.stopping = true; }

  async run(): Promise<void> {
    let failures = 0;
    while (!this.stopping) {
      try { await this.processOnce(); failures = 0; }
      catch (error) {
        failures++;
        metrics.retries++;
        structuredLog('error', 'sqs_unavailable', { errorType: error instanceof Error ? error.name : 'UNKNOWN', attempt: failures });
        if (!this.stopping) await Bun.sleep(Math.min(5000, 100 * 2 ** Math.min(failures, 5)));
      }
    }
  }

  async processOnce(): Promise<boolean> {
    const received = await this.client.send(new ReceiveMessageCommand({ QueueUrl: this.queueUrl, MaxNumberOfMessages: 1, WaitTimeSeconds: 1, MessageSystemAttributeNames: ['ApproximateReceiveCount'] }));
    const message = received.Messages?.[0];
    if (!message?.ReceiptHandle) return false;
    try {
      const payload = JSON.parse(message.Body ?? '') as RequestedMessage;
      if (!payload || payload.type !== 'WagerTransactionRequested' || typeof payload.messageId !== 'string' || !payload.messageId.trim() || payload.messageId.length > 256 || typeof payload.occurredAt !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?(?:Z|[+-]\d{2}:\d{2})$/.test(payload.occurredAt) || Number.isNaN(Date.parse(payload.occurredAt)) || !payload.data?.idempotencyKey) throw new Error('INVALID_MESSAGE');
      const result = await this.service.submit(payload.data, payload.data.idempotencyKey, { consumerName: 'wager-transactions', messageId: payload.messageId });
      await this.afterCommit?.();
      // The service returns only after its PostgreSQL commit has completed.
      await this.client.send(new DeleteMessageCommand({ QueueUrl: this.queueUrl, ReceiptHandle: message.ReceiptHandle }));
      structuredLog('info', 'sqs_acked', { correlationId: result.transactionId, messageId: payload.messageId, transactionId: result.transactionId,
        walletId: payload.data.walletId, providerId: payload.data.providerId, status: result.status });
      return true;
    } catch (error) {
      metrics.retries++;
      const count = Number(message.Attributes?.ApproximateReceiveCount ?? '1');
      const visibility = Math.min(60, 2 ** Math.min(count, 5));
      await this.client.send(new ChangeMessageVisibilityCommand({ QueueUrl: this.queueUrl, ReceiptHandle: message.ReceiptHandle, VisibilityTimeout: visibility }));
      const reason = errorCode(error) ?? (error instanceof Error && /^[A-Z][A-Z0-9_]{1,63}$/.test(error.message) ? error.message : error instanceof SyntaxError ? 'INVALID_JSON' : 'UNKNOWN');
      structuredLog('warn', 'sqs_retry', { messageId: message.MessageId, errorType: error instanceof Error ? error.name : 'UNKNOWN', errorCode: reason, classification: classifyMessageError(error), attempt: count });
      return true;
    }
  }
}
