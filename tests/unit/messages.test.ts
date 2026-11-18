import { expect, test } from 'bun:test';
import { InboxMessage, OutboxMessage } from '../../src/domain/messages';
import { WagerTransactionProcessed } from '../../src/domain/events';

test('Inbox preserva estado reidratado e não processa duas vezes', () => {
  const inbox = InboxMessage.receive({ consumerName: 'worker', messageId: 'm', payloadHash: 'h' });
  expect(inbox.isProcessed()).toBe(false);
  inbox.markProcessed(new Date());
  expect(inbox.isProcessed()).toBe(true);
  expect(() => inbox.markProcessed(new Date())).toThrow('INBOX_ALREADY_PROCESSED');
  const restored = InboxMessage.rehydrate({ consumerName: 'worker', messageId: 'm', payloadHash: 'h', receivedAt: new Date(), processedAt: new Date() });
  expect(restored.isProcessed()).toBe(true);
});

test('Outbox agenda retry e termina ao publicar', () => {
  const event = WagerTransactionProcessed.from({ walletId: 'w', transactionId: 't', status: 'PROCESSED' }, { correlationId: 'c' });
  const outbox = OutboxMessage.enqueue(event);
  expect(outbox.isPending()).toBe(true);
  outbox.scheduleRetry(new Date('2026-01-01T00:00:00Z'));
  expect(outbox.attempts).toBe(1);
  expect(outbox.nextAttemptAt?.getTime()).toBeGreaterThan(new Date('2026-01-01T00:00:00Z').getTime());
  outbox.markPublished(new Date());
  expect(outbox.isPending()).toBe(false);
  expect(() => outbox.scheduleRetry(new Date())).toThrow('OUTBOX_ALREADY_PUBLISHED');
});
