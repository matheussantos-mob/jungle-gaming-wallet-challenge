import { afterAll, expect, test } from 'bun:test';
import { join } from 'node:path';
import { CreateQueueCommand, DeleteMessageBatchCommand, ReceiveMessageCommand, SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { createOrm } from '../../src/infrastructure/orm';
import { WagerService } from '../../src/application/wager-service';
import { createSqsClient } from '../../src/workers/sqs-consumer';
import { OutboxPublisher } from '../../src/workers/outbox-publisher';

const orm = await createOrm();
await orm.getMigrator().up();
const client = createSqsClient();
afterAll(async () => { client.destroy(); await orm.close(true); });

test('falha após envio da outbox permite reenvio e deduplicação pelo eventId', async () => {
  const marker = crypto.randomUUID();
  const queue = await client.send(new CreateQueueCommand({ QueueName: `${marker}.fifo`, Attributes: { FifoQueue: 'true' } }));
  if (!queue.QueueUrl) throw new Error('EVENT_QUEUE_MISSING');
  const service = new WagerService(orm);
  const wallet = await service.createWallet(marker, { amount: '0.00', currency: 'BRL' });
  const result = await service.submit({ providerId: 'p', externalTransactionId: marker, playerId: marker, walletId: wallet.id, roundId: 'r', gameId: 'g', kind: 'LOSS', money: { amount: '1.00', currency: 'BRL' } }, marker);
  const db = orm.em.getConnection();
  const events = await db.execute<Array<{ id: string }>>(`SELECT id FROM outbox_messages WHERE payload->'data'->>'transactionId'=?`, [result.transactionId]);
  const eventId = events[0]!.id;
  const failAfterSend = { send: async (command: SendMessageCommand) => { await client.send(command); if (JSON.parse(command.input.MessageBody ?? '{}').eventId === eventId) throw new Error('injected after send'); } } as unknown as SQSClient;
  const failingPublisher = new OutboxPublisher(orm, failAfterSend, queue.QueueUrl);
  for (let i = 0; i < 500; i++) {
    await failingPublisher.publishOnce();
    const state = await db.execute<Array<{ attempts: number }>>('SELECT attempts FROM outbox_messages WHERE id=?', [eventId]);
    if (state[0]!.attempts > 0) break;
  }
  const pending = await db.execute<Array<{ published_at: Date | null }>>('SELECT published_at FROM outbox_messages WHERE id=?', [eventId]);
  expect(pending[0]!.published_at).toBeNull();
  await db.execute('UPDATE outbox_messages SET next_attempt_at=now() WHERE id=?', [eventId]);
  const publisher = new OutboxPublisher(orm, client, queue.QueueUrl);
  for (let i = 0; i < 500; i++) {
    await publisher.publishOnce();
    const state = await db.execute<Array<{ published_at: Date | null }>>('SELECT published_at FROM outbox_messages WHERE id=?', [eventId]);
    if (state[0]!.published_at) break;
  }
  const published = await db.execute<Array<{ published_at: Date | null }>>('SELECT published_at FROM outbox_messages WHERE id=?', [eventId]);
  expect(published[0]!.published_at).not.toBeNull();
  const processed = new Set<string>();
  for (let i = 0; i < 100 && !processed.has(eventId); i++) {
    const received = await client.send(new ReceiveMessageCommand({ QueueUrl: queue.QueueUrl, WaitTimeSeconds: 1, MaxNumberOfMessages: 10 }));
    for (const message of received.Messages ?? []) processed.add(JSON.parse(message.Body ?? '{}').eventId);
    const entries = (received.Messages ?? []).filter(message => message.ReceiptHandle).map((message, index) => ({ Id: String(index), ReceiptHandle: message.ReceiptHandle! }));
    if (entries.length) await client.send(new DeleteMessageBatchCommand({ QueueUrl: queue.QueueUrl, Entries: entries }));
  }
  expect(processed.has(eventId)).toBe(true);
});

test('dois publishers disputam a outbox sem perder evento confirmado', async () => {
  const queue = await client.send(new CreateQueueCommand({ QueueName: 'wager-events.fifo', Attributes: { FifoQueue: 'true' } }));
  if (!queue.QueueUrl) throw new Error('EVENT_QUEUE_MISSING');
  const service = new WagerService(orm);
  const marker = crypto.randomUUID();
  const wallet = await service.createWallet(marker, { amount: '10.00', currency: 'BRL' });
  const result = await service.submit({ providerId: 'p', externalTransactionId: marker, playerId: marker, walletId: wallet.id, roundId: 'r', gameId: 'g', kind: 'BET', money: { amount: '1.00', currency: 'BRL' } }, marker);
  const publisher1 = new OutboxPublisher(orm, client, queue.QueueUrl);
  const publisher2 = new OutboxPublisher(orm, client, queue.QueueUrl);
  const db = orm.em.getConnection();
  let rows: Array<{ published_at: Date | null }> = [];
  const deadline = Date.now() + 15000;
  while (Date.now() < deadline) {
    await Promise.all([publisher1.publishOnce(), publisher2.publishOnce()]);
    rows = await db.execute('SELECT published_at FROM outbox_messages WHERE payload->\'data\'->>\'transactionId\'=?', [result.transactionId]);
    if (rows[0]?.published_at) break;
  }
  expect(rows[0].published_at).not.toBeNull();
  let found = false;
  for (let attempt = 0; attempt < 30 && !found; attempt++) {
    const messages = await client.send(new ReceiveMessageCommand({ QueueUrl: queue.QueueUrl, WaitTimeSeconds: 1, MaxNumberOfMessages: 10 }));
    found = !!messages.Messages?.some(message => JSON.parse(message.Body ?? '{}').data?.transactionId === result.transactionId);
    const entries = messages.Messages?.filter(message => message.ReceiptHandle).map((message, index) => ({ Id: String(index), ReceiptHandle: message.ReceiptHandle! })) ?? [];
    if (entries.length) await client.send(new DeleteMessageBatchCommand({ QueueUrl: queue.QueueUrl, Entries: entries }));
  }
  expect(found).toBe(true);
});

test('processo publisher morre após envio e evento é reenviado sem efeito duplicado no consumidor', async () => {
  const marker = crypto.randomUUID();
  const queue = await client.send(new CreateQueueCommand({ QueueName: `${marker}.fifo`, Attributes: { FifoQueue: 'true' } }));
  if (!queue.QueueUrl) throw new Error('EVENT_QUEUE_MISSING');
  const service = new WagerService(orm);
  const wallet = await service.createWallet(marker, { amount: '0.00', currency: 'BRL' });
  const result = await service.submit({ providerId: 'p', externalTransactionId: marker, playerId: marker, walletId: wallet.id, roundId: 'r', gameId: 'g', kind: 'LOSS', money: { amount: '1.00', currency: 'BRL' } }, marker);
  const db = orm.em.getConnection();
  const events = await db.execute<Array<{ id: string }>>(`SELECT id FROM outbox_messages WHERE payload->'data'->>'transactionId'=?`, [result.transactionId]);
  const eventId = events[0]!.id;
  const child = Bun.spawn([process.execPath, join(import.meta.dir, '..', 'helpers', 'crash-after-publish.ts'), queue.QueueUrl, eventId], { stdout: 'ignore', stderr: 'pipe' });
  try {
    expect(await Promise.race([child.exited, Bun.sleep(15000).then(() => -1)])).toBe(77);
    expect((await db.execute<Array<{ published_at: Date | null }>>('SELECT published_at FROM outbox_messages WHERE id=?', [eventId]))[0]!.published_at).toBeNull();
    const publisher = new OutboxPublisher(orm, client, queue.QueueUrl);
    for (let i = 0; i < 1000; i++) {
      await publisher.publishOnce();
      if ((await db.execute<Array<{ published_at: Date | null }>>('SELECT published_at FROM outbox_messages WHERE id=?', [eventId]))[0]!.published_at) break;
    }
    expect((await db.execute<Array<{ published_at: Date | null }>>('SELECT published_at FROM outbox_messages WHERE id=?', [eventId]))[0]!.published_at).not.toBeNull();
    // Use a distinct broker deduplication id to exercise consumer idempotency.
    const payload = (await db.execute<Array<{ payload: object }>>('SELECT payload FROM outbox_messages WHERE id=?', [eventId]))[0]!.payload;
    await client.send(new SendMessageCommand({ QueueUrl: queue.QueueUrl, MessageBody: JSON.stringify(payload), MessageGroupId: wallet.id, MessageDeduplicationId: crypto.randomUUID() }));
    const processed = new Set<string>();
    let effects = 0;
    let deliveries = 0;
    for (let i = 0; i < 30 && deliveries < 2; i++) {
      const received = await client.send(new ReceiveMessageCommand({ QueueUrl: queue.QueueUrl, WaitTimeSeconds: 1, MaxNumberOfMessages: 10 }));
      for (const message of received.Messages ?? []) { const id = JSON.parse(message.Body ?? '{}').eventId; if (id === eventId) deliveries++; if (!processed.has(id)) { processed.add(id); if (id === eventId) effects++; } }
      const entries = (received.Messages ?? []).filter(message => message.ReceiptHandle).map((message, index) => ({ Id: String(index), ReceiptHandle: message.ReceiptHandle! }));
      if (entries.length) await client.send(new DeleteMessageBatchCommand({ QueueUrl: queue.QueueUrl, Entries: entries }));
    }
    expect(deliveries).toBeGreaterThanOrEqual(2);
    expect(effects).toBe(1);
  } finally { child.kill(); }
}, 30000);
