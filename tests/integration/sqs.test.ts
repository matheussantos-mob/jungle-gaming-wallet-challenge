import { afterAll, expect, test } from 'bun:test';
import { CreateQueueCommand, GetQueueAttributesCommand, ReceiveMessageCommand, SendMessageCommand } from '@aws-sdk/client-sqs';
import { join } from 'node:path';
import { createOrm } from '../../src/infrastructure/orm';
import { WagerService } from '../../src/application/wager-service';
import { createSqsClient, ensureQueues, WagerConsumer } from '../../src/workers/sqs-consumer';

const orm = await createOrm();
await orm.getMigrator().up();
const client = createSqsClient();
afterAll(async () => { client.destroy(); await orm.close(true); });

test('SQS real entrega BET, consumer confirma inbox e faz ack após commit', async () => {
  const queues = await ensureQueues(client);
  const service = new WagerService(orm);
  const playerId = crypto.randomUUID();
  const wallet = await service.createWallet(playerId, { amount: '10.00', currency: 'BRL' });
  const messageId = crypto.randomUUID();
  const data = { providerId: 'provider', externalTransactionId: messageId, idempotencyKey: messageId, playerId, walletId: wallet.id, roundId: 'r', gameId: 'g', kind: 'BET', money: { amount: '1.00', currency: 'BRL' } };
  await client.send(new SendMessageCommand({ QueueUrl: queues.main, MessageBody: JSON.stringify({ messageId, type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(), data }), MessageGroupId: wallet.id, MessageDeduplicationId: messageId }));
  const consumer = new WagerConsumer(client, queues.main, service);
  expect(await consumer.processOnce()).toBe(true);
  const db = orm.em.getConnection();
  const rows = await db.execute('SELECT balance FROM wallets WHERE id=?', [wallet.id]);
  expect(rows[0].balance).toBe('9.00');
  const inbox = await db.execute('SELECT processed_at FROM inbox_messages WHERE consumer_name=? AND message_id=?', ['wager-transactions',messageId]);
  expect(inbox[0].processed_at).not.toBeNull();
});

test('rejeição de negócio confirmada recebe ack e não altera saldo', async () => {
  const marker = crypto.randomUUID();
  const queue = await client.send(new CreateQueueCommand({ QueueName: `${marker}.fifo`, Attributes: { FifoQueue: 'true', VisibilityTimeout: '1' } }));
  if (!queue.QueueUrl) throw new Error('QUEUE_MISSING');
  const service = new WagerService(orm);
  const wallet = await service.createWallet(marker, { amount: '1.00', currency: 'BRL' });
  const data = { providerId: 'p', externalTransactionId: marker, idempotencyKey: marker, playerId: marker, walletId: wallet.id, roundId: 'r', gameId: 'g', kind: 'BET', money: { amount: '2.00', currency: 'BRL' } };
  await client.send(new SendMessageCommand({ QueueUrl: queue.QueueUrl, MessageBody: JSON.stringify({ messageId: marker, type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(), data }), MessageGroupId: wallet.id, MessageDeduplicationId: marker }));
  expect(await new WagerConsumer(client, queue.QueueUrl, service).processOnce()).toBe(true);
  const rows = await orm.em.getConnection().execute(`SELECT w.balance::text, t.status, i.processed_at FROM wallets w JOIN wager_transactions t ON t.wallet_id=w.id JOIN inbox_messages i ON i.message_id=? WHERE w.id=? AND t.external_transaction_id=?`, [marker, wallet.id, marker]);
  expect(rows[0].balance).toBe('1.00');
  expect(rows[0].status).toBe('REJECTED');
  expect(rows[0].processed_at).not.toBeNull();
  await Bun.sleep(1200);
  expect((await client.send(new ReceiveMessageCommand({ QueueUrl: queue.QueueUrl, WaitTimeSeconds: 1 }))).Messages ?? []).toHaveLength(0);
});

test('parada durante processamento aguarda commit e ack antes de encerrar', async () => {
  const marker = crypto.randomUUID();
  const queue = await client.send(new CreateQueueCommand({ QueueName: `${marker}.fifo`, Attributes: { FifoQueue: 'true', VisibilityTimeout: '2' } }));
  if (!queue.QueueUrl) throw new Error('QUEUE_MISSING');
  const service = new WagerService(orm);
  const wallet = await service.createWallet(marker, { amount: '10.00', currency: 'BRL' });
  const data = { providerId: 'p', externalTransactionId: marker, idempotencyKey: marker, playerId: marker, walletId: wallet.id, roundId: 'r', gameId: 'g', kind: 'BET', money: { amount: '1.00', currency: 'BRL' } };
  await client.send(new SendMessageCommand({ QueueUrl: queue.QueueUrl, MessageBody: JSON.stringify({ messageId: marker, type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(), data }), MessageGroupId: wallet.id, MessageDeduplicationId: marker }));
  let release!: () => void;
  let entered!: () => void;
  const started = new Promise<void>(resolve => { entered = resolve; });
  const gate = new Promise<void>(resolve => { release = resolve; });
  const submit = service.submit.bind(service);
  service.submit = async (...args) => { entered(); await gate; return submit(...args); };
  const consumer = new WagerConsumer(client, queue.QueueUrl, service);
  const running = consumer.run();
  await started;
  consumer.stop();
  release();
  await running;
  const rows = await orm.em.getConnection().execute('SELECT balance FROM wallets WHERE id=?', [wallet.id]);
  expect(rows[0].balance).toBe('9.00');
  await Bun.sleep(2100);
  expect((await client.send(new ReceiveMessageCommand({ QueueUrl: queue.QueueUrl, WaitTimeSeconds: 1 }))).Messages ?? []).toHaveLength(0);
}, 15_000);

test('falha transitória durante processamento retorna mensagem e recupera sem duplicar', async () => {
  const marker = crypto.randomUUID();
  const queue = await client.send(new CreateQueueCommand({ QueueName: `${marker}.fifo`, Attributes: { FifoQueue: 'true', VisibilityTimeout: '1' } }));
  if (!queue.QueueUrl) throw new Error('QUEUE_MISSING');
  const service = new WagerService(orm);
  const wallet = await service.createWallet(marker, { amount: '10.00', currency: 'BRL' });
  const data = { providerId: 'p', externalTransactionId: marker, idempotencyKey: marker, playerId: marker, walletId: wallet.id, roundId: 'r', gameId: 'g', kind: 'BET', money: { amount: '1.00', currency: 'BRL' } };
  await client.send(new SendMessageCommand({ QueueUrl: queue.QueueUrl, MessageBody: JSON.stringify({ messageId: marker, type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(), data }), MessageGroupId: wallet.id, MessageDeduplicationId: marker }));
  const submit = service.submit.bind(service);
  let attempts = 0;
  service.submit = async (...args) => { if (++attempts === 1) throw Object.assign(new Error('database unavailable'), { code: '08006' }); return submit(...args); };
  const consumer = new WagerConsumer(client, queue.QueueUrl, service);
  expect(await consumer.processOnce()).toBe(true);
  expect((await orm.em.getConnection().execute('SELECT balance FROM wallets WHERE id=?', [wallet.id]))[0].balance).toBe('10.00');
  await Bun.sleep(2300);
  expect(await consumer.processOnce()).toBe(true);
  const rows = await orm.em.getConnection().execute(`SELECT w.balance, count(l.id)::int AS debits FROM wallets w LEFT JOIN wallet_ledger_entries l ON l.wallet_id=w.id AND l.direction='DEBIT' WHERE w.id=? GROUP BY w.id`, [wallet.id]);
  expect(rows[0]).toEqual({ balance: '9.00', debits: 1 });
}, 15_000);

test('conflito de identidade é reenviado à DLQ sem débito', async () => {
  const marker = crypto.randomUUID();
  const dlq = await client.send(new CreateQueueCommand({ QueueName: `${marker}-dlq.fifo`, Attributes: { FifoQueue: 'true' } }));
  if (!dlq.QueueUrl) throw new Error('DLQ_MISSING');
  const attributes = await client.send(new GetQueueAttributesCommand({ QueueUrl: dlq.QueueUrl, AttributeNames: ['QueueArn'] }));
  const queue = await client.send(new CreateQueueCommand({ QueueName: `${marker}-main.fifo`, Attributes: { FifoQueue: 'true', RedrivePolicy: JSON.stringify({ deadLetterTargetArn: attributes.Attributes?.QueueArn, maxReceiveCount: 1 }) } }));
  if (!queue.QueueUrl) throw new Error('QUEUE_MISSING');
  const service = new WagerService(orm);
  const wallet = await service.createWallet(marker, { amount: '10.00', currency: 'BRL' });
  const data = { providerId: 'p', externalTransactionId: marker, idempotencyKey: marker, playerId: `${marker}-wrong`, walletId: wallet.id, roundId: 'r', gameId: 'g', kind: 'BET', money: { amount: '1.00', currency: 'BRL' } };
  await client.send(new SendMessageCommand({ QueueUrl: queue.QueueUrl, MessageBody: JSON.stringify({ messageId: marker, type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(), data }), MessageGroupId: wallet.id, MessageDeduplicationId: marker }));
  const consumer = new WagerConsumer(client, queue.QueueUrl, service);
  expect(await consumer.processOnce()).toBe(true);
  await Bun.sleep(2300);
  await consumer.processOnce();
  expect((await client.send(new ReceiveMessageCommand({ QueueUrl: dlq.QueueUrl, WaitTimeSeconds: 1 }))).Messages?.length).toBe(1);
  expect((await orm.em.getConnection().execute('SELECT balance FROM wallets WHERE id=?', [wallet.id]))[0].balance).toBe('10.00');
}, 15_000);

test('processo morre após commit e antes do ack; redelivery preserva um único débito', async () => {
  const marker = crypto.randomUUID();
  const queue = await client.send(new CreateQueueCommand({ QueueName: `${marker}.fifo`, Attributes: { FifoQueue: 'true', VisibilityTimeout: '1' } }));
  if (!queue.QueueUrl) throw new Error('QUEUE_MISSING');
  const service = new WagerService(orm);
  const wallet = await service.createWallet(marker, { amount: '10.00', currency: 'BRL' });
  const data = { providerId: 'provider', externalTransactionId: marker, idempotencyKey: marker, playerId: marker, walletId: wallet.id, roundId: 'r', gameId: 'g', kind: 'BET', money: { amount: '1.00', currency: 'BRL' } };
  await client.send(new SendMessageCommand({ QueueUrl: queue.QueueUrl, MessageBody: JSON.stringify({ messageId: marker, type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(), data }), MessageGroupId: wallet.id, MessageDeduplicationId: marker }));
  const child = Bun.spawn([process.execPath, join(import.meta.dir, '..', 'helpers', 'crash-after-commit.ts'), queue.QueueUrl], { stdout: 'pipe', stderr: 'pipe' });
  expect(await child.exited).toBe(77);
  await Bun.sleep(1200);
  expect(await new WagerConsumer(client, queue.QueueUrl, service).processOnce()).toBe(true);
  const db = orm.em.getConnection();
  const rows = await db.execute('SELECT count(*)::int AS count FROM wallet_ledger_entries WHERE wallet_id=? AND direction=?', [wallet.id,'DEBIT']);
  expect(rows[0].count).toBe(1);
  const balance = await db.execute('SELECT balance FROM wallets WHERE id=?', [wallet.id]);
  expect(balance[0].balance).toBe('9.00');
}, 15_000);

test('mensagem inválida excede tentativas e chega à DLQ real', async () => {
  const marker = crypto.randomUUID();
  const dlq = await client.send(new CreateQueueCommand({ QueueName: `${marker}-dlq.fifo`, Attributes: { FifoQueue: 'true' } }));
  if (!dlq.QueueUrl) throw new Error('DLQ_MISSING');
  const attrs = await client.send(new GetQueueAttributesCommand({ QueueUrl: dlq.QueueUrl, AttributeNames: ['QueueArn'] }));
  const main = await client.send(new CreateQueueCommand({ QueueName: `${marker}-main.fifo`, Attributes: { FifoQueue: 'true', VisibilityTimeout: '1', RedrivePolicy: JSON.stringify({ deadLetterTargetArn: attrs.Attributes?.QueueArn, maxReceiveCount: 2 }) } }));
  if (!main.QueueUrl) throw new Error('MAIN_MISSING');
  await client.send(new SendMessageCommand({ QueueUrl: main.QueueUrl, MessageBody: '{invalid', MessageGroupId: marker, MessageDeduplicationId: marker }));
  const consumer = new WagerConsumer(client, main.QueueUrl, new WagerService(orm));
  expect(await consumer.processOnce()).toBe(true);
  await Bun.sleep(2300);
  expect(await consumer.processOnce()).toBe(true);
  await Bun.sleep(4400);
  await consumer.processOnce();
  const moved = await client.send(new ReceiveMessageCommand({ QueueUrl: dlq.QueueUrl, WaitTimeSeconds: 1 }));
  expect(moved.Messages?.length).toBe(1);
}, 15_000);

// Run on Linux: Windows does not deliver POSIX SIGTERM to child processes.
test.skipIf(process.platform === 'win32')('SIGTERM durante processamento aguarda commit e ack', async () => {
  const marker = crypto.randomUUID();
  const queue = await client.send(new CreateQueueCommand({ QueueName: `${marker}.fifo`, Attributes: { FifoQueue: 'true', VisibilityTimeout: '10' } }));
  if (!queue.QueueUrl) throw new Error('QUEUE_MISSING');
  const service = new WagerService(orm);
  const wallet = await service.createWallet(marker, { amount: '10.00', currency: 'BRL' });
  const data = { providerId: 'p', externalTransactionId: marker, idempotencyKey: marker, playerId: marker, walletId: wallet.id, roundId: 'r', gameId: 'g', kind: 'BET', money: { amount: '1.00', currency: 'BRL' } };
  await client.send(new SendMessageCommand({ QueueUrl: queue.QueueUrl, MessageBody: JSON.stringify({ messageId: marker, type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(), data }), MessageGroupId: wallet.id, MessageDeduplicationId: marker }));
  const child = Bun.spawn([process.execPath, join(import.meta.dir, '..', 'helpers', 'sigterm-consumer.ts'), queue.QueueUrl], { stdout: 'pipe', stderr: 'pipe' });
  try {
    const reader = child.stdout.getReader();
    let output = '';
    const ready = (async () => { while (!output.includes('COMMITTED')) { const chunk = await reader.read(); if (chunk.done) throw new Error('CHILD_EXITED_BEFORE_COMMIT'); output += new TextDecoder().decode(chunk.value); } })();
    await Promise.race([ready, Bun.sleep(10000).then(() => { throw new Error('COMMIT_TIMEOUT'); })]);
    child.kill('SIGTERM');
    expect(await Promise.race([child.exited, Bun.sleep(7000).then(() => -1)])).toBe(0);
    const rows = await orm.em.getConnection().execute(`SELECT w.balance, count(l.id)::int AS debits FROM wallets w LEFT JOIN wallet_ledger_entries l ON l.wallet_id=w.id AND l.direction='DEBIT' WHERE w.id=? GROUP BY w.id`, [wallet.id]);
    expect(rows[0]).toEqual({ balance: '9.00', debits: 1 });
    expect((await client.send(new GetQueueAttributesCommand({ QueueUrl: queue.QueueUrl, AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'] }))).Attributes).toMatchObject({ ApproximateNumberOfMessages: '0', ApproximateNumberOfMessagesNotVisible: '0' });
  } finally { child.kill(); }
}, 20000);

test('envelope sem occurredAt válido não produz efeito financeiro', async () => {
  const marker = crypto.randomUUID();
  const queue = await client.send(new CreateQueueCommand({ QueueName: `${marker}.fifo`, Attributes: { FifoQueue: 'true', VisibilityTimeout: '1' } }));
  if (!queue.QueueUrl) throw new Error('QUEUE_MISSING');
  const service = new WagerService(orm);
  const wallet = await service.createWallet(marker, { amount: '10.00', currency: 'BRL' });
  const data = { providerId: 'p', externalTransactionId: marker, idempotencyKey: marker, playerId: marker, walletId: wallet.id, roundId: 'r', gameId: 'g', kind: 'BET', money: { amount: '1.00', currency: 'BRL' } };
  await client.send(new SendMessageCommand({ QueueUrl: queue.QueueUrl, MessageBody: JSON.stringify({ messageId: marker, type: 'WagerTransactionRequested', occurredAt: 'invalid', data }), MessageGroupId: wallet.id, MessageDeduplicationId: marker }));
  await new WagerConsumer(client, queue.QueueUrl, service).processOnce();
  const rows = await orm.em.getConnection().execute('SELECT balance FROM wallets WHERE id=?', [wallet.id]);
  expect(rows[0].balance).toBe('10.00');
});

// Executes the production entrypoint in a Linux subprocess; PostgreSQL keeps it inside submit until SIGTERM is delivered.
test.skipIf(process.platform === 'win32')('SIGTERM no entrypoint de produção aguarda transação bloqueada e confirma ack', async () => {
  const queues = await ensureQueues(client);
  const marker = crypto.randomUUID();
  const service = new WagerService(orm);
  const wallet = await service.createWallet(marker, { amount: '10.00', currency: 'BRL' });
  const data = { providerId: 'p', externalTransactionId: marker, idempotencyKey: marker, playerId: marker, walletId: wallet.id, roundId: 'r', gameId: 'g', kind: 'BET', money: { amount: '1.00', currency: 'BRL' } };
  let unlock!: () => void;
  let locked!: () => void;
  const gate = new Promise<void>(resolve => { unlock = resolve; });
  const ready = new Promise<void>(resolve => { locked = resolve; });
  const holding = orm.em.fork().transactional(async em => {
    await em.getConnection().execute('SELECT id FROM wallets WHERE id=? FOR UPDATE', [wallet.id], 'all', em.getTransactionContext());
    locked();
    await gate;
  });
  await ready;
  const child = Bun.spawn([process.execPath, join(import.meta.dir, '..', '..', 'src', 'workers', 'run-consumer.ts')], { stdout: 'pipe', stderr: 'pipe', env: { ...process.env, METRICS_PORT: '0' } });
  try {
    await client.send(new SendMessageCommand({ QueueUrl: queues.main, MessageBody: JSON.stringify({ messageId: marker, type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(), data }), MessageGroupId: wallet.id, MessageDeduplicationId: marker }));
    const deadline = Date.now() + 15000;
    let blocked = false;
    while (Date.now() < deadline) {
      const rows = await orm.em.getConnection().execute<Array<{ count: number }>>(`SELECT count(*)::int AS count FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'SELECT balance,currency,player_id,version,created_at,updated_at FROM wallets%'`);
      if (rows[0]!.count > 0) { blocked = true; break; }
      await Bun.sleep(100);
    }
    expect(blocked).toBe(true);
    child.kill('SIGTERM');
    await Bun.sleep(300);
    expect(child.exitCode).toBeNull();
    unlock();
    await holding;
    expect(await Promise.race([child.exited, Bun.sleep(10000).then(() => -1)])).toBe(0);
    const rows = await orm.em.getConnection().execute(`SELECT w.balance, count(l.id)::int AS debits FROM wallets w LEFT JOIN wallet_ledger_entries l ON l.wallet_id=w.id AND l.direction='DEBIT' WHERE w.id=? GROUP BY w.id`, [wallet.id]);
    expect(rows[0]).toEqual({ balance: '9.00', debits: 1 });
    const inbox = await orm.em.getConnection().execute<Array<{ processed_at: Date | null }>>('SELECT processed_at FROM inbox_messages WHERE consumer_name=? AND message_id=?', ['wager-transactions', marker]);
    expect(inbox[0]?.processed_at).not.toBeNull();
    expect((await client.send(new GetQueueAttributesCommand({ QueueUrl: queues.main, AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'] }))).Attributes).toMatchObject({ ApproximateNumberOfMessages: '0', ApproximateNumberOfMessagesNotVisible: '0' });
  } finally { unlock(); await holding; child.kill(); }
}, 30000);
