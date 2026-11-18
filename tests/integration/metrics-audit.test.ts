import { afterAll, expect, test } from 'bun:test';
import { createOrm } from '../../src/infrastructure/orm';
import { WagerService } from '../../src/application/wager-service';
import { retryTransient } from '../../src/application/retry';
import { metrics } from '../../src/infrastructure/metrics';

const orm = await createOrm();
await orm.getMigrator().up();
afterAll(async () => { await orm.close(true); });

test('deadlock real do PostgreSQL incrementa conflitos e retries', async () => {
  const service = new WagerService(orm);
  const a = await service.createWallet(crypto.randomUUID(), { amount: '0.00', currency: 'BRL' });
  const b = await service.createWallet(crypto.randomUUID(), { amount: '0.00', currency: 'BRL' });
  let arrived = 0;
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  const previous = { conflicts: metrics.lockConflicts, retries: metrics.retries };
  const run = (first: string, second: string) => {
    let attempt = 0;
    return retryTransient(() => orm.em.fork().transactional(async em => {
      const db = em.getConnection();
      const ctx = em.getTransactionContext();
      await db.execute('SET LOCAL statement_timeout = 5000', [], 'run', ctx);
      await db.execute('SELECT id FROM wallets WHERE id=? FOR UPDATE', [first], 'all', ctx);
      if (++attempt === 1) { if (++arrived === 2) release(); await gate; }
      await db.execute('SELECT id FROM wallets WHERE id=? FOR UPDATE', [second], 'all', ctx);
    }));
  };
  await Promise.all([run(a.id, b.id), run(b.id, a.id)]);
  expect(metrics.lockConflicts - previous.conflicts).toBeGreaterThanOrEqual(1);
  expect(metrics.retries - previous.retries).toBeGreaterThanOrEqual(1);
}, 15000);

// Exercise the real service and the HTTP metrics endpoint against PostgreSQL and SQS.
test('endpoint mede estados, duplicata, latência, lag e DLQ', async () => {
  const { createHttpApp } = await import('../../src/interfaces/http/app');
  const { createSqsClient, ensureQueues } = await import('../../src/workers/sqs-consumer');
  const { SendMessageCommand } = await import('@aws-sdk/client-sqs');
  const app = await createHttpApp();
  const sqs = createSqsClient();
  await app.listen(0);
  const url = `http://127.0.0.1:${app.getHttpServer().address().port}`;
  try {
    const queues = await ensureQueues(sqs);
    const marker = crypto.randomUUID();
    const service = new WagerService(orm);
    const wallet = await service.createWallet(marker, { amount: '10.00', currency: 'BRL' });
    const before = await (await fetch(`${url}/metrics`)).json() as any;
    const base = { providerId: 'p', playerId: marker, walletId: wallet.id, roundId: 'r', gameId: 'g', money: { amount: '1.00', currency: 'BRL' } };
    const command = { ...base, kind: 'BET' as const, externalTransactionId: `${marker}-bet` };
    await service.submit(command, `${marker}-bet`);
    await service.submit(command, `${marker}-bet`);
    await service.submit({ ...base, kind: 'BET', externalTransactionId: `${marker}-reject`, money: { amount: '99.00', currency: 'BRL' } }, `${marker}-reject`);
    await service.submit({ ...base, kind: 'REFUND', externalTransactionId: `${marker}-pending`, referenceExternalTransactionId: `${marker}-missing` }, `${marker}-pending`);
    await sqs.send(new SendMessageCommand({ QueueUrl: queues.dlq, MessageBody: marker, MessageGroupId: marker, MessageDeduplicationId: marker }));
    await Bun.sleep(30);
    const after = await (await fetch(`${url}/metrics`)).json() as any;
    expect(after.transactionStatuses.PROCESSED - before.transactionStatuses.PROCESSED).toBe(2);
    expect(after.transactionStatuses.REJECTED - before.transactionStatuses.REJECTED).toBe(1);
    expect(after.transactionStatuses.PENDING_REFERENCE - before.transactionStatuses.PENDING_REFERENCE).toBe(1);
    expect(after.duplicates - before.duplicates).toBe(1);
    expect(after.processingLatencyMs.count - before.processingLatencyMs.count).toBe(4);
    expect(after.processingLatencyMs.sum).toBeGreaterThan(before.processingLatencyMs.sum);
    expect(after.processingLatencyMs.max).toBeGreaterThan(0);
    expect(after.outboxLagMs).toBeGreaterThan(0);
    expect(after.dlq).toBeGreaterThan(0);
    expect(after.dlqCollectionError).toBe(false);
  } finally { sqs.destroy(); await app.close(); }
}, 15000);

test('deadlock real sem retry também é contabilizado como conflito', async () => {
  const service = new WagerService(orm);
  const a = await service.createWallet(crypto.randomUUID(), { amount: '0.00', currency: 'BRL' });
  const b = await service.createWallet(crypto.randomUUID(), { amount: '0.00', currency: 'BRL' });
  let arrived = 0;
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  const previous = metrics.lockConflicts;
  const run = (first: string, second: string) => retryTransient(() => orm.em.fork().transactional(async em => {
    const db = em.getConnection(); const ctx = em.getTransactionContext();
    await db.execute('SET LOCAL statement_timeout = 5000', [], 'run', ctx);
    await db.execute('SELECT id FROM wallets WHERE id=? FOR UPDATE', [first], 'all', ctx);
    if (++arrived === 2) release(); await gate;
    await db.execute('SELECT id FROM wallets WHERE id=? FOR UPDATE', [second], 'all', ctx);
  }), 1);
  const outcomes = await Promise.allSettled([run(a.id, b.id), run(b.id, a.id)]);
  expect(outcomes.filter(outcome => outcome.status === 'rejected')).toHaveLength(1);
  expect(metrics.lockConflicts - previous).toBe(1);
}, 15000);

test('resolução de pendências contabiliza somente transições terminais confirmadas', async () => {
  const service = new WagerService(orm);
  const marker = crypto.randomUUID();
  const wallet = await service.createWallet(marker, { amount: '10.00', currency: 'BRL' });
  const base = { providerId: marker, playerId: marker, walletId: wallet.id, roundId: 'r', gameId: 'g', money: { amount: '1.00', currency: 'BRL' } };
  const pending = await service.submit({ ...base, kind: 'REFUND', externalTransactionId: 'refund', referenceExternalTransactionId: 'bet' }, 'refund');
  await service.submit({ ...base, kind: 'BET', externalTransactionId: 'bet' }, 'bet');
  const before = metrics.snapshot();
  await service.resolvePending();
  expect(metrics.transactionStatuses.PROCESSED - before.transactionStatuses.PROCESSED).toBe(1);
  await service.resolvePending();
  expect(metrics.transactionStatuses.PROCESSED - before.transactionStatuses.PROCESSED).toBe(1);
  const missing = await service.submit({ ...base, kind: 'REFUND', externalTransactionId: 'missing-refund', referenceExternalTransactionId: 'missing' }, 'missing-refund');
  await orm.em.getConnection().execute('UPDATE wager_transactions SET attempts=4,next_attempt_at=now() WHERE id=?', [missing.transactionId]);
  const rejected = metrics.transactionStatuses.REJECTED;
  await service.resolvePending();
  expect(metrics.transactionStatuses.REJECTED - rejected).toBe(1);
  expect((await orm.em.getConnection().execute('SELECT status FROM wager_transactions WHERE id=?', [pending.transactionId]))[0].status).toBe('PROCESSED');
});

test('métricas de status não expõem estados sem fluxo persistido', () => {
  expect(Object.keys(metrics.transactionStatuses).sort()).toEqual(['PENDING_REFERENCE', 'PROCESSED', 'REJECTED']);
});
