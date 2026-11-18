import { afterAll, beforeAll, expect, spyOn, test } from 'bun:test';
import type { INestApplication } from '@nestjs/common';
import { createHttpApp } from '../../src/interfaces/http/app';
import { createOrm } from '../../src/infrastructure/orm';

let app: INestApplication;
let baseUrl: string;
beforeAll(async () => {
  app = await createHttpApp();
  await app.listen(0);
  const address = app.getHttpServer().address();
  baseUrl = `http://127.0.0.1:${address.port}`;
});
afterAll(async () => { await app?.close(); });

test('correlationId HTTP identifica logs de sucesso e erro', async () => {
  const service = app.get<import('../../src/application/wager-service').WagerService>('WAGER_SERVICE');
  const playerId = crypto.randomUUID();
  const wallet = await service.createWallet(playerId, { amount: '10.00', currency: 'BRL' });
  const oldEnv = process.env.NODE_ENV;
  const logs: string[] = [];
  const write = spyOn(process.stdout, 'write').mockImplementation(((chunk: string | Uint8Array) => { logs.push(String(chunk)); return true; }) as typeof process.stdout.write);
  process.env.NODE_ENV = 'audit';
  try {
    const id = crypto.randomUUID();
    const requestId = `request-${id}`;
    const response = await fetch(`${baseUrl}/wagering/transactions`, { method: 'POST', headers: { 'content-type': 'application/json', 'idempotency-key': id, 'x-correlation-id': requestId }, body: JSON.stringify({ providerId: 'p', externalTransactionId: id, playerId, walletId: wallet.id, roundId: 'r', gameId: 'g', kind: 'BET', money: { amount: '1.00', currency: 'BRL' } }) });
    expect(response.status).toBe(201);
    expect(response.headers.get('x-correlation-id')).toBe(requestId);
    expect(logs.map(line => JSON.parse(line)).find(line => line.event === 'wager_transaction_result')?.correlationId).toBe(requestId);
    const failure = await fetch(`${baseUrl}/wallets/missing`, { headers: { 'x-correlation-id': requestId } });
    expect((await failure.json() as { correlationId: string }).correlationId).toBe(requestId);
    expect(logs.map(line => JSON.parse(line)).find(line => line.event === 'http_request_failed')?.correlationId).toBe(requestId);
  } finally { write.mockRestore(); if (oldEnv === undefined) delete process.env.NODE_ENV; else process.env.NODE_ENV = oldEnv; }
});

test('wallet zerada reconcilia sem lançamentos', async () => {
  const created = await fetch(`${baseUrl}/wallets`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ playerId: crypto.randomUUID(), initialBalance: { amount: '0.00', currency: 'BRL' } }) });
  const wallet = await created.json() as { id: string };
  const response = await fetch(`${baseUrl}/wallets/${wallet.id}/reconciliation`, { method: 'POST' });
  expect(response.status).toBe(201);
  expect(await response.json()).toMatchObject({ storedBalance: { amount: '0.00', currency: 'BRL' }, calculatedBalance: { amount: '0.00', currency: 'BRL' }, difference: { amount: '0.00', currency: 'BRL' }, consistent: true, checkedEntries: 0 });
});

test('hash ignora campos extras em money e BET e LOSS rejeitam referências', async () => {
  const playerId = crypto.randomUUID();
  const created = await fetch(`${baseUrl}/wallets`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ playerId, initialBalance: { amount: '10.00', currency: 'BRL' } }) });
  const wallet = await created.json() as { id: string };
  const id = crypto.randomUUID();
  const command = { providerId: 'p', externalTransactionId: id, playerId, walletId: wallet.id, roundId: 'r', gameId: 'g', kind: 'BET', money: { amount: '1.00', currency: 'BRL', extra: 'ignored' } };
  const send = (body: object, key: string) => fetch(`${baseUrl}/wagering/transactions`, { method: 'POST', headers: { 'content-type': 'application/json', 'idempotency-key': key }, body: JSON.stringify(body) });
  expect((await send(command, id)).status).toBe(201);
  const replay = await send({ ...command, money: { amount: '1.00', currency: 'BRL' } }, id);
  expect(replay.status).toBe(201);
  expect((await replay.json() as { idempotentReplay: boolean }).idempotentReplay).toBe(true);
  for (const kind of ['BET', 'LOSS']) {
    const key = crypto.randomUUID();
    const invalid = await send({ ...command, kind, externalTransactionId: key, referenceExternalTransactionId: 'missing' }, key);
    expect(invalid.status).toBe(400);
    expect(await invalid.json()).toMatchObject({ code: 'REFERENCE_NOT_ALLOWED', correlationId: expect.any(String) });
  }
});

test('respostas HTTP de erro incluem correlationId', async () => {
  for (const response of [await fetch(`${baseUrl}/wallets/missing`), await fetch(`${baseUrl}/not-a-route`), await fetch(`${baseUrl}/wallets`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{' })]) {
    expect(response.status).toBeGreaterThanOrEqual(400);
    expect(await response.json()).toMatchObject({ code: expect.any(String), message: expect.any(String), correlationId: expect.any(String) });
  }
  const service = app.get<import('../../src/application/wager-service').WagerService>('WAGER_SERVICE');
  const failing = spyOn(service, 'createWallet').mockRejectedValueOnce(new Error('unexpected internal detail'));
  try {
    const response = await fetch(`${baseUrl}/wallets`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ playerId: 'p', initialBalance: { amount: '0.00', currency: 'BRL' } }) });
    expect(response.status).toBe(500);
    expect(await response.json()).toMatchObject({ code: 'INTERNAL_ERROR', message: 'INTERNAL_ERROR', correlationId: expect.any(String) });
  } finally { failing.mockRestore(); }
});

test('cursor preserva lançamentos no mesmo milissegundo', async () => {
  const orm = app.get<Awaited<ReturnType<typeof createOrm>>>('ORM');
  const service = app.get<import('../../src/application/wager-service').WagerService>('WAGER_SERVICE');
  const playerId = crypto.randomUUID();
  const wallet = await service.createWallet(playerId, { amount: '0.00', currency: 'BRL' });
  const ids = [crypto.randomUUID(), crypto.randomUUID()];
  await orm.em.fork().transactional(async em => {
    const db = em.getConnection();
    const ctx = em.getTransactionContext();
    for (let i = 0; i < 2; i++) {
      const tx = crypto.randomUUID();
      await db.execute(`INSERT INTO wager_transactions(id,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,kind,amount,currency,status) VALUES (?,?,?,?,?,?,?,?,?,'WIN',1,'BRL','PROCESSED')`, [tx, 'p', tx, tx, 'hash', wallet.id, playerId, 'r', 'g'], 'run', ctx);
      await db.execute('UPDATE wallets SET balance=?,version=version+1 WHERE id=?', [(i + 1).toFixed(2), wallet.id], 'run', ctx);
      await db.execute(`INSERT INTO wallet_ledger_entries(id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after,created_at) VALUES (?,?,?,'CREDIT',1,'BRL',?,?,?::timestamptz)`, [ids[i], wallet.id, tx, i.toFixed(2), (i + 1).toFixed(2), `2026-01-01T00:00:00.123${i === 0 ? '100' : '900'}Z`], 'run', ctx);
    }
  });
  const first = await (await fetch(`${baseUrl}/wallets/${wallet.id}/ledger?limit=1`)).json() as { items: Array<{ id: string }>; nextCursor: string };
  const second = await (await fetch(`${baseUrl}/wallets/${wallet.id}/ledger?limit=1&cursor=${encodeURIComponent(first.nextCursor)}`)).json() as { items: Array<{ id: string }> };
  expect(first.items[0]?.id).toBe(ids[1]);
  expect(second.items[0]?.id).toBe(ids[0]);
});

test('consultas HTTP retornam 503 quando o banco está indisponível', async () => {
  const orm = app.get<Awaited<ReturnType<typeof createOrm>>>('ORM');
  const execute = spyOn(orm.em.getConnection(), 'execute');
  try {
    for (const [method, path] of [
      ['GET', '/wallets/example'],
      ['GET', '/wallets/example/ledger'],
      ['GET', '/wagering/transactions/example'],
      ['GET', '/providers/example/wagering/transactions/example'],
      ['POST', '/wallets/example/reconciliation'],
      ['GET', '/metrics'],
    ]) {
      execute.mockRejectedValueOnce(Object.assign(new Error('connection lost'), { code: '08006' }));
      const response = await fetch(baseUrl + path, { method });
      expect(response.status).toBe(503);
      expect(await response.json()).toMatchObject({ code: 'DATABASE_UNAVAILABLE' });
    }
  } finally {
    execute.mockRestore();
  }
});

test('cria wallet, submete BET e devolve replay HTTP', async () => {
  const playerId = crypto.randomUUID();
  const created = await fetch(`${baseUrl}/wallets`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ playerId, initialBalance: { amount: '100.00', currency: 'BRL' } }) });
  expect(created.status).toBe(201);
  const wallet = await created.json() as { id: string; balance: { amount: string } };
  expect(wallet.balance.amount).toBe('100.00');
  const command = { providerId: 'provider', externalTransactionId: crypto.randomUUID(), playerId, walletId: wallet.id, roundId: 'r', gameId: 'g', kind: 'BET', money: { amount: '20.00', currency: 'BRL' } };
  const key = crypto.randomUUID();
  const send = () => fetch(`${baseUrl}/wagering/transactions`, { method: 'POST', headers: { 'content-type': 'application/json', 'Idempotency-Key': key }, body: JSON.stringify(command) });
  const first = await send();
  expect(first.status).toBe(201);
  const result = await first.json() as { transactionId: string; balance: { amount: string } };
  expect(result.balance.amount).toBe('80.00');
  const fetched = await (await fetch(`${baseUrl}/wagering/transactions/${result.transactionId}`)).json() as { status: string; money: { amount: string }; payload_hash?: string };
  expect(fetched.status).toBe('PROCESSED');
  expect(fetched.money.amount).toBe('20.00');
  expect(fetched.payload_hash).toBeUndefined();
  const byExternal = await (await fetch(`${baseUrl}/providers/provider/wagering/transactions/${command.externalTransactionId}`)).json() as { id: string };
  expect(byExternal.id).toBe(result.transactionId);
  const second = await send();
  const replay = await second.json() as { transactionId: string; idempotentReplay: boolean };
  expect(replay.transactionId).toBe(result.transactionId);
  expect(replay.idempotentReplay).toBe(true);
  const reordered = await fetch(`${baseUrl}/wagering/transactions`, { method: 'POST', headers: { 'content-type': 'application/json', 'Idempotency-Key': key }, body: JSON.stringify({ ...command, money: { currency: 'BRL', amount: '20.00' } }) });
  expect((await reordered.json() as { transactionId: string }).transactionId).toBe(result.transactionId);
  const metrics = await (await fetch(`${baseUrl}/metrics`)).json() as { transactionStatuses: { PROCESSED: number }; duplicates: number; processingLatencyMs: { count: number } };
  expect(metrics.transactionStatuses.PROCESSED).toBeGreaterThan(0);
  expect(metrics.duplicates).toBeGreaterThan(0);
  expect(metrics.processingLatencyMs.count).toBeGreaterThan(0);
});

test('readiness verifica PostgreSQL e SQS', async () => {
  const response = await fetch(`${baseUrl}/health/ready`);
  expect(response.status).toBe(200);
  expect(await response.json()).toEqual({ status: 'ok' });
  const unavailable = await createHttpApp({ sqsEndpoint: 'http://127.0.0.1:1' });
  try {
    await unavailable.listen(0);
    const address = unavailable.getHttpServer().address();
    const failed = await fetch(`http://127.0.0.1:${address.port}/health/ready`);
    expect(failed.status).toBe(503);
    const live = await fetch(`http://127.0.0.1:${address.port}/health/live`);
    expect(live.status).toBe(200);
    const metricsResponse = await fetch(`http://127.0.0.1:${address.port}/metrics`);
    const snapshot = await metricsResponse.json() as { dlq: number | null; dlqCollectionError: boolean };
    expect(snapshot.dlq).toBeNull();
    expect(snapshot.dlqCollectionError).toBe(true);
  } finally { await unavailable.close(); }
});

test('reconciliação sinaliza divergência e incrementa métrica real', async () => {
  const playerId = crypto.randomUUID();
  const created = await fetch(`${baseUrl}/wallets`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ playerId, initialBalance: { amount: '10.00', currency: 'BRL' } }) });
  const wallet = await created.json() as { id: string };
  const orm = await createOrm();
  try { await orm.em.getConnection().execute('UPDATE wallets SET balance=?,version=version+1 WHERE id=?', ['20.00',wallet.id]); }
  finally { await orm.close(true); }
  const response = await fetch(`${baseUrl}/wallets/${wallet.id}/reconciliation`, { method: 'POST' });
  const body = await response.json() as { consistent: boolean; difference: { amount: string } };
  expect(body.consistent).toBe(false);
  expect(body.difference.amount).toBe('10.00');
  const metrics = await (await fetch(`${baseUrl}/metrics`)).json() as { reconciliationDivergences: number };
  expect(metrics.reconciliationDivergences).toBeGreaterThan(0);
  const restore = await createOrm();
  try { await restore.em.getConnection().execute('UPDATE wallets SET balance=?,version=version+1 WHERE id=?', ['10.00',wallet.id]); }
  finally { await restore.close(true); }
});

test('cursor opaco pagina ledger sem repetir item após nova operação', async () => {
  const playerId = crypto.randomUUID();
  const created = await fetch(`${baseUrl}/wallets`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ playerId, initialBalance: { amount: '10.00', currency: 'BRL' } }) });
  const wallet = await created.json() as { id: string };
  const send = (kind: 'BET' | 'WIN') => fetch(`${baseUrl}/wagering/transactions`, { method: 'POST', headers: { 'content-type': 'application/json', 'Idempotency-Key': crypto.randomUUID() }, body: JSON.stringify({ providerId: 'p', externalTransactionId: crypto.randomUUID(), playerId, walletId: wallet.id, roundId: 'r', gameId: 'g', kind, money: { amount: '1.00', currency: 'BRL' } }) });
  await send('BET');
  await send('WIN');
  const firstResponse = await fetch(`${baseUrl}/wallets/${wallet.id}/ledger?limit=2`);
  expect(firstResponse.status).toBe(200);
  const first = await firstResponse.json() as { items: Array<{ id: string }>; nextCursor: string };
  expect(first.items.length).toBe(2);
  expect(typeof first.nextCursor).toBe('string');
  await send('BET');
  const second = await (await fetch(`${baseUrl}/wallets/${wallet.id}/ledger?limit=2&cursor=${encodeURIComponent(first.nextCursor)}`)).json() as { items: Array<{ id: string }> };
  expect(second.items.length).toBe(1);
  expect(first.items.some(item => item.id === second.items[0]?.id)).toBe(false);
  const invalid = await fetch(`${baseUrl}/wallets/${wallet.id}/ledger?cursor=${encodeURIComponent(Buffer.from('null').toString('base64url'))}`);
  expect(invalid.status).toBe(400);
});

test('HTTP separa validação, conflito, rejeição e referência pendente', async () => {
  const playerId = crypto.randomUUID();
  const created = await fetch(`${baseUrl}/wallets`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ playerId, initialBalance: { amount: '10.00', currency: 'BRL' } }) });
  const wallet = await created.json() as { id: string };
  const key = crypto.randomUUID();
  const command = { providerId: 'p', externalTransactionId: key, playerId, walletId: wallet.id, roundId: 'r', gameId: 'g', kind: 'BET', money: { amount: '20.00', currency: 'BRL' } };
  const send = (body: object, headers: Record<string,string> = {}) => fetch(`${baseUrl}/wagering/transactions`, { method: 'POST', headers: { 'content-type': 'application/json', ...headers }, body: JSON.stringify(body) });
  expect((await send(command)).status).toBe(400);
  expect((await send({ ...command, providerId: '' }, { 'Idempotency-Key': crypto.randomUUID() })).status).toBe(400);
  expect((await send(command, { 'Idempotency-Key': key })).status).toBe(422);
  expect((await send({ ...command, money: { amount: '1.00', currency: 'BRL' } }, { 'Idempotency-Key': key })).status).toBe(409);
  const pending = { ...command, kind: 'REFUND', externalTransactionId: `${key}-refund`, referenceExternalTransactionId: 'missing', money: { amount: '1.00', currency: 'BRL' } };
  expect((await send(pending, { 'Idempotency-Key': `${key}-refund` })).status).toBe(202);
});

test('HTTP rejeita campos e valores monetários inválidos', async () => {
  const playerId = crypto.randomUUID();
  const created = await fetch(`${baseUrl}/wallets`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ playerId, initialBalance: { amount: '10.00', currency: 'BRL' } }) });
  const wallet = await created.json() as { id: string };
  const command = { providerId: 'p', externalTransactionId: crypto.randomUUID(), playerId, walletId: wallet.id, roundId: 'r', gameId: 'g', kind: 'BET', money: { amount: '1.00', currency: 'BRL' } };
  for (const invalid of [
    { ...command, providerId: ' ' }, { ...command, providerId: 'x'.repeat(129) },
    { ...command, gameId: '' }, { ...command, roundId: null },
    { ...command, money: { amount: '-1.00', currency: 'BRL' } },
    { ...command, money: { amount: '1.001', currency: 'BRL' } },
    { ...command, money: { amount: '1e2', currency: 'BRL' } },
    { ...command, money: { amount: 1, currency: 'BRL' } },
    { ...command, money: { amount: '1.00', currency: 'ZZZ' } },
    { ...command, money: { amount: '1.00', currency: 'USD' } },
    { ...command, kind: 'REFUND' },
  ]) {
    const response = await fetch(`${baseUrl}/wagering/transactions`, { method: 'POST', headers: { 'content-type': 'application/json', 'Idempotency-Key': crypto.randomUUID() }, body: JSON.stringify(invalid) });
    expect(response.status).toBe(400);
  }
  expect((await fetch(`${baseUrl}/wallets/${wallet.id}/ledger?limit=0`)).status).toBe(400);
  expect((await fetch(`${baseUrl}/wallets/${wallet.id}/ledger?cursor=invalid`)).status).toBe(400);
  expect((await fetch(`${baseUrl}/wallets/nonexistent`)).status).toBe(404);
});

test('replay idempotente retorna conflito antes de validar wallet removida/inexistente', async () => {
  const playerId = crypto.randomUUID();
  const created = await fetch(`${baseUrl}/wallets`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ playerId, initialBalance: { amount: '10.00', currency: 'BRL' } }) });
  const wallet = await created.json() as { id: string };
  const command = { providerId: 'p', externalTransactionId: crypto.randomUUID(), playerId, walletId: wallet.id, roundId: 'r', gameId: 'g', kind: 'BET', money: { amount: '1.00', currency: 'BRL' } };
  const key = crypto.randomUUID();
  const send = (body: object) => fetch(`${baseUrl}/wagering/transactions`, { method: 'POST', headers: { 'content-type': 'application/json', 'Idempotency-Key': key }, body: JSON.stringify(body) });
  expect((await send(command)).status).toBe(201);
  const replay = await send({ ...command, walletId: crypto.randomUUID() });
  expect(replay.status).toBe(409);
  expect((await replay.json() as { code: string }).code).toBe('IDEMPOTENCY_CONFLICT');
});
