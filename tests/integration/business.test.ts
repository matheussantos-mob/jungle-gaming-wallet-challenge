import { afterAll, expect, test } from 'bun:test';
import { createOrm } from '../../src/infrastructure/orm';
import { WagerService } from '../../src/application/wager-service';

const orm = await createOrm();
await orm.getMigrator().up();
const service = new WagerService(orm);
afterAll(async () => { await orm.close(true); });

test('reversões de tipos distintos respeitam valor e unicidade por tipo', async () => {
  const id = crypto.randomUUID();
  const wallet = await service.createWallet(id, { amount: '100.00', currency: 'BRL' });
  const base = { providerId: 'p', playerId: id, walletId: wallet.id, roundId: 'r', gameId: 'g', money: { amount: '10.00', currency: 'BRL' } };
  const send = (suffix: string, kind: 'BET' | 'REFUND' | 'ROLLBACK', ref?: string) => service.submit({ ...base, kind, externalTransactionId: `${id}-${suffix}`, ...(ref ? { referenceExternalTransactionId: `${id}-${ref}` } : {}) }, `${id}-${suffix}`);
  await send('bet', 'BET');
  expect((await send('refund', 'REFUND', 'bet')).balance.amount).toBe('100.00');
  expect((await send('rollback-too-early', 'ROLLBACK', 'bet')).status).toBe('REJECTED');
  expect((await send('undo-refund', 'ROLLBACK', 'refund')).balance.amount).toBe('90.00');
  expect((await send('rollback-bet', 'ROLLBACK', 'bet')).balance.amount).toBe('100.00');
  expect((await send('rollback-again', 'ROLLBACK', 'bet')).status).toBe('REJECTED');
  expect((await send('refund-again', 'REFUND', 'bet')).status).toBe('REJECTED');
});

test('replay mantém resposta inicial pendente após liquidação', async () => {
  const id = crypto.randomUUID();
  const wallet = await service.createWallet(id, { amount: '10.00', currency: 'BRL' });
  const base = { providerId: 'p', playerId: id, walletId: wallet.id, roundId: 'r', gameId: 'g' };
  const command = { ...base, kind: 'WIN' as const, externalTransactionId: `${id}-win`, referenceExternalTransactionId: `${id}-bet`, money: { amount: '2.00', currency: 'BRL' } };
  const initial = await service.submit(command, command.externalTransactionId);
  await service.submit({ ...base, kind: 'BET', externalTransactionId: `${id}-bet`, money: { amount: '1.00', currency: 'BRL' } }, `${id}-bet`);
  await service.resolvePending();
  expect(await service.submit(command, command.externalTransactionId)).toEqual({ ...initial, idempotentReplay: true });
  const row = await orm.em.getConnection().execute<Array<{ status: string; result_balance: string }>>('SELECT status,result_balance FROM wager_transactions WHERE id=?', [initial.transactionId]);
  expect(row[0]).toEqual({ status: 'PROCESSED', result_balance: '11.00' });
});

test('pendência que excede limite é rejeitada sem bloquear a seguinte', async () => {
  const marker = crypto.randomUUID();
  const wallet = await service.createWallet(marker, { amount: '999999999999999999.99', currency: 'BRL' });
  const other = await service.createWallet(`${marker}-other`, { amount: '10.00', currency: 'BRL' });
  const base = { providerId: 'p', playerId: marker, walletId: wallet.id, roundId: 'r', gameId: 'g' };
  const otherBase = { ...base, playerId: `${marker}-other`, walletId: other.id };
  const first = { ...base, kind: 'WIN' as const, externalTransactionId: `${marker}-win`, referenceExternalTransactionId: `${marker}-bet`, money: { amount: '2.00', currency: 'BRL' } };
  const second = { ...otherBase, kind: 'WIN' as const, externalTransactionId: `${marker}-win2`, referenceExternalTransactionId: `${marker}-bet2`, money: { amount: '2.00', currency: 'BRL' } };
  const pending = await service.submit(first, first.externalTransactionId);
  const next = await service.submit(second, second.externalTransactionId);
  await service.submit({ ...base, kind: 'BET', externalTransactionId: `${marker}-bet`, money: { amount: '1.00', currency: 'BRL' } }, `${marker}-bet`);
  await service.submit({ ...otherBase, kind: 'BET', externalTransactionId: `${marker}-bet2`, money: { amount: '1.00', currency: 'BRL' } }, `${marker}-bet2`);
  await service.resolvePending();
  const db = orm.em.getConnection();
  expect((await db.execute('SELECT status,failure_code FROM wager_transactions WHERE id=?', [pending.transactionId]))[0]).toMatchObject({ status: 'REJECTED', failure_code: 'AMOUNT_OUT_OF_RANGE' });
  expect((await db.execute('SELECT status,result_balance FROM wager_transactions WHERE id=?', [next.transactionId]))[0]).toMatchObject({ status: 'PROCESSED', result_balance: '11.00' });
  const events = await db.execute<Array<{ count: number }>>(`SELECT count(*)::int AS count FROM outbox_messages WHERE event_type='WagerTransactionRejected' AND payload->'data'->>'transactionId'=?`, [pending.transactionId]);
  expect(events[0]?.count).toBe(1);
  const debits = await db.execute<Array<{ count: number }>>(`SELECT count(*)::int AS count FROM wallet_ledger_entries WHERE transaction_id IN (?,?)`, [pending.transactionId,next.transactionId]);
  expect(debits[0]?.count).toBe(1);
});

test('WIN anterior à BET aguarda e é liquidada quando a referência chega', async () => {
  const marker = crypto.randomUUID();
  const wallet = await service.createWallet(marker, { amount: '100.00', currency: 'BRL' });
  const base = { providerId: 'p', playerId: marker, walletId: wallet.id, roundId: 'r', gameId: 'g' };
  const command = { ...base, kind: 'WIN' as const, externalTransactionId: `${marker}-win`, referenceExternalTransactionId: `${marker}-bet`, money: { amount: '30.00', currency: 'BRL' } };
  const pending = await service.submit(command, `${marker}-win`);
  expect(pending.status).toBe('PENDING_REFERENCE');
  await service.submit({ ...base, kind: 'BET', externalTransactionId: `${marker}-bet`, money: { amount: '10.00', currency: 'BRL' } }, `${marker}-bet`);
  await service.resolvePending();
  const replay = await service.submit(command, `${marker}-win`);
  expect(replay.status).toBe('PENDING_REFERENCE');
  expect(replay.balance.amount).toBe('100.00');
  const rows = await orm.em.getConnection().execute<Array<{ count: number; total: string }>>(`SELECT count(*)::int AS count, SUM(CASE WHEN direction='CREDIT' THEN amount ELSE -amount END)::text AS total FROM wallet_ledger_entries WHERE wallet_id=?`, [wallet.id]);
  expect(rows[0]).toEqual({ count: 3, total: '120.00' });
});

test('BET, WIN, LOSS e ROLLBACK preservam saldo e ledger', async () => {
  const marker = crypto.randomUUID();
  const wallet = await service.createWallet(marker, { amount: '100.00', currency: 'BRL' });
  const base = { providerId: 'p', playerId: marker, walletId: wallet.id, roundId: 'r', gameId: 'g' };
  const send = (suffix: string, kind: 'BET' | 'WIN' | 'LOSS' | 'ROLLBACK', amount: string, ref?: string) => service.submit({ ...base, kind, externalTransactionId: `${marker}-${suffix}`, money: { amount, currency: 'BRL' }, ...(ref ? { referenceExternalTransactionId: `${marker}-${ref}` } : {}) }, `${marker}-${suffix}`);
  expect((await send('bet','BET','10.00')).balance.amount).toBe('90.00');
  expect((await send('win','WIN','30.00','bet')).balance.amount).toBe('120.00');
  expect((await send('loss','LOSS','5.00')).balance.amount).toBe('120.00');
  expect((await send('rollback-win','ROLLBACK','30.00','win')).balance.amount).toBe('90.00');
  expect((await send('rollback-bet','ROLLBACK','10.00','bet')).balance.amount).toBe('100.00');
  const db = orm.em.getConnection();
  const rows = await db.execute<Array<{ balance: string; version: number }>>('SELECT balance,version FROM wallets WHERE id=?', [wallet.id]);
  expect(rows[0].balance).toBe('100.00');
  expect(rows[0].version).toBe(5);
  const ledger = await db.execute<Array<{ count: number; calculated: string }>>(`SELECT count(*)::int AS count,
    SUM(CASE WHEN direction='CREDIT' THEN amount ELSE -amount END)::text AS calculated FROM wallet_ledger_entries WHERE wallet_id=?`, [wallet.id]);
  expect(ledger[0].count).toBe(5);
  expect(ledger[0].calculated).toBe('100.00');
});

test('ROLLBACK de WIN que tornaria saldo negativo é rejeitado com código próprio', async () => {
  const marker = crypto.randomUUID();
  const wallet = await service.createWallet(marker, { amount: '0.00', currency: 'BRL' });
  const base = { providerId: 'p', playerId: marker, walletId: wallet.id, roundId: 'r', gameId: 'g' };
  await service.submit({ ...base, kind: 'WIN', externalTransactionId: `${marker}-win`, money: { amount: '100.00', currency: 'BRL' } }, `${marker}-win`);
  await service.submit({ ...base, kind: 'BET', externalTransactionId: `${marker}-bet`, money: { amount: '100.00', currency: 'BRL' } }, `${marker}-bet`);
  const result = await service.submit({ ...base, kind: 'ROLLBACK', externalTransactionId: `${marker}-rollback`, referenceExternalTransactionId: `${marker}-win`, money: { amount: '100.00', currency: 'BRL' } }, `${marker}-rollback`);
  expect(result.status).toBe('REJECTED');
  expect(result.failureCode).toBe('REVERSAL_INSUFFICIENT_BALANCE');
  const db = orm.em.getConnection();
  const rows = await db.execute('SELECT balance FROM wallets WHERE id=?', [wallet.id]);
  expect(rows[0].balance).toBe('0.00');
  const ledger = await db.execute('SELECT count(*)::int AS count FROM wallet_ledger_entries WHERE wallet_id=?', [wallet.id]);
  expect(ledger[0].count).toBe(2);
});

test('referência inexistente expira após tentativas e publica rejeição', async () => {
  const marker = crypto.randomUUID();
  const wallet = await service.createWallet(marker, { amount: '10.00', currency: 'BRL' });
  const command = { providerId: 'p', externalTransactionId: marker, playerId: marker, walletId: wallet.id, roundId: 'r', gameId: 'g', kind: 'REFUND' as const, referenceExternalTransactionId: `${marker}-missing`, money: { amount: '1.00', currency: 'BRL' } };
  const pending = await service.submit(command, marker);
  expect(pending.status).toBe('PENDING_REFERENCE');
  const db = orm.em.getConnection();
  for (let attempt = 0; attempt < 5; attempt++) {
    await db.execute('UPDATE wager_transactions SET next_attempt_at=now() WHERE id=?', [pending.transactionId]);
    await service.resolvePending();
  }
  const replay = await service.submit(command, marker);
  expect(replay.status).toBe('PENDING_REFERENCE');
  expect((await db.execute('SELECT status,failure_code FROM wager_transactions WHERE id=?', [pending.transactionId]))[0]).toMatchObject({ status: 'REJECTED', failure_code: 'REFERENCE_NOT_FOUND' });
  const events = await db.execute<Array<{ event_type: string }>>(`SELECT event_type FROM outbox_messages WHERE payload->'data'->>'transactionId'=?`, [pending.transactionId]);
  expect(events.map(event => event.event_type).sort()).toEqual(['WagerTransactionPendingReference','WagerTransactionRejected']);
});

test('ROLLBACK de REFUND debita o crédito reembolsado uma vez', async () => {
  const marker = crypto.randomUUID();
  const wallet = await service.createWallet(marker, { amount: '100.00', currency: 'BRL' });
  const base = { providerId: 'p', playerId: marker, walletId: wallet.id, roundId: 'r', gameId: 'g', money: { amount: '25.00', currency: 'BRL' } };
  await service.submit({ ...base, kind: 'BET', externalTransactionId: `${marker}-bet` }, `${marker}-bet`);
  await service.submit({ ...base, kind: 'REFUND', externalTransactionId: `${marker}-refund`, referenceExternalTransactionId: `${marker}-bet` }, `${marker}-refund`);
  const rollback = await service.submit({ ...base, kind: 'ROLLBACK', externalTransactionId: `${marker}-rollback`, referenceExternalTransactionId: `${marker}-refund` }, `${marker}-rollback`);
  expect(rollback.status).toBe('PROCESSED');
  expect(rollback.balance.amount).toBe('75.00');
  const duplicate = await service.submit({ ...base, kind: 'ROLLBACK', externalTransactionId: `${marker}-rollback2`, referenceExternalTransactionId: `${marker}-refund` }, `${marker}-rollback2`);
  expect(duplicate.status).toBe('REJECTED');
  expect(duplicate.failureCode).toBe('ALREADY_REVERSED');
});

test('cadeia fora de ordem ROLLBACK, REFUND, BET aguarda cada referência', async () => {
  const marker = crypto.randomUUID();
  const wallet = await service.createWallet(marker, { amount: '100.00', currency: 'BRL' });
  const base = { providerId: 'p', playerId: marker, walletId: wallet.id, roundId: 'r', gameId: 'g', money: { amount: '25.00', currency: 'BRL' } };
  const rollback = await service.submit({ ...base, kind: 'ROLLBACK', externalTransactionId: `${marker}-rollback`, referenceExternalTransactionId: `${marker}-refund` }, `${marker}-rollback`);
  const refund = await service.submit({ ...base, kind: 'REFUND', externalTransactionId: `${marker}-refund`, referenceExternalTransactionId: `${marker}-bet` }, `${marker}-refund`);
  expect(rollback.status).toBe('PENDING_REFERENCE');
  expect(refund.status).toBe('PENDING_REFERENCE');
  await service.resolvePending();
  const beforeBet = await orm.em.getConnection().execute('SELECT status FROM wager_transactions WHERE id=?', [rollback.transactionId]);
  expect(beforeBet[0].status).toBe('PENDING_REFERENCE');
  await service.submit({ ...base, kind: 'BET', externalTransactionId: `${marker}-bet` }, `${marker}-bet`);
  await orm.em.getConnection().execute(`UPDATE wager_transactions SET next_attempt_at=now() WHERE status='PENDING_REFERENCE' AND wallet_id=?`, [wallet.id]);
  await service.resolvePending();
  await orm.em.getConnection().execute(`UPDATE wager_transactions SET next_attempt_at=now() WHERE status='PENDING_REFERENCE' AND wallet_id=?`, [wallet.id]);
  await service.resolvePending();
  const rows = await orm.em.getConnection().execute('SELECT status FROM wager_transactions WHERE id=?', [rollback.transactionId]);
  expect(rows[0].status).toBe('PROCESSED');
  const balance = await orm.em.getConnection().execute('SELECT balance FROM wallets WHERE id=?', [wallet.id]);
  expect(balance[0].balance).toBe('75.00');
});

test('referência pendente circular expira e gera rejeição auditável', async () => {
  const marker = crypto.randomUUID();
  const wallet = await service.createWallet(marker, { amount: '10.00', currency: 'BRL' });
  const command = { providerId: 'p', externalTransactionId: marker, playerId: marker, walletId: wallet.id, roundId: 'r', gameId: 'g', kind: 'ROLLBACK' as const, referenceExternalTransactionId: marker, money: { amount: '1.00', currency: 'BRL' } };
  const pending = await service.submit(command, marker);
  const db = orm.em.getConnection();
  for (let attempt = 0; attempt < 5; attempt++) {
    await db.execute('UPDATE wager_transactions SET next_attempt_at=now() WHERE id=?', [pending.transactionId]);
    await service.resolvePending();
  }
  const replay = await service.submit(command, marker);
  expect(replay.status).toBe('PENDING_REFERENCE');
  expect((await db.execute('SELECT status,failure_code FROM wager_transactions WHERE id=?', [pending.transactionId]))[0]).toMatchObject({ status: 'REJECTED', failure_code: 'REFERENCE_NOT_FOUND' });
  const events = await db.execute<Array<{ event_type: string }>>(`SELECT event_type FROM outbox_messages WHERE payload->'data'->>'transactionId'=?`, [pending.transactionId]);
  expect(events.map(event => event.event_type)).toContain('WagerTransactionRejected');
});
