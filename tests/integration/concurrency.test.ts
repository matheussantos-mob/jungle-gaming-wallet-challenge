import { afterAll, beforeAll, expect, test } from 'bun:test';
import { createOrm } from '../../src/infrastructure/orm';
import { WagerService } from '../../src/application/wager-service';

const orm = await createOrm();
await orm.getMigrator().up();
const service = new WagerService(orm);
afterAll(async () => { await orm.close(true); });

test('falha na outbox reverte wallet, ledger, transação e inbox', async () => {
  const marker = crypto.randomUUID();
  const wallet = await service.createWallet(marker, { amount: '10.00', currency: 'BRL' });
  const db = orm.em.getConnection();
  await db.execute(`CREATE OR REPLACE FUNCTION audit_fail_outbox() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.aggregate_id = '${wallet.id}' THEN RAISE EXCEPTION 'injected outbox failure'; END IF; RETURN NEW; END $$`);
  await db.execute('CREATE TRIGGER audit_fail_outbox BEFORE INSERT ON outbox_messages FOR EACH ROW EXECUTE FUNCTION audit_fail_outbox()');
  try {
    await expect(service.submit({ providerId: 'p', externalTransactionId: marker, playerId: marker, walletId: wallet.id, roundId: 'r', gameId: 'g', kind: 'BET', money: { amount: '1.00', currency: 'BRL' } }, marker, { consumerName: 'audit', messageId: marker })).rejects.toThrow('injected outbox failure');
    const rows = await db.execute(`SELECT (SELECT balance::text FROM wallets WHERE id=?) AS balance,
      (SELECT count(*)::int FROM wager_transactions WHERE external_transaction_id=?) AS txs,
      (SELECT count(*)::int FROM inbox_messages WHERE consumer_name='audit' AND message_id=?) AS inbox,
      (SELECT count(*)::int FROM wallet_ledger_entries WHERE wallet_id=? AND direction='DEBIT') AS debits`, [wallet.id, marker, marker, wallet.id]);
    expect(rows[0]).toEqual({ balance: '10.00', txs: 0, inbox: 0, debits: 0 });
  } finally {
    await db.execute('DROP TRIGGER audit_fail_outbox ON outbox_messages');
    await db.execute('DROP FUNCTION audit_fail_outbox()');
  }
});

test('duas BET de 80 sobre saldo 100 produzem exatamente um débito', async () => {
  const marker = crypto.randomUUID();
  const wallet = await service.createWallet(marker, { amount: '100.00', currency: 'BRL' });
  const base = { providerId: 'provider', playerId: marker, walletId: wallet.id, roundId: 'round', gameId: 'game', kind: 'BET' as const, money: { amount: '80.00', currency: 'BRL' } };
  const [a, b] = await Promise.all([
    service.submit({ ...base, externalTransactionId: `${marker}-a` }, `${marker}-a`),
    service.submit({ ...base, externalTransactionId: `${marker}-b` }, `${marker}-b`),
  ]);
  expect([a.status,b.status].sort()).toEqual(['PROCESSED','REJECTED']);
  const db = orm.em.getConnection();
  const rows = await db.execute('SELECT balance FROM wallets WHERE id=?', [wallet.id]);
  expect(rows[0].balance).toBe('20.00');
  const ledger = await db.execute("SELECT count(*)::int AS count FROM wallet_ledger_entries WHERE wallet_id=? AND direction='DEBIT'", [wallet.id]);
  expect(ledger[0].count).toBe(1);
  const reconciliation = await db.execute(`SELECT w.balance::text AS stored,
    COALESCE(SUM(CASE WHEN l.direction='CREDIT' THEN l.amount ELSE -l.amount END),0)::text AS calculated
    FROM wallets w LEFT JOIN wallet_ledger_entries l ON l.wallet_id=w.id WHERE w.id=? GROUP BY w.id`, [wallet.id]);
  expect(reconciliation[0].stored).toBe(reconciliation[0].calculated);
});

test('cinquenta envios simultâneos da mesma aposta debitam uma vez e preservam resposta', async () => {
  const marker = crypto.randomUUID();
  const wallet = await service.createWallet(marker, { amount: '100.00', currency: 'BRL' });
  const command = { providerId: 'provider', externalTransactionId: marker, playerId: marker, walletId: wallet.id, roundId: 'r', gameId: 'g', kind: 'BET' as const, money: { amount: '1.00', currency: 'BRL' } };
  const responses = await Promise.all(Array.from({ length: 50 }, () => service.submit(command, marker)));
  expect(new Set(responses.map(x => x.transactionId)).size).toBe(1);
  expect(responses.filter(x => !x.idempotentReplay).length).toBe(1);
  expect(responses.every(x => x.balance.amount === '99.00')).toBe(true);
  const db = orm.em.getConnection();
  const ledger = await db.execute('SELECT count(*)::int AS count FROM wallet_ledger_entries WHERE wallet_id=? AND direction=?', [wallet.id, 'DEBIT']);
  expect(ledger[0].count).toBe(1);
  const reconciliation = await db.execute(`SELECT w.balance::text AS stored,
    COALESCE(SUM(CASE WHEN l.direction='CREDIT' THEN l.amount ELSE -l.amount END),0)::text AS calculated
    FROM wallets w LEFT JOIN wallet_ledger_entries l ON l.wallet_id=w.id WHERE w.id=? GROUP BY w.id`, [wallet.id]);
  expect(reconciliation[0].stored).toBe(reconciliation[0].calculated);
});

test('chave idempotente concorrente em wallets diferentes retorna conflito de domínio', async () => {
  for (let attempt = 0; attempt < 5; attempt++) {
    const marker = crypto.randomUUID();
    const wallets = await Promise.all([0, 1].map(index => service.createWallet(`${marker}-${index}`, { amount: '100.00', currency: 'BRL' })));
    const commands = wallets.map((wallet, index) => ({ providerId: 'provider', externalTransactionId: `${marker}-${index}`, playerId: wallet.playerId,
      walletId: wallet.id, roundId: 'r', gameId: 'g', kind: 'BET' as const, money: { amount: '1.00', currency: 'BRL' } }));
    const outcomes = await Promise.allSettled(commands.map(command => service.submit(command, marker)));
    expect(outcomes.filter(outcome => outcome.status === 'fulfilled')).toHaveLength(1);
    const rejected = outcomes.find(outcome => outcome.status === 'rejected');
    expect(rejected?.status === 'rejected' ? rejected.reason.message : '').toBe('IDEMPOTENCY_CONFLICT');
  }
});

test('reembolso integral da BET, replay e conflito de payload', async () => {
  const marker = crypto.randomUUID();
  const wallet = await service.createWallet(marker, { amount: '100.00', currency: 'BRL' });
  const base = { providerId: 'provider', playerId: marker, walletId: wallet.id, roundId: 'r', gameId: 'g' };
  const bet = await service.submit({ ...base, kind: 'BET', externalTransactionId: `${marker}-bet`, money: { amount: '25.00', currency: 'BRL' } }, `${marker}-bet`);
  expect(bet.balance.amount).toBe('75.00');
  const refundCommand = { ...base, kind: 'REFUND' as const, externalTransactionId: `${marker}-refund`, referenceExternalTransactionId: `${marker}-bet`, money: { amount: '25.00', currency: 'BRL' } };
  const refund = await service.submit(refundCommand, `${marker}-refund`);
  expect(refund.status).toBe('PROCESSED');
  expect(refund.balance.amount).toBe('100.00');
  const replay = await service.submit(refundCommand, `${marker}-refund`);
  expect(replay.transactionId).toBe(refund.transactionId);
  expect(replay.idempotentReplay).toBe(true);
  await expect(service.submit(refundCommand, `${marker}-different-key`)).rejects.toThrow('IDEMPOTENCY_CONFLICT');
  await service.submit({ ...base, kind: 'BET', externalTransactionId: `${marker}-later`, money: { amount: '1.00', currency: 'BRL' } }, `${marker}-later`);
  expect((await service.submit(refundCommand, `${marker}-refund`)).balance.amount).toBe('100.00');
  await expect(service.submit({ ...refundCommand, money: { amount: '20.00', currency: 'BRL' } }, `${marker}-refund`)).rejects.toThrow('IDEMPOTENCY_CONFLICT');
  const rollback = await service.submit({ ...base, kind: 'ROLLBACK', externalTransactionId: `${marker}-rollback`, referenceExternalTransactionId: `${marker}-bet`, money: { amount: '25.00', currency: 'BRL' } }, `${marker}-rollback`);
  expect(rollback.status).toBe('REJECTED');
  expect(rollback.failureCode).toBe('ALREADY_REVERSED');
});

test('inbox e financeiro confirmam juntos e redelivery não duplica', async () => {
  const marker = crypto.randomUUID();
  const wallet = await service.createWallet(marker, { amount: '10.00', currency: 'BRL' });
  const command = { providerId: 'provider', externalTransactionId: marker, playerId: marker, walletId: wallet.id, roundId: 'r', gameId: 'g', kind: 'BET' as const, money: { amount: '1.00', currency: 'BRL' } };
  const inbox = { consumerName: 'wager', messageId: marker };
  const first = await service.submit(command, marker, inbox);
  const second = await service.submit(command, marker, inbox);
  expect(first.transactionId).toBe(second.transactionId);
  expect(second.idempotentReplay).toBe(true);
  const db = orm.em.getConnection();
  const rows = await db.execute('SELECT count(*)::int AS count FROM inbox_messages WHERE consumer_name=? AND message_id=? AND processed_at IS NOT NULL', ['wager',marker]);
  expect(rows[0].count).toBe(1);
  const ledger = await db.execute('SELECT count(*)::int AS count FROM wallet_ledger_entries WHERE wallet_id=? AND direction=?', [wallet.id,'DEBIT']);
  expect(ledger[0].count).toBe(1);
  const events = await db.execute<Array<{ event_type: string }>>(`SELECT event_type FROM outbox_messages WHERE payload->'data'->>'transactionId'=? ORDER BY event_type`, [first.transactionId]);
  expect(events.map(event => event.event_type)).toEqual(['WagerTransactionProcessed','WalletBalanceChanged']);
});

test('REFUND fora de ordem aguarda BET e reprocessa sem duplicação', async () => {
  const marker = crypto.randomUUID();
  const wallet = await service.createWallet(marker, { amount: '100.00', currency: 'BRL' });
  const base = { providerId: 'provider', playerId: marker, walletId: wallet.id, roundId: 'r', gameId: 'g', money: { amount: '25.00', currency: 'BRL' } };
  const refundCommand = { ...base, kind: 'REFUND' as const, externalTransactionId: `${marker}-refund`, referenceExternalTransactionId: `${marker}-bet` };
  const pending = await service.submit(refundCommand, `${marker}-refund`);
  expect(pending.status).toBe('PENDING_REFERENCE');
  await service.submit({ ...base, kind: 'BET', externalTransactionId: `${marker}-bet` }, `${marker}-bet`);
  expect(await service.resolvePending()).toBeGreaterThan(0);
  const db = orm.em.getConnection();
  const tx = await db.execute('SELECT status FROM wager_transactions WHERE id=?', [pending.transactionId]);
  expect(tx[0].status).toBe('PROCESSED');
  const balance = await db.execute('SELECT balance FROM wallets WHERE id=?', [wallet.id]);
  expect(balance[0].balance).toBe('100.00');
  const replay = await service.submit(refundCommand, `${marker}-refund`);
  expect(replay.status).toBe('PENDING_REFERENCE');
  expect(replay.balance.amount).toBe('100.00');
});

test('WIN com referência de outra rodada não credita', async () => {
  const marker = crypto.randomUUID();
  const wallet = await service.createWallet(marker, { amount: '100.00', currency: 'BRL' });
  await service.submit({ providerId: 'p', externalTransactionId: `${marker}-bet`, playerId: marker, walletId: wallet.id, roundId: 'r1', gameId: 'g', kind: 'BET', money: { amount: '10.00', currency: 'BRL' } }, `${marker}-bet`);
  const win = await service.submit({ providerId: 'p', externalTransactionId: `${marker}-win`, playerId: marker, walletId: wallet.id, roundId: 'r2', gameId: 'g', kind: 'WIN', money: { amount: '50.00', currency: 'BRL' }, referenceExternalTransactionId: `${marker}-bet` }, `${marker}-win`);
  expect(win.status).toBe('REJECTED');
  const rows = await orm.em.getConnection().execute('SELECT balance FROM wallets WHERE id=?', [wallet.id]);
  expect(rows[0].balance).toBe('90.00');
});
