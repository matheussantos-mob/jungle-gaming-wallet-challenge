import { afterAll, expect, test } from 'bun:test';
import { createOrm } from '../../src/infrastructure/orm';

const orm = await createOrm();
await orm.getMigrator().up();
afterAll(async () => { await orm.close(true); });

test('todas as wallets persistidas reconciliam com seu ledger', async () => {
  const rows = await orm.em.getConnection().execute<Array<{ id: string; stored: string; calculated: string }>>(`
    SELECT w.id,w.balance::text AS stored,
      COALESCE(SUM(CASE WHEN l.direction='CREDIT' THEN l.amount ELSE -l.amount END),0)::text AS calculated
    FROM wallets w LEFT JOIN wallet_ledger_entries l ON l.wallet_id=w.id
    GROUP BY w.id HAVING w.balance <> COALESCE(SUM(CASE WHEN l.direction='CREDIT' THEN l.amount ELSE -l.amount END),0)
  `);
  expect(rows).toEqual([]);
});
