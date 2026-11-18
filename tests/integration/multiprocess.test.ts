import { afterAll, expect, test } from 'bun:test';
import { existsSync, mkdtempSync, rmSync, writeFileSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createOrm } from '../../src/infrastructure/orm';
import { WagerService } from '../../src/application/wager-service';

const orm = await createOrm();
await orm.getMigrator().up();
afterAll(async () => { await orm.close(true); });

test('três processos reais serializam a hot wallet', async () => {
  const marker = crypto.randomUUID();
  const wallet = await new WagerService(orm).createWallet(marker, { amount: '100.00', currency: 'BRL' });
  const dir = mkdtempSync(join(tmpdir(), 'jungle-multiprocess-'));
  try {
    const start = join(dir, 'start');
    const children = Array.from({ length: 3 }, (_, index) => {
      const ready = join(dir, `ready-${index}`);
      const result = join(dir, `result-${index}`);
      const child = Bun.spawn([process.execPath, join(import.meta.dir, '..', 'helpers', 'submit-bet.ts'), marker, wallet.id, marker, String(index), ready, start, result], { stdout: 'pipe', stderr: 'pipe' });
      return { child, ready, result };
    });
    for (let attempt = 0; attempt < 300 && children.some(item => !existsSync(item.ready)); attempt++) await Bun.sleep(20);
    expect(children.every(item => existsSync(item.ready))).toBe(true);
    writeFileSync(start, 'go');
    const exits = await Promise.all(children.map(item => item.child.exited));
    expect(exits).toEqual([0,0,0]);
    const outcomes = children.map(item => JSON.parse(readFileSync(item.result, 'utf8')) as { pid: number; result: { status: string } });
    expect(new Set(outcomes.map(item => item.pid)).size).toBe(3);
    expect(outcomes.filter(item => item.result.status === 'PROCESSED').length).toBe(2);
    expect(outcomes.filter(item => item.result.status === 'REJECTED').length).toBe(1);
    const rows = await orm.em.getConnection().execute('SELECT balance FROM wallets WHERE id=?', [wallet.id]);
    expect(rows[0].balance).toBe('20.00');
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('wallets distintas avançam em três processos independentes', async () => {
  const marker = crypto.randomUUID();
  const service = new WagerService(orm);
  const wallets = await Promise.all(Array.from({ length: 3 }, async (_, index) => {
    const playerId = `${marker}-${index}`;
    return { playerId, wallet: await service.createWallet(playerId, { amount: '100.00', currency: 'BRL' }) };
  }));
  const dir = mkdtempSync(join(tmpdir(), 'jungle-independent-'));
  try {
    const start = join(dir, 'start');
    const children = wallets.map((item, index) => {
      const ready = join(dir, `ready-${index}`);
      const result = join(dir, `result-${index}`);
      const child = Bun.spawn([process.execPath, join(import.meta.dir, '..', 'helpers', 'submit-bet.ts'), item.playerId, item.wallet.id, marker, String(index), ready, start, result], { stdout: 'pipe', stderr: 'pipe' });
      return { child, ready, result };
    });
    for (let attempt = 0; attempt < 300 && children.some(item => !existsSync(item.ready)); attempt++) await Bun.sleep(20);
    expect(children.every(item => existsSync(item.ready))).toBe(true);
    writeFileSync(start, 'go');
    expect(await Promise.all(children.map(item => item.child.exited))).toEqual([0,0,0]);
    const outcomes = children.map(item => JSON.parse(readFileSync(item.result, 'utf8')) as { pid: number; result: { status: string } });
    expect(new Set(outcomes.map(item => item.pid)).size).toBe(3);
    expect(outcomes.every(item => item.result.status === 'PROCESSED')).toBe(true);
    const db = orm.em.getConnection();
    for (const item of wallets) {
      const rows = await db.execute('SELECT balance FROM wallets WHERE id=?', [item.wallet.id]);
      expect(rows[0].balance).toBe('60.00');
    }
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
