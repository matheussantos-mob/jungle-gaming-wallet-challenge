import { afterAll, expect, test } from 'bun:test';
import { createOrm } from '../../src/infrastructure/orm';
import { WagerService } from '../../src/application/wager-service';

const orm = await createOrm();
await orm.getMigrator().up();
afterAll(async () => { await orm.close(true); });

test('constraints impedem saldo negativo, duplicata, mutação e lançamento incompatível', async () => {
  const service = new WagerService(orm);
  const marker = crypto.randomUUID();
  const wallet = await service.createWallet(marker, { amount: '10.00', currency: 'BRL' });
  const bet = await service.submit({ providerId: 'p', externalTransactionId: marker, playerId: marker, walletId: wallet.id, roundId: 'r', gameId: 'g', kind: 'BET', money: { amount: '1.00', currency: 'BRL' } }, marker);
  const db = orm.em.getConnection();
  await expect(db.execute('INSERT INTO wallets(id,player_id,currency,balance,version) VALUES (?,?,?,?,1)', [crypto.randomUUID(),marker,'BRL','0.00'])).rejects.toThrow();
  await expect(db.execute('UPDATE wallets SET balance=-1 WHERE id=?', [wallet.id])).rejects.toThrow();
  await expect(db.execute('UPDATE wallets SET balance=8 WHERE id=?', [wallet.id])).rejects.toThrow();
  await expect(db.execute("UPDATE wager_transactions SET status='PENDING' WHERE id=?", [bet.transactionId])).rejects.toThrow();
  await expect(db.execute('UPDATE wallet_ledger_entries SET amount=2 WHERE transaction_id=?', [bet.transactionId])).rejects.toThrow();
  await expect(db.execute('DELETE FROM wallet_ledger_entries WHERE transaction_id=?', [bet.transactionId])).rejects.toThrow();
  await expect(db.execute(`INSERT INTO wallet_ledger_entries(id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after)
    VALUES (?,?,?,'DEBIT',1,'BRL',9,8)`, [crypto.randomUUID(),wallet.id,bet.transactionId])).rejects.toThrow();
  await expect(orm.em.fork().transactional(async em => {
    const txId = crypto.randomUUID();
    const ctx = em.getTransactionContext();
    await em.getConnection().execute(`INSERT INTO wager_transactions(id,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,kind,amount,currency,status)
      VALUES (?,?,?,?,?,?,?,?,?,'BET',1,'BRL','PROCESSED')`, [txId,'p',txId,txId,'hash',wallet.id,marker,'r','g'], 'run', ctx);
    await em.getConnection().execute(`INSERT INTO wallet_ledger_entries(id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after)
      VALUES (?,?,?,'DEBIT',1,'BRL',9,8)`, [crypto.randomUUID(),wallet.id,txId], 'run', ctx);
  })).rejects.toThrow();
});
