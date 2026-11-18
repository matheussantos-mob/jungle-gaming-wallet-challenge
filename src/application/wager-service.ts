import type { MikroORM } from '@mikro-orm/core';
import type { PostgreSqlDriver } from '@mikro-orm/postgresql';
import { randomUUID } from 'node:crypto';
import { Money, type MoneyProps } from '../domain/money';
import { WagerTransaction, type WagerKind, type WagerStatus } from '../domain/wager';
import { Wallet } from '../domain/wallet';
import { WagerTransactionProcessed, WagerTransactionRejected, WagerTransactionPendingReference, WalletBalanceChanged } from '../domain/events';
import { metrics } from '../infrastructure/metrics';
import { errorCode, retryTransient } from './retry';
import { canonicalHash } from './canonical-hash';
import { structuredLog } from '../infrastructure/logger';

export interface SubmitCommand {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: Exclude<WagerKind, 'OPENING'>;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
}

export interface SubmitResult { transactionId: string; status: WagerStatus; balance: MoneyProps; idempotentReplay: boolean; failureCode?: string }
export interface InboxContext { consumerName: string; messageId: string }

function nonBlank(value: unknown, maxLength: number): value is string {
  return typeof value === 'string' && value.length > 0 && value.length <= maxLength && value.trim() === value;
}

export class WagerService {
  constructor(private readonly orm: MikroORM<PostgreSqlDriver>) {}

  async createWallet(playerId: string, initial: MoneyProps): Promise<{ id: string; playerId: string; balance: MoneyProps; version: number }> {
    if (!nonBlank(playerId, 256)) throw new Error('INVALID_PAYLOAD');
    const money = Money.from(initial);
    if (money.isNegative()) throw new Error('INVALID_AMOUNT');
    const walletId = randomUUID();
    const wallet = Wallet.open({ id: walletId, playerId, initialBalance: money });
    await this.orm.em.fork().transactional(async em => {
      const db = em.getConnection();
      const ctx = em.getTransactionContext();
      await db.execute('INSERT INTO wallets(id,player_id,currency,balance,version) VALUES (?,?,?,?,1)', [walletId, playerId, money.currency, money.toString()], 'run', ctx);
      if (money.isPositive()) {
        const txId = randomUUID();
        await db.execute(`INSERT INTO wager_transactions(id,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,kind,amount,currency,status,result_balance,processed_at)
          VALUES (?,?,?,?,?,?,?,?,?, 'OPENING',?,?,'PROCESSED',?,now())`, [txId, 'internal', txId, txId, 'opening', walletId, playerId, 'opening', 'opening', money.toString(), money.currency, money.toString()], 'run', ctx);
        await db.execute(`INSERT INTO wallet_ledger_entries(id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after) VALUES (?,?,?,'CREDIT',?,?,0,?)`, [randomUUID(), walletId, txId, money.toString(), money.currency, money.toString()], 'run', ctx);
        const events = [
          WagerTransactionProcessed.from({ walletId, transactionId: txId, status: 'PROCESSED' }, { correlationId: txId }),
          WalletBalanceChanged.from({ walletId, transactionId: txId, direction: 'CREDIT', money, balanceBefore: Money.zero(money.currency), balanceAfter: money, walletVersion: 1 }, { correlationId: txId }),
        ];
        for (const event of events) await db.execute('INSERT INTO outbox_messages(id,aggregate_id,event_type,payload,occurred_at) VALUES (?,?,?,?,?)',
          [event.eventId, event.aggregateId, event.eventType, JSON.stringify(event.toJSON()), event.occurredAt], 'run', ctx);
      }
    });
    return { id: walletId, playerId, balance: wallet.balance.toJSON(), version: wallet.version };
  }

  async submit(command: SubmitCommand, idempotencyKey: string, inbox?: InboxContext): Promise<SubmitResult> {
    const started = performance.now();
    if (!nonBlank(idempotencyKey, 256)) throw new Error('IDEMPOTENCY_KEY_REQUIRED');
    if (!command || !nonBlank(command.providerId, 128) || !nonBlank(command.externalTransactionId, 256)
      || !nonBlank(command.playerId, 256) || !nonBlank(command.walletId, 256) || !nonBlank(command.roundId, 256)
      || !nonBlank(command.gameId, 256) || !['BET','WIN','LOSS','REFUND','ROLLBACK'].includes(command.kind)
      || (command.referenceExternalTransactionId !== undefined && !nonBlank(command.referenceExternalTransactionId, 256))) throw new Error('INVALID_PAYLOAD');
    const money = Money.from(command.money);
    if (!money.isPositive()) throw new Error('INVALID_AMOUNT');
    if (command.kind === 'REFUND' || command.kind === 'ROLLBACK') {
      if (!command.referenceExternalTransactionId) throw new Error('REFERENCE_REQUIRED');
    }
    if ((command.kind === 'BET' || command.kind === 'LOSS') && command.referenceExternalTransactionId !== undefined) throw new Error('REFERENCE_NOT_ALLOWED');
    const payloadHash = canonicalHash({
      externalTransactionId: command.externalTransactionId, gameId: command.gameId, kind: command.kind,
      money: { amount: command.money.amount, currency: command.money.currency }, playerId: command.playerId, providerId: command.providerId,
      referenceExternalTransactionId: command.referenceExternalTransactionId ?? null,
      roundId: command.roundId, walletId: command.walletId,
    });
    let result: SubmitResult;
    try { result = await retryTransient(() => this.orm.em.fork().transactional(async em => {
      const db = em.getConnection();
      const ctx = em.getTransactionContext();
      if (inbox) {
        await db.execute(`INSERT INTO inbox_messages(consumer_name,message_id,payload_hash) VALUES (?,?,?) ON CONFLICT DO NOTHING`,
          [inbox.consumerName,inbox.messageId,payloadHash], 'run', ctx);
        const messages = await db.execute<{ payload_hash: string }[]>(
          'SELECT payload_hash FROM inbox_messages WHERE consumer_name=? AND message_id=?',
          [inbox.consumerName,inbox.messageId], 'all', ctx);
        if (messages[0]?.payload_hash !== payloadHash) throw new Error('INBOX_PAYLOAD_CONFLICT');
      }
      const existing = await db.execute<{ id: string; idempotency_key: string; payload_hash: string; response_snapshot: SubmitResult | null }[]>(
        'SELECT id,idempotency_key,payload_hash,response_snapshot FROM wager_transactions WHERE provider_id=? AND (idempotency_key=? OR external_transaction_id=?) LIMIT 1',
        [command.providerId, idempotencyKey, command.externalTransactionId], 'all', ctx);
      if (existing[0]) {
        if (existing[0].idempotency_key !== idempotencyKey || existing[0].payload_hash !== payloadHash) throw new Error('IDEMPOTENCY_CONFLICT');
        if (!existing[0].response_snapshot) throw new Error('RESULT_NOT_AVAILABLE');
        if (inbox) await db.execute('UPDATE inbox_messages SET processed_at=COALESCE(processed_at,now()) WHERE consumer_name=? AND message_id=?', [inbox.consumerName,inbox.messageId], 'run', ctx);
        return { ...existing[0].response_snapshot, idempotentReplay: true };
      }
      const rows = await db.execute<{ balance: string; currency: string; player_id: string; version: number; created_at: string | Date; updated_at: string | Date }[]>(
        'SELECT balance,currency,player_id,version,created_at,updated_at FROM wallets WHERE id=? FOR UPDATE', [command.walletId], 'all', ctx);
      const wallet = rows[0];
      if (!wallet) throw new Error('WALLET_NOT_FOUND');
      const serializedExisting = await db.execute<{ id: string; idempotency_key: string; payload_hash: string; response_snapshot: SubmitResult | null }[]>(
        'SELECT id,idempotency_key,payload_hash,response_snapshot FROM wager_transactions WHERE provider_id=? AND (idempotency_key=? OR external_transaction_id=?) LIMIT 1',
        [command.providerId, idempotencyKey, command.externalTransactionId], 'all', ctx);
      if (serializedExisting[0]) {
        if (serializedExisting[0].idempotency_key !== idempotencyKey || serializedExisting[0].payload_hash !== payloadHash) throw new Error('IDEMPOTENCY_CONFLICT');
        if (!serializedExisting[0].response_snapshot) throw new Error('RESULT_NOT_AVAILABLE');
        if (inbox) await db.execute('UPDATE inbox_messages SET processed_at=COALESCE(processed_at,now()) WHERE consumer_name=? AND message_id=?', [inbox.consumerName,inbox.messageId], 'run', ctx);
        return { ...serializedExisting[0].response_snapshot, idempotentReplay: true };
      }
      if (wallet.player_id !== command.playerId) throw new Error('PLAYER_MISMATCH');
      if (wallet.currency.trim() !== money.currency) throw new Error('CURRENCY_MISMATCH');
      const before = Money.from({ amount: wallet.balance, currency: money.currency });
      const aggregate = Wallet.rehydrate({ id: command.walletId, playerId: wallet.player_id, currency: wallet.currency.trim(), balance: before, version: wallet.version, createdAt: new Date(wallet.created_at), updatedAt: new Date(wallet.updated_at) });
      let after = before;
      let direction: 'DEBIT' | 'CREDIT' | undefined;
      let status: WagerStatus = 'PROCESSED';
      let failureCode: string | undefined;
      let referenceId: string | undefined;
      if (command.kind === 'BET') direction = 'DEBIT';
      else if (command.kind === 'WIN') {
        if (!command.referenceExternalTransactionId) direction = 'CREDIT';
        else {
          const refs = await db.execute<Array<{ id: string; kind: WagerKind; status: WagerStatus; wallet_id: string; player_id: string; round_id: string; currency: string }>>(
            'SELECT id,kind,status,wallet_id,player_id,round_id,currency FROM wager_transactions WHERE provider_id=? AND external_transaction_id=?',
            [command.providerId,command.referenceExternalTransactionId], 'all', ctx);
          const ref = refs[0];
          if (!ref || ref.status === 'PENDING_REFERENCE') status = 'PENDING_REFERENCE';
          else if (ref.kind !== 'BET' || ref.status !== 'PROCESSED' || ref.wallet_id !== command.walletId || ref.player_id !== command.playerId || ref.round_id !== command.roundId || ref.currency.trim() !== money.currency) {
            status = 'REJECTED'; failureCode = 'INVALID_REFERENCE';
          } else { referenceId = ref.id; direction = 'CREDIT'; }
        }
      }
      else if (command.kind === 'REFUND' || command.kind === 'ROLLBACK') {
        const refs = await db.execute<{ id: string; kind: WagerKind; amount: string; currency: string; wallet_id: string; player_id: string; round_id: string; status: WagerStatus }[]>(
          'SELECT id,kind,amount,currency,wallet_id,player_id,round_id,status FROM wager_transactions WHERE provider_id=? AND external_transaction_id=?',
          [command.providerId, command.referenceExternalTransactionId], 'all', ctx);
        const ref = refs[0];
        if (!ref) status = 'PENDING_REFERENCE';
        else if (ref.status === 'PENDING_REFERENCE') status = 'PENDING_REFERENCE';
        else if (ref.status !== 'PROCESSED' || ref.wallet_id !== command.walletId || ref.player_id !== command.playerId || ref.round_id !== command.roundId || ref.currency.trim() !== money.currency || ref.amount !== money.toString() || (command.kind === 'REFUND' && ref.kind !== 'BET') || (command.kind === 'ROLLBACK' && !['BET','WIN','REFUND'].includes(ref.kind))) {
          status = 'REJECTED'; failureCode = 'INVALID_REFERENCE';
        } else {
          const reversals = await db.execute<{ id: string }[]>(`SELECT r.id FROM wager_transactions r WHERE r.reference_transaction_id=? AND r.kind IN ('REFUND','ROLLBACK') AND r.status='PROCESSED' AND (r.kind=? OR NOT EXISTS (SELECT 1 FROM wager_transactions undo WHERE undo.reference_transaction_id=r.id AND undo.kind='ROLLBACK' AND undo.status='PROCESSED'))`, [ref.id, command.kind], 'all', ctx);
          if (reversals.length) { status = 'REJECTED'; failureCode = 'ALREADY_REVERSED'; }
          else { referenceId = ref.id; direction = ref.kind === 'WIN' || ref.kind === 'REFUND' ? 'DEBIT' : 'CREDIT'; }
        }
      }
      const txId = randomUUID();
      if (direction) {
        try { after = (direction === 'DEBIT' ? aggregate.debit(txId, money) : aggregate.credit(txId, money)).balanceAfter; }
        catch (error) {
          if (!(error instanceof Error) || error.message !== 'INSUFFICIENT_BALANCE') throw error;
          after = before;
          status = 'REJECTED';
          failureCode = command.kind === 'BET' ? 'INSUFFICIENT_BALANCE' : 'REVERSAL_INSUFFICIENT_BALANCE';
          direction = undefined;
        }
      }
      const transaction = WagerTransaction.create({ id: txId, providerId: command.providerId, externalTransactionId: command.externalTransactionId,
        idempotencyKey, payloadHash, walletId: command.walletId, playerId: command.playerId, roundId: command.roundId,
        gameId: command.gameId, kind: command.kind, money, referenceExternalTransactionId: command.referenceExternalTransactionId });
      if (status === 'PROCESSED') transaction.markProcessed(referenceId, new Date());
      else if (status === 'PENDING_REFERENCE') transaction.markPendingReference();
      else transaction.reject(failureCode ?? 'BUSINESS_RULE');
      status = transaction.status;
      failureCode = transaction.failureCode;
      const response: SubmitResult = { transactionId: txId, status, balance: after.toJSON(), idempotentReplay: false, ...(failureCode ? { failureCode } : {}) };
      await db.execute(`INSERT INTO wager_transactions(id,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,kind,amount,currency,reference_external_transaction_id,reference_transaction_id,status,failure_code,result_balance,response_snapshot,processed_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CASE WHEN ?='PENDING_REFERENCE' THEN NULL ELSE now() END)`,
        [txId, command.providerId, command.externalTransactionId, idempotencyKey, payloadHash, command.walletId, command.playerId, command.roundId, command.gameId, command.kind, money.toString(), money.currency, command.referenceExternalTransactionId ?? null, referenceId ?? null, status, failureCode ?? null, after.toString(), JSON.stringify(response), status], 'run', ctx);
      if (direction) {
        await db.execute('UPDATE wallets SET balance=?,version=version+1,updated_at=now() WHERE id=?', [after.toString(),command.walletId], 'run', ctx);
        await db.execute('INSERT INTO wallet_ledger_entries(id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after) VALUES (?,?,?,?,?,?,?,?)',
          [randomUUID(),command.walletId,txId,direction,money.toString(),money.currency,before.toString(),after.toString()], 'run', ctx);
      }
      const eventData = { walletId: command.walletId, transactionId: txId, status, ...(failureCode ? { failureCode } : {}) };
      const event = status === 'PROCESSED' ? WagerTransactionProcessed.from(eventData, { correlationId: txId })
        : status === 'REJECTED' ? WagerTransactionRejected.from(eventData, { correlationId: txId })
        : WagerTransactionPendingReference.from(eventData, { correlationId: txId });
      await db.execute('INSERT INTO outbox_messages(id,aggregate_id,event_type,payload,occurred_at) VALUES (?,?,?,?,?)',
        [event.eventId, event.aggregateId, event.eventType, JSON.stringify(event.toJSON()), event.occurredAt], 'run', ctx);
      if (direction) {
        const changed = WalletBalanceChanged.from({ walletId: command.walletId, transactionId: txId, direction, money, balanceBefore: before, balanceAfter: after, walletVersion: wallet.version + 1 }, { correlationId: txId });
        await db.execute('INSERT INTO outbox_messages(id,aggregate_id,event_type,payload,occurred_at) VALUES (?,?,?,?,?)',
          [changed.eventId, changed.aggregateId, changed.eventType, JSON.stringify(changed.toJSON()), changed.occurredAt], 'run', ctx);
      }
      if (inbox) await db.execute('UPDATE inbox_messages SET processed_at=now() WHERE consumer_name=? AND message_id=?', [inbox.consumerName,inbox.messageId], 'run', ctx);
      return response;
    })); }
    catch (error) {
      if (errorCode(error) !== '23505') throw error;
      const existing = await this.orm.em.getConnection().execute<Array<{ id: string; idempotency_key: string; payload_hash: string; response_snapshot: SubmitResult | null }>>(
        'SELECT id,idempotency_key,payload_hash,response_snapshot FROM wager_transactions WHERE provider_id=? AND (idempotency_key=? OR external_transaction_id=?) LIMIT 1',
        [command.providerId,idempotencyKey,command.externalTransactionId]);
      const row = existing[0];
      if (!row) throw error;
      if (row.idempotency_key !== idempotencyKey || row.payload_hash !== payloadHash) throw new Error('IDEMPOTENCY_CONFLICT');
      if (!row.response_snapshot) throw new Error('RESULT_NOT_AVAILABLE');
      result = { ...row.response_snapshot, idempotentReplay: true };
    }
    metrics.recordStatus(result.status, performance.now() - started, result.idempotentReplay);
    structuredLog('info', 'wager_transaction_result', { correlationId: result.transactionId, transactionId: result.transactionId, walletId: command.walletId,
      providerId: command.providerId, status: result.status, idempotentReplay: result.idempotentReplay });
    return result;
  }

  async resolvePending(limit = 20): Promise<number> {
    const due = await this.orm.em.getConnection().execute<Array<{ id: string; wallet_id: string }>>(
      `SELECT id,wallet_id FROM wager_transactions WHERE status='PENDING_REFERENCE'
       AND (next_attempt_at IS NULL OR next_attempt_at <= now()) ORDER BY created_at,id LIMIT ?`, [limit]);
    let handled = 0;
    for (const candidate of due) {
      const changed = await this.orm.em.fork().transactional(async em => {
        const db = em.getConnection();
        const ctx = em.getTransactionContext();
        const wallets = await db.execute<Array<{ balance: string; currency: string; version: number }>>(
          'SELECT balance,currency,version FROM wallets WHERE id=? FOR UPDATE', [candidate.wallet_id], 'all', ctx);
        const wallet = wallets[0];
        if (!wallet) return false;
        const txRows = await db.execute<Array<{ id: string; wallet_id: string; provider_id: string; player_id: string; round_id: string; kind: 'WIN' | 'REFUND' | 'ROLLBACK'; amount: string; currency: string; reference_external_transaction_id: string; attempts: number; status: WagerStatus }>>(
          'SELECT * FROM wager_transactions WHERE id=? FOR UPDATE', [candidate.id], 'all', ctx);
        const tx = txRows[0];
        if (!tx || tx.status !== 'PENDING_REFERENCE') return false;
        const refs = await db.execute<Array<{ id: string; kind: WagerKind; wallet_id: string; player_id: string; round_id: string; amount: string; currency: string; status: WagerStatus }>>(
          'SELECT id,kind,wallet_id,player_id,round_id,amount,currency,status FROM wager_transactions WHERE provider_id=? AND external_transaction_id=?',
          [tx.provider_id,tx.reference_external_transaction_id], 'all', ctx);
        const ref = refs[0];
        if (!ref || ref.status === 'PENDING_REFERENCE') {
          if (tx.attempts >= 4) {
            const snapshot: SubmitResult = { transactionId: tx.id, status: 'REJECTED', balance: Money.from({ amount: wallet.balance, currency: wallet.currency.trim() }).toJSON(), idempotentReplay: false, failureCode: 'REFERENCE_NOT_FOUND' };
            await db.execute(`UPDATE wager_transactions SET status='REJECTED',failure_code='REFERENCE_NOT_FOUND',result_balance=?,processed_at=now() WHERE id=?`, [wallet.balance,tx.id], 'run', ctx);
            const event = WagerTransactionRejected.from({ walletId: tx.wallet_id, transactionId: tx.id, status: 'REJECTED', failureCode: 'REFERENCE_NOT_FOUND' }, { correlationId: tx.id });
            await db.execute('INSERT INTO outbox_messages(id,aggregate_id,event_type,payload,occurred_at) VALUES (?,?,?,?,?)', [event.eventId,event.aggregateId,event.eventType,JSON.stringify(event.toJSON()),event.occurredAt], 'run', ctx);
            return 'REJECTED' as const;
          } else {
            const delay = Math.min(60, 2 ** (tx.attempts + 1));
            await db.execute(`UPDATE wager_transactions SET attempts=attempts+1,next_attempt_at=now()+(? * interval '1 second') WHERE id=?`, [delay,tx.id], 'run', ctx);
          }
          return true;
        }
        const invalid = ref.status !== 'PROCESSED' || ref.wallet_id !== tx.wallet_id || ref.player_id !== tx.player_id || ref.round_id !== tx.round_id || ref.currency.trim() !== tx.currency.trim() || (tx.kind !== 'WIN' && ref.amount !== tx.amount) || ((tx.kind === 'WIN' || tx.kind === 'REFUND') && ref.kind !== 'BET') || (tx.kind === 'ROLLBACK' && !['BET','WIN','REFUND'].includes(ref.kind));
        const prior = await db.execute<Array<{ id: string }>>(`SELECT r.id FROM wager_transactions r WHERE r.reference_transaction_id=? AND r.kind IN ('REFUND','ROLLBACK') AND r.status='PROCESSED' AND (r.kind=? OR NOT EXISTS (SELECT 1 FROM wager_transactions undo WHERE undo.reference_transaction_id=r.id AND undo.kind='ROLLBACK' AND undo.status='PROCESSED'))`, [ref.id, tx.kind], 'all', ctx);
        const before = Money.from({ amount: wallet.balance, currency: wallet.currency.trim() });
        const direction: 'DEBIT' | 'CREDIT' = tx.kind !== 'WIN' && (ref.kind === 'WIN' || ref.kind === 'REFUND') ? 'DEBIT' : 'CREDIT';
        let money: Money | undefined;
        let after = before;
        let entry: ReturnType<Wallet['credit']> | undefined;
        let failureCode: string | undefined = invalid ? 'INVALID_REFERENCE' : tx.kind !== 'WIN' && prior.length ? 'ALREADY_REVERSED' : undefined;
        if (!failureCode) {
          try {
            money = Money.from({ amount: tx.amount, currency: tx.currency.trim() });
            const aggregate = Wallet.rehydrate({ id: tx.wallet_id, playerId: tx.player_id, currency: wallet.currency.trim(), balance: before, version: wallet.version, createdAt: new Date(), updatedAt: new Date() });
            entry = direction === 'DEBIT' ? aggregate.debit(tx.id, money) : aggregate.credit(tx.id, money);
            after = entry.balanceAfter;
          } catch (error) {
            if (!(error instanceof Error) || !['AMOUNT_OUT_OF_RANGE', 'INVALID_AMOUNT', 'INVALID_CURRENCY', 'CURRENCY_MISMATCH', 'INSUFFICIENT_BALANCE', 'INVALID_LEDGER_ENTRY'].includes(error.message)) throw error;
            failureCode = error.message === 'INSUFFICIENT_BALANCE' ? 'REVERSAL_INSUFFICIENT_BALANCE' : error.message;
          }
        }
        if (failureCode) {
          const snapshot: SubmitResult = { transactionId: tx.id, status: 'REJECTED', balance: before.toJSON(), idempotentReplay: false, failureCode };
          await db.execute(`UPDATE wager_transactions SET status='REJECTED',failure_code=?,result_balance=?,processed_at=now() WHERE id=?`, [failureCode,before.toString(),tx.id], 'run', ctx);
          const event = WagerTransactionRejected.from({ walletId: tx.wallet_id, transactionId: tx.id, status: 'REJECTED', failureCode }, { correlationId: tx.id });
          await db.execute('INSERT INTO outbox_messages(id,aggregate_id,event_type,payload,occurred_at) VALUES (?,?,?,?,?)', [event.eventId,event.aggregateId,event.eventType,JSON.stringify(event.toJSON()),event.occurredAt], 'run', ctx);
          return 'REJECTED' as const;
        }
        if (!entry || !money) throw new Error('PENDING_RESOLUTION_INVARIANT');
        await db.execute('UPDATE wallets SET balance=?,version=version+1,updated_at=now() WHERE id=?', [after.toString(),tx.wallet_id], 'run', ctx);
        const snapshot: SubmitResult = { transactionId: tx.id, status: 'PROCESSED', balance: after.toJSON(), idempotentReplay: false };
        await db.execute(`UPDATE wager_transactions SET status='PROCESSED',reference_transaction_id=?,result_balance=?,processed_at=now(),next_attempt_at=NULL WHERE id=?`, [ref.id,after.toString(),tx.id], 'run', ctx);
        await db.execute('INSERT INTO wallet_ledger_entries(id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after) VALUES (?,?,?,?,?,?,?,?)',
          [randomUUID(),tx.wallet_id,tx.id,direction,money.toString(),money.currency,entry.balanceBefore.toString(),entry.balanceAfter.toString()], 'run', ctx);
        const events = [
          WagerTransactionProcessed.from({ walletId: tx.wallet_id, transactionId: tx.id, status: 'PROCESSED' }, { correlationId: tx.id }),
          WalletBalanceChanged.from({ walletId: tx.wallet_id, transactionId: tx.id, direction, money, balanceBefore: before, balanceAfter: after, walletVersion: wallet.version + 1 }, { correlationId: tx.id }),
        ];
        for (const event of events) await db.execute('INSERT INTO outbox_messages(id,aggregate_id,event_type,payload,occurred_at) VALUES (?,?,?,?,?)', [event.eventId,event.aggregateId,event.eventType,JSON.stringify(event.toJSON()),event.occurredAt], 'run', ctx);
        return 'PROCESSED' as const;
      });
      if (changed) handled++;
      if (changed === 'PROCESSED' || changed === 'REJECTED') metrics.transactionStatuses[changed]++;
    }
    return handled;
  }
}
