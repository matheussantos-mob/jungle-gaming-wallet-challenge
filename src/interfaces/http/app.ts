import 'reflect-metadata';
import { ArgumentsHost, Body, Catch, Controller, Get, Headers, HttpException, HttpStatus, Inject, Injectable, Module, OnModuleDestroy, Param, Post, Query, UseGuards, type ExceptionFilter } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { randomUUID } from 'node:crypto';
import type { INestApplication } from '@nestjs/common';
import { WagerService, type SubmitCommand } from '../../application/wager-service';
import { createOrm } from '../../infrastructure/orm';
import type { MikroORM } from '@mikro-orm/core';
import type { PostgreSqlDriver } from '@mikro-orm/postgresql';
import { errorCode } from '../../application/retry';
import { Money } from '../../domain/money';
import { GetQueueAttributesCommand, GetQueueUrlCommand, ListQueuesCommand, SQSClient } from '@aws-sdk/client-sqs';
import { createSqsClient } from '../../workers/sqs-consumer';
import { metrics } from '../../infrastructure/metrics';
import { NoopProviderIdentityGuard } from './provider-identity';
import { correlationContext, structuredLog } from '../../infrastructure/logger';

type Db = MikroORM<PostgreSqlDriver>;

@Catch()
class HttpErrorFilter implements ExceptionFilter {
  catch(error: unknown, host: ArgumentsHost): void {
    const status = error instanceof HttpException ? error.getStatus() : 500;
    const payload = error instanceof HttpException ? error.getResponse() : { statusCode: 500, message: 'Internal server error' };
    const body: Record<string, unknown> = typeof payload === 'object' && payload !== null ? { ...payload } : { message: payload };
    if (status >= 400) {
      const code = typeof body.code === 'string' ? body.code : typeof body.failureCode === 'string' ? body.failureCode : status === 503 ? 'DEPENDENCY_UNAVAILABLE' : status === 500 ? 'INTERNAL_ERROR' : status === 404 ? 'NOT_FOUND' : status === 400 ? 'INVALID_PAYLOAD' : `HTTP_${status}`;
      const message = typeof body.message === 'string' ? body.message : code;
      const correlationId = correlationContext.getStore() ?? randomUUID();
      structuredLog('error', 'http_request_failed', { correlationId, code, status });
      Object.assign(body, { code, message, correlationId });
    }
    host.switchToHttp().getResponse<{ status(code: number): { json(value: unknown): void } }>().status(status).json(body);
  }
}

function databaseUnavailable(error: unknown): boolean {
  const code = errorCode(error);
  return code !== undefined && (code.startsWith('08') ||
    ['40P01', '40001', '57P01', '57P02', '57P03', '53300', 'ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT'].includes(code));
}

function errorResponse(error: unknown): never {
  if (databaseUnavailable(error)) {
    throw new HttpException({ code: 'DATABASE_UNAVAILABLE', message: 'DATABASE_UNAVAILABLE' }, 503);
  }
  const code = error instanceof Error ? error.message : 'INTERNAL_ERROR';
  const status = code === 'IDEMPOTENCY_CONFLICT' ? 409
    : code === 'INSUFFICIENT_BALANCE' || code === 'REVERSAL_INSUFFICIENT_BALANCE' ? 422
    : code === 'WALLET_NOT_FOUND' ? 404
    : code === 'INVALID_PAYLOAD' || code === 'INVALID_AMOUNT' || code === 'AMOUNT_OUT_OF_RANGE' || code === 'INVALID_CURRENCY' || code === 'CURRENCY_MISMATCH' || code === 'PLAYER_MISMATCH' || code === 'REFERENCE_REQUIRED' || code === 'REFERENCE_NOT_ALLOWED' || code === 'IDEMPOTENCY_KEY_REQUIRED' ? 400
    : code.includes('unique') || code.includes('duplicate key') ? 409 : 500;
  const responseCode = status === 500 ? 'INTERNAL_ERROR' : code;
  throw new HttpException({ code: responseCode, message: responseCode }, status);
}

@Controller()
class ApiController {
  constructor(@Inject('WAGER_SERVICE') private readonly service: WagerService, @Inject('ORM') private readonly orm: Db, @Inject('SQS') private readonly sqs: SQSClient) {}

  private async query<T extends object[]>(sql: string, params: unknown[] = []): Promise<T> {
    try { return await this.orm.em.getConnection().execute<T>(sql, params); }
    catch (error) {
      if (databaseUnavailable(error)) return errorResponse(error);
      throw error;
    }
  }

  @Post('wallets')
  async createWallet(@Body() body: { playerId?: string; initialBalance?: { amount: string; currency: string } }) {
    if (typeof body?.playerId !== 'string' || !body.initialBalance) throw new HttpException({ code: 'INVALID_PAYLOAD' }, 400);
    try { return await this.service.createWallet(body.playerId, body.initialBalance); }
    catch (error) { return errorResponse(error); }
  }

  @Get('wallets/:walletId')
  async wallet(@Param('walletId') id: string) {
    const rows = await this.query<{ id: string; player_id: string; balance: string; currency: string; version: number }[]>('SELECT id,player_id,balance,currency,version FROM wallets WHERE id=?', [id]);
    if (!rows[0]) throw new HttpException({ code: 'WALLET_NOT_FOUND' }, 404);
    return { id: rows[0].id, playerId: rows[0].player_id, balance: { amount: rows[0].balance, currency: rows[0].currency.trim() }, version: rows[0].version };
  }

  @Get('wallets/:walletId/ledger')
  async ledger(@Param('walletId') walletId: string, @Query('cursor') cursor?: string, @Query('limit') limitRaw?: string) {
    const limit = limitRaw ? Number(limitRaw) : 50;
    if (!Number.isInteger(limit) || limit < 1 || limit > 100) throw new HttpException({ code: 'INVALID_LIMIT' }, 400);
    let boundary: { createdAt: string; id: string } | undefined;
    try {
      if (cursor) {
        boundary = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8')) as { createdAt: string; id: string };
        if (typeof boundary?.createdAt !== 'string' || typeof boundary.id !== 'string' || !boundary.id || Number.isNaN(new Date(boundary.createdAt).getTime())) throw new Error('INVALID_CURSOR');
      }
    }
    catch { throw new HttpException({ code: 'INVALID_CURSOR' }, 400); }
    const rows = await this.query<Array<{ id: string; transaction_id: string; direction: string; amount: string; currency: string; balance_before: string; balance_after: string; created_at: string }>>(
      `SELECT id,transaction_id,direction,amount,currency,balance_before,balance_after,created_at::text AS created_at FROM wallet_ledger_entries
       WHERE wallet_id=? AND (?::timestamptz IS NULL OR (created_at,id) < (?::timestamptz,?))
       ORDER BY created_at DESC,id DESC LIMIT ?`, [walletId,boundary?.createdAt ?? null,boundary?.createdAt ?? null,boundary?.id ?? null,limit+1]);
    const items = rows.slice(0, limit);
    const last = items.at(-1);
    return { items, nextCursor: rows.length > limit && last ? Buffer.from(JSON.stringify({ createdAt: last.created_at, id: last.id })).toString('base64url') : null };
  }

  @Post('wagering/transactions')
  @UseGuards(NoopProviderIdentityGuard)
  async submit(@Body() body: SubmitCommand, @Headers('idempotency-key') key?: string) {
    if (!body || typeof body.providerId !== 'string' || typeof body.externalTransactionId !== 'string' || typeof body.walletId !== 'string' || typeof body.playerId !== 'string' || typeof body.roundId !== 'string' || typeof body.gameId !== 'string' || !['BET','WIN','LOSS','REFUND','ROLLBACK'].includes(body.kind) || !body.money) throw new HttpException({ code: 'INVALID_PAYLOAD' }, 400);
    try {
      const result = await this.service.submit(body, key ?? '');
      if (result.status === 'PENDING_REFERENCE') throw new HttpException(result, 202);
      if (result.status === 'REJECTED') throw new HttpException(result, 422);
      return result;
    } catch (error) {
      if (error instanceof HttpException) throw error;
      return errorResponse(error);
    }
  }

  @Get('wagering/transactions/:transactionId')
  async byId(@Param('transactionId') id: string) { return this.transaction('id=?', [id]); }

  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  async byExternal(@Param('providerId') providerId: string, @Param('externalTransactionId') externalId: string) { return this.transaction('provider_id=? AND external_transaction_id=?', [providerId,externalId]); }

  private async transaction(where: string, params: string[]) {
    const rows = await this.query<Array<{
      id: string; provider_id: string; external_transaction_id: string; wallet_id: string; player_id: string;
      round_id: string; game_id: string; kind: string; amount: string; currency: string;
      reference_external_transaction_id: string | null; status: string; failure_code: string | null;
      result_balance: string | null; created_at: Date | string; processed_at: Date | string | null;
    }>>(`SELECT id,provider_id,external_transaction_id,wallet_id,player_id,round_id,game_id,kind,amount,currency,
      reference_external_transaction_id,status,failure_code,result_balance,created_at,processed_at FROM wager_transactions WHERE ${where}`, params);
    const row = rows[0];
    if (!row) throw new HttpException({ code: 'TRANSACTION_NOT_FOUND' }, 404);
    return { id: row.id, providerId: row.provider_id, externalTransactionId: row.external_transaction_id,
      walletId: row.wallet_id, playerId: row.player_id, roundId: row.round_id, gameId: row.game_id,
      kind: row.kind, money: { amount: row.amount, currency: row.currency.trim() },
      referenceExternalTransactionId: row.reference_external_transaction_id, status: row.status,
      failureCode: row.failure_code, balance: row.result_balance ? { amount: row.result_balance, currency: row.currency.trim() } : null,
      createdAt: new Date(row.created_at).toISOString(), processedAt: row.processed_at ? new Date(row.processed_at).toISOString() : null };
  }

  @Post('wallets/:walletId/reconciliation')
  async reconcile(@Param('walletId') walletId: string) {
    const rows = await this.query<Array<{ stored: string; calculated: string; currency: string; count: number }>>(
      `SELECT w.balance::text AS stored, COALESCE(SUM(CASE WHEN l.direction='CREDIT' THEN l.amount ELSE -l.amount END),0.00)::numeric(20,2)::text AS calculated,
       w.currency, COUNT(l.id)::int AS count FROM wallets w LEFT JOIN wallet_ledger_entries l ON l.wallet_id=w.id WHERE w.id=? GROUP BY w.id`, [walletId]);
    if (!rows[0]) throw new HttpException({ code: 'WALLET_NOT_FOUND' }, 404);
    const row = rows[0];
    const stored = Money.from({ amount: row.stored, currency: row.currency.trim() });
    const calculated = Money.from({ amount: row.calculated, currency: row.currency.trim() });
    const difference = stored.subtract(calculated);
    if (!difference.isZero()) {
      metrics.reconciliationDivergences++;
      structuredLog('error', 'reconciliation_divergence', { walletId, correlationId: walletId });
    }
    return { walletId, storedBalance: stored.toJSON(), calculatedBalance: calculated.toJSON(), difference: difference.toJSON(), consistent: difference.isZero(), checkedEntries: row.count };
  }

  @Get('metrics')
  async getMetrics() {
    const pending = await this.query<Array<{ oldest: Date | null }>>(`SELECT MIN(occurred_at) AS oldest FROM outbox_messages WHERE published_at IS NULL`);
    metrics.outboxLagMs = pending[0]?.oldest ? Math.max(0, Date.now() - new Date(pending[0].oldest).getTime()) : 0;
    let dlqCollectionError = false;
    let dlq: number | null = null;
    try {
      const queue = await this.sqs.send(new GetQueueUrlCommand({ QueueName: 'wager-transactions-dlq.fifo' }));
      if (!queue.QueueUrl) throw new Error('DLQ_URL_MISSING');
      const attributes = await this.sqs.send(new GetQueueAttributesCommand({ QueueUrl: queue.QueueUrl, AttributeNames: ['ApproximateNumberOfMessages'] }));
      const count = attributes.Attributes?.ApproximateNumberOfMessages;
      if (count === undefined || !/^\d+$/.test(count)) throw new Error('DLQ_COUNT_MISSING');
      dlq = Number(count);
    } catch {
      dlqCollectionError = true;
      structuredLog('warn', 'dlq_collection_failed', {});
    }
    return { ...metrics.snapshot(), dlq, dlqCollectionError };
  }

  @Get('health/live')
  live() { return { status: 'ok' }; }

  @Get('health/ready')
  async ready() {
    try { await this.orm.em.getConnection().execute('SELECT 1'); await this.sqs.send(new ListQueuesCommand({ MaxResults: 1 })); return { status: 'ok' }; }
    catch { throw new HttpException({ status: 'unavailable' }, HttpStatus.SERVICE_UNAVAILABLE); }
  }
}

@Injectable()
class OrmLifecycle implements OnModuleDestroy {
  constructor(@Inject('ORM') private readonly orm: Db, @Inject('SQS') private readonly sqs: SQSClient) {}
  async onModuleDestroy() { this.sqs.destroy(); await this.orm.close(true); }
}

export async function createHttpApp(options?: { sqsEndpoint?: string }): Promise<INestApplication> {
  const orm = await createOrm();
  await orm.getMigrator().up();
  const sqs = createSqsClient(options?.sqsEndpoint);
  @Module({ controllers: [ApiController], providers: [
    { provide: 'ORM', useValue: orm },
    { provide: 'SQS', useValue: sqs },
    { provide: 'WAGER_SERVICE', useValue: new WagerService(orm) },
    NoopProviderIdentityGuard,
    OrmLifecycle,
  ] })
  class AppModule {}
  const app = await NestFactory.create(AppModule, { logger: false });
  app.use((req: { headers: Record<string, string | string[] | undefined> }, res: { setHeader(name: string, value: string): void }, next: () => void) => {
    const header = req.headers['x-correlation-id'];
    const correlationId = typeof header === 'string' && /^[A-Za-z0-9._:-]{1,128}$/.test(header) ? header : randomUUID();
    res.setHeader('x-correlation-id', correlationId);
    correlationContext.run(correlationId, next);
  });
  app.useGlobalFilters(new HttpErrorFilter());
  return app;
}
