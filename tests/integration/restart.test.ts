import { expect, test } from 'bun:test';
import { CreateQueueCommand, ReceiveMessageCommand } from '@aws-sdk/client-sqs';
import { createHttpApp } from '../../src/interfaces/http/app';
import { createOrm } from '../../src/infrastructure/orm';
import { WagerService } from '../../src/application/wager-service';
import { createSqsClient } from '../../src/workers/sqs-consumer';
import { OutboxPublisher } from '../../src/workers/outbox-publisher';

test('reinício da API preserva wallet e consultas', async () => {
  const app1 = await createHttpApp();
  await app1.listen(0);
  const address1 = app1.getHttpServer().address();
  const playerId = crypto.randomUUID();
  const created = await fetch(`http://127.0.0.1:${address1.port}/wallets`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ playerId, initialBalance: { amount: '12.00', currency: 'BRL' } }) });
  const wallet = await created.json() as { id: string };
  await app1.close();
  const app2 = await createHttpApp();
  try {
    await app2.listen(0);
    const address2 = app2.getHttpServer().address();
    const response = await fetch(`http://127.0.0.1:${address2.port}/wallets/${wallet.id}`);
    expect(response.status).toBe(200);
    const body = await response.json() as { balance: { amount: string } };
    expect(body.balance.amount).toBe('12.00');
  } finally { await app2.close(); }
});

test('outbox pendente é publicada por novo processo lógico após reinício', async () => {
  const client = createSqsClient();
  const queue = await client.send(new CreateQueueCommand({ QueueName: `${crypto.randomUUID()}.fifo`, Attributes: { FifoQueue: 'true' } }));
  if (!queue.QueueUrl) throw new Error('QUEUE_MISSING');
  const marker = crypto.randomUUID();
  const orm1 = await createOrm();
  await orm1.getMigrator().up();
  let transactionId: string;
  try {
    const service = new WagerService(orm1);
    const wallet = await service.createWallet(marker, { amount: '10.00', currency: 'BRL' });
    transactionId = (await service.submit({ providerId: 'p', externalTransactionId: marker, playerId: marker, walletId: wallet.id, roundId: 'r', gameId: 'g', kind: 'BET', money: { amount: '1.00', currency: 'BRL' } }, marker)).transactionId;
  } finally { await orm1.close(true); }
  const orm2 = await createOrm();
  try {
    const publisher = new OutboxPublisher(orm2, client, queue.QueueUrl);
    let published: Array<{ published_at: Date | null }> = [];
    for (let attempt = 0; attempt < 500; attempt++) {
      await publisher.publishOnce();
      published = await orm2.em.getConnection().execute<Array<{ published_at: Date | null }>>(`SELECT published_at FROM outbox_messages WHERE payload->'data'->>'transactionId'=? AND event_type='WagerTransactionProcessed'`, [transactionId]);
      if (published[0]?.published_at) break;
    }
    expect(published[0]?.published_at).not.toBeNull();
    const message = await client.send(new ReceiveMessageCommand({ QueueUrl: queue.QueueUrl, WaitTimeSeconds: 1, MaxNumberOfMessages: 10 }));
    expect(message.Messages?.length).toBeGreaterThan(0);
  } finally { await orm2.close(true); client.destroy(); }
});
