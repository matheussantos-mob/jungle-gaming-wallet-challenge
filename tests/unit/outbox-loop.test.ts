import { expect, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/core';
import type { PostgreSqlDriver } from '@mikro-orm/postgresql';
import { createSqsClient } from '../../src/workers/sqs-consumer';
import { OutboxPublisher } from '../../src/workers/outbox-publisher';

test('publisher aguarda e continua após indisponibilidade transitória do banco', async () => {
  const broken = { em: { fork: () => ({ transactional: async () => { throw new Error('DB_UNAVAILABLE'); } }) } } as unknown as MikroORM<PostgreSqlDriver>;
  const client = createSqsClient();
  const publisher = new OutboxPublisher(broken, client, 'http://example.invalid/queue');
  try {
    const running = publisher.run();
    await Bun.sleep(300);
    publisher.stop();
    const result = await Promise.race([running.then(() => 'stopped'), Bun.sleep(3000).then(() => 'timeout')]);
    expect(result).toBe('stopped');
  } finally { client.destroy(); }
}, 10_000);
