import { expect, test } from 'bun:test';
import { WagerService } from '../../src/application/wager-service';
import { createSqsClient, WagerConsumer } from '../../src/workers/sqs-consumer';

test('consumer continua vivo durante indisponibilidade transitória do SQS e encerra graciosamente', async () => {
  const client = createSqsClient('http://127.0.0.1:1');
  const consumer = new WagerConsumer(client, 'http://127.0.0.1:1/queue/test', {} as WagerService);
  try {
    const running = consumer.run();
    await Bun.sleep(600);
    consumer.stop();
    const result = await Promise.race([running.then(() => 'stopped'), Bun.sleep(3000).then(() => 'timeout')]);
    expect(result).toBe('stopped');
  } finally { client.destroy(); }
}, 15_000);
