import { createOrm } from '../../src/infrastructure/orm';
import { WagerService } from '../../src/application/wager-service';
import { createSqsClient, WagerConsumer } from '../../src/workers/sqs-consumer';

const queue = process.argv[2];
if (!queue) throw new Error('QUEUE_URL_REQUIRED');
const orm = await createOrm();
const client = createSqsClient();
const consumer = new WagerConsumer(client, queue, new WagerService(orm), async () => {
  console.log('COMMITTED');
  await Bun.sleep(1500);
});
process.once('SIGTERM', () => consumer.stop());
try { await consumer.run(); }
finally { client.destroy(); await orm.close(true); }
