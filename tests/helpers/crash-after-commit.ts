import { createOrm } from '../../src/infrastructure/orm';
import { WagerService } from '../../src/application/wager-service';
import { createSqsClient, WagerConsumer } from '../../src/workers/sqs-consumer';

const queueUrl = process.argv[2];
if (!queueUrl) throw new Error('QUEUE_URL_REQUIRED');
const orm = await createOrm();
const client = createSqsClient();
const consumer = new WagerConsumer(client, queueUrl, new WagerService(orm), async () => { process.exit(77); });
await consumer.processOnce();
process.exit(2);
