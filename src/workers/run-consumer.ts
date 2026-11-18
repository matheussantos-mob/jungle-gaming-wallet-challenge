import { createOrm } from '../infrastructure/orm';
import { WagerService } from '../application/wager-service';
import { createSqsClient, ensureQueues, WagerConsumer } from './sqs-consumer';
import { startWorkerMetrics } from '../infrastructure/worker-metrics';

const orm = await createOrm();
const client = createSqsClient();
const queues = await ensureQueues(client);
const consumer = new WagerConsumer(client, queues.main, new WagerService(orm));
const metricsServer = startWorkerMetrics('consumer');
process.once('SIGTERM', () => consumer.stop());
process.once('SIGINT', () => consumer.stop());
try { await consumer.run(); }
finally { metricsServer.stop(true); client.destroy(); await orm.close(true); }
