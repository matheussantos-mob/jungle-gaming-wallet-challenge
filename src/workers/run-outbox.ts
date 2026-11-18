import { CreateQueueCommand } from '@aws-sdk/client-sqs';
import { createOrm } from '../infrastructure/orm';
import { createSqsClient } from './sqs-consumer';
import { OutboxPublisher } from './outbox-publisher';
import { startWorkerMetrics } from '../infrastructure/worker-metrics';

const orm = await createOrm();
const client = createSqsClient();
const queue = await client.send(new CreateQueueCommand({ QueueName: 'wager-events.fifo', Attributes: { FifoQueue: 'true' } }));
if (!queue.QueueUrl) throw new Error('EVENT_QUEUE_MISSING');
const publisher = new OutboxPublisher(orm, client, queue.QueueUrl);
const metricsServer = startWorkerMetrics('outbox', Number(process.env.METRICS_PORT ?? 9465));
process.once('SIGTERM', () => publisher.stop());
process.once('SIGINT', () => publisher.stop());
try { await publisher.run(); }
finally { metricsServer.stop(true); client.destroy(); await orm.close(true); }
