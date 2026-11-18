import { createOrm } from '../infrastructure/orm';
import { WagerService } from '../application/wager-service';
import { PendingReferenceWorker } from './pending-worker';
import { startWorkerMetrics } from '../infrastructure/worker-metrics';

const orm = await createOrm();
const service = new WagerService(orm);
const worker = new PendingReferenceWorker(service);
const metricsServer = startWorkerMetrics('pending', Number(process.env.METRICS_PORT ?? 9466));
process.once('SIGTERM', () => worker.stop());
process.once('SIGINT', () => worker.stop());
try { await worker.run(); }
finally { metricsServer.stop(true); await orm.close(true); }
