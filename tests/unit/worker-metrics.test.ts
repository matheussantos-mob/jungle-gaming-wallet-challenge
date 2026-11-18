import { expect, test } from 'bun:test';
import { startWorkerMetrics } from '../../src/infrastructure/worker-metrics';
import { metrics } from '../../src/infrastructure/metrics';

test('each worker exposes its own metrics and identity', async () => {
  const server = startWorkerMetrics('consumer', 0);
  try {
    metrics.retries++;
    const response = await fetch(`http://127.0.0.1:${server.port}/metrics`);
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ process: 'consumer', retries: metrics.retries, dlq: null });
    expect((await fetch(`http://127.0.0.1:${server.port}/other`)).status).toBe(404);
  } finally { server.stop(true); }
});
