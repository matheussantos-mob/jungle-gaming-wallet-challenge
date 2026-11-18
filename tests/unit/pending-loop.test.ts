import { expect, test } from 'bun:test';
import { PendingReferenceWorker } from '../../src/workers/pending-worker';
import { WagerService } from '../../src/application/wager-service';

test('worker de referência pendente aguarda e continua após falha temporária', async () => {
  const service = { resolvePending: async () => { throw new Error('DB_UNAVAILABLE'); } } as unknown as WagerService;
  const worker = new PendingReferenceWorker(service);
  const running = worker.run();
  await Bun.sleep(300);
  worker.stop();
  expect(await Promise.race([running.then(() => 'stopped'), Bun.sleep(3000).then(() => 'timeout')])).toBe('stopped');
}, 10_000);
