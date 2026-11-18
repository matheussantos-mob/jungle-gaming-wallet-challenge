import { WagerService } from '../application/wager-service';
import { metrics } from '../infrastructure/metrics';
import { structuredLog } from '../infrastructure/logger';

export class PendingReferenceWorker {
  private stopping = false;
  constructor(private readonly service: WagerService) {}
  stop(): void { this.stopping = true; }
  async run(): Promise<void> {
    let failures = 0;
    while (!this.stopping) {
      try {
        await this.service.resolvePending();
        failures = 0;
        if (!this.stopping) await Bun.sleep(1000);
      } catch (error) {
        failures++;
        metrics.retries++;
        structuredLog('error', 'pending_reference_unavailable', { errorType: error instanceof Error ? error.name : 'UNKNOWN', attempt: failures });
        if (!this.stopping) await Bun.sleep(Math.min(5000, 100 * 2 ** Math.min(failures, 5)));
      }
    }
  }
}
