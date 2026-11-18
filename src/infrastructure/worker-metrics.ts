import { metrics } from './metrics';

export function startWorkerMetrics(role: string, port = Number(process.env.METRICS_PORT ?? 9464)) {
  return Bun.serve({
    port,
    fetch(request) {
      if (new URL(request.url).pathname !== '/metrics') return new Response('Not found', { status: 404 });
      return Response.json({ process: role, ...metrics.snapshot(), dlq: null });
    },
  });
}
