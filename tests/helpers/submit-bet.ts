import { existsSync } from 'node:fs';
import { createOrm } from '../../src/infrastructure/orm';
import { WagerService } from '../../src/application/wager-service';

const [playerId, walletId, marker, index, readyPath, startPath, resultPath] = process.argv.slice(2);
if (!playerId || !walletId || !marker || !index || !readyPath || !startPath || !resultPath) throw new Error('MISSING_ARGS');
await Bun.write(readyPath, String(process.pid));
while (!existsSync(startPath)) await Bun.sleep(10);
const orm = await createOrm();
try {
  const result = await new WagerService(orm).submit({ providerId: 'provider', externalTransactionId: `${marker}-${index}`, playerId, walletId, roundId: 'r', gameId: 'g', kind: 'BET', money: { amount: '40.00', currency: 'BRL' } }, `${marker}-${index}`);
  await Bun.write(resultPath, JSON.stringify({ pid: process.pid, result }));
} finally { await orm.close(true); }
