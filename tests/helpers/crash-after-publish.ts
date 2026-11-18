import { SendMessageCommand } from '@aws-sdk/client-sqs';
import { createOrm } from '../../src/infrastructure/orm';
import { createSqsClient } from '../../src/workers/sqs-consumer';
import { OutboxPublisher } from '../../src/workers/outbox-publisher';

const [queue, eventId] = process.argv.slice(2);
if (!queue || !eventId) throw new Error('ARGUMENTS_REQUIRED');
const orm = await createOrm();
const client = createSqsClient();
const send = client.send.bind(client);
// Kill the process at the real send boundary, before the outbox transaction can commit.
const wrapped = { send: async (command: SendMessageCommand) => {
  const result = await send(command);
  if (JSON.parse(command.input.MessageBody ?? '{}').eventId === eventId) process.exit(77);
  return result;
} } as typeof client;
const publisher = new OutboxPublisher(orm, wrapped, queue);
for (let i = 0; i < 1000; i++) await publisher.publishOnce();
process.exit(2);
