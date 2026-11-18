import { createOrm } from '../src/infrastructure/orm';

const orm = await createOrm();
try {
  if (process.argv[2] === 'down-all') await orm.getMigrator().down({ to: 0 });
  else if (process.argv[2] === 'down') await orm.getMigrator().down();
  else await orm.getMigrator().up();
} finally {
  await orm.close(true);
}
