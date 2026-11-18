import { PostgreSqlDriver } from '@mikro-orm/postgresql';
import { MikroORM } from '@mikro-orm/core';
import { Migrator } from '@mikro-orm/migrations';
import { Migration202611170001 } from './migrations/Migration202611170001';
import { Migration202611170002 } from './migrations/Migration202611170002';
import { Migration202611170003 } from './migrations/Migration202611170003';
import { Migration202611170004 } from './migrations/Migration202611170004';
import { Migration202611170005 } from './migrations/Migration202611170005';

export async function createOrm(): Promise<MikroORM<PostgreSqlDriver>> {
  return MikroORM.init<PostgreSqlDriver>({
    driver: PostgreSqlDriver,
    clientUrl: process.env.DATABASE_URL ?? 'postgresql://jungle:jungle@localhost:5439/jungle',
    entities: [],
    discovery: { warnWhenNoEntities: false },
    extensions: [Migrator],
    migrations: { migrationsList: [Migration202611170001, Migration202611170002, Migration202611170003, Migration202611170004, Migration202611170005] },
  });
}
