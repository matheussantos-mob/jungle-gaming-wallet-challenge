import { Migration } from '@mikro-orm/migrations';

export class Migration202611170004 extends Migration {
  override async up(): Promise<void> {
    this.addSql(`CREATE FUNCTION enforce_wallet_version() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF (NEW.balance = OLD.balance AND NEW.version <> OLD.version)
          OR (NEW.balance <> OLD.balance AND NEW.version <> OLD.version + 1)
        THEN RAISE EXCEPTION 'wallet version must track balance changes'; END IF;
        RETURN NEW;
      END $$`);
    this.addSql(`CREATE TRIGGER wallet_version_guard BEFORE UPDATE ON wallets
      FOR EACH ROW EXECUTE FUNCTION enforce_wallet_version()`);
    this.addSql(`CREATE FUNCTION enforce_terminal_transaction() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF OLD.status IN ('PROCESSED','REJECTED','FAILED') AND NEW IS DISTINCT FROM OLD
        THEN RAISE EXCEPTION 'terminal transaction is immutable'; END IF;
        RETURN NEW;
      END $$`);
    this.addSql(`CREATE TRIGGER transaction_terminal_guard BEFORE UPDATE ON wager_transactions
      FOR EACH ROW EXECUTE FUNCTION enforce_terminal_transaction()`);
  }
  override async down(): Promise<void> {
    this.addSql('DROP TRIGGER transaction_terminal_guard ON wager_transactions');
    this.addSql('DROP FUNCTION enforce_terminal_transaction()');
    this.addSql('DROP TRIGGER wallet_version_guard ON wallets');
    this.addSql('DROP FUNCTION enforce_wallet_version()');
  }
}
