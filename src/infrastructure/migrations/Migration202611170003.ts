import { Migration } from '@mikro-orm/migrations';

export class Migration202611170003 extends Migration {
  override async up(): Promise<void> {
    this.addSql(`CREATE FUNCTION validate_ledger_insert() RETURNS trigger LANGUAGE plpgsql AS $$
      DECLARE wallet_row wallets%ROWTYPE; transaction_row wager_transactions%ROWTYPE;
      BEGIN
        SELECT * INTO wallet_row FROM wallets WHERE id=NEW.wallet_id FOR SHARE;
        SELECT * INTO transaction_row FROM wager_transactions WHERE id=NEW.transaction_id;
        IF wallet_row.id IS NULL OR transaction_row.id IS NULL
          OR transaction_row.wallet_id <> NEW.wallet_id
          OR transaction_row.currency <> NEW.currency
          OR wallet_row.currency <> NEW.currency
          OR transaction_row.amount <> NEW.amount
          OR transaction_row.status <> 'PROCESSED'
          OR transaction_row.kind = 'LOSS'
          OR wallet_row.balance <> NEW.balance_after
        THEN RAISE EXCEPTION 'ledger does not match wallet and transaction'; END IF;
        RETURN NEW;
      END $$`);
    this.addSql(`CREATE TRIGGER ledger_insert_consistency BEFORE INSERT ON wallet_ledger_entries
      FOR EACH ROW EXECUTE FUNCTION validate_ledger_insert()`);
  }
  override async down(): Promise<void> {
    this.addSql('DROP TRIGGER ledger_insert_consistency ON wallet_ledger_entries');
    this.addSql('DROP FUNCTION validate_ledger_insert()');
  }
}
