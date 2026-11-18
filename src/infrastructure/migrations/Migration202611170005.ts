import { Migration } from '@mikro-orm/migrations';

export class Migration202611170005 extends Migration {
  override async up(): Promise<void> {
    this.addSql('DROP INDEX wager_reversal_unique');
    this.addSql(`CREATE UNIQUE INDEX wager_reversal_unique ON wager_transactions(reference_transaction_id,kind)
      WHERE reference_transaction_id IS NOT NULL AND kind IN ('REFUND','ROLLBACK') AND status='PROCESSED'`);
  }
  override async down(): Promise<void> {
    this.addSql('DROP INDEX wager_reversal_unique');
    this.addSql(`CREATE UNIQUE INDEX wager_reversal_unique ON wager_transactions(reference_transaction_id)
      WHERE reference_transaction_id IS NOT NULL AND kind IN ('REFUND','ROLLBACK') AND status='PROCESSED'`);
  }
}
