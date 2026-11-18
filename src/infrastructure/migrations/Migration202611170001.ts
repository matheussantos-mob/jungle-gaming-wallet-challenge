import { Migration } from '@mikro-orm/migrations';

export class Migration202611170001 extends Migration {
  override async up(): Promise<void> {
    this.addSql(`CREATE TABLE wallets (
      id text PRIMARY KEY, player_id text NOT NULL, currency char(3) NOT NULL,
      balance numeric(20,2) NOT NULL DEFAULT 0, version integer NOT NULL DEFAULT 1,
      created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
      CONSTRAINT wallets_player_currency_unique UNIQUE (player_id,currency),
      CONSTRAINT wallets_balance_nonnegative CHECK (balance >= 0),
      CONSTRAINT wallets_version_positive CHECK (version >= 1),
      CONSTRAINT wallets_currency_format CHECK (currency ~ '^[A-Z]{3}$')
    )`);
    this.addSql(`CREATE TABLE wager_transactions (
      id text PRIMARY KEY, provider_id text NOT NULL, external_transaction_id text NOT NULL,
      idempotency_key text NOT NULL, payload_hash text NOT NULL, wallet_id text NOT NULL REFERENCES wallets(id),
      player_id text NOT NULL, round_id text NOT NULL, game_id text NOT NULL,
      kind text NOT NULL CHECK (kind IN ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
      amount numeric(20,2) NOT NULL CHECK (amount > 0), currency char(3) NOT NULL,
      reference_external_transaction_id text, reference_transaction_id text REFERENCES wager_transactions(id),
      status text NOT NULL CHECK (status IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
      failure_code text, result_balance numeric(20,2), response_snapshot jsonb,
      attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0), next_attempt_at timestamptz,
      created_at timestamptz NOT NULL DEFAULT now(), processed_at timestamptz,
      CONSTRAINT wager_external_unique UNIQUE (provider_id,external_transaction_id),
      CONSTRAINT wager_idempotency_unique UNIQUE (provider_id,idempotency_key),
      CONSTRAINT wager_reference_required CHECK (kind NOT IN ('REFUND','ROLLBACK') OR reference_external_transaction_id IS NOT NULL)
    )`);
    this.addSql(`CREATE UNIQUE INDEX wager_reversal_unique ON wager_transactions(reference_transaction_id,kind)
      WHERE reference_transaction_id IS NOT NULL AND kind IN ('REFUND','ROLLBACK') AND status = 'PROCESSED'`);
    this.addSql(`CREATE INDEX wager_pending_due ON wager_transactions(next_attempt_at) WHERE status = 'PENDING_REFERENCE'`);
    this.addSql(`CREATE TABLE wallet_ledger_entries (
      id text PRIMARY KEY, wallet_id text NOT NULL REFERENCES wallets(id),
      transaction_id text NOT NULL REFERENCES wager_transactions(id),
      direction text NOT NULL CHECK (direction IN ('DEBIT','CREDIT')),
      amount numeric(20,2) NOT NULL CHECK (amount > 0), currency char(3) NOT NULL,
      balance_before numeric(20,2) NOT NULL CHECK (balance_before >= 0),
      balance_after numeric(20,2) NOT NULL CHECK (balance_after >= 0),
      created_at timestamptz NOT NULL DEFAULT now(),
      CONSTRAINT ledger_transaction_unique UNIQUE (transaction_id,wallet_id),
      CONSTRAINT ledger_arithmetic CHECK (balance_after = CASE WHEN direction='CREDIT' THEN balance_before+amount ELSE balance_before-amount END)
    )`);
    this.addSql(`CREATE INDEX ledger_wallet_cursor ON wallet_ledger_entries(wallet_id,created_at,id)`);
    this.addSql(`CREATE FUNCTION reject_ledger_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN RAISE EXCEPTION 'ledger is immutable'; END $$`);
    this.addSql(`CREATE TRIGGER ledger_immutable BEFORE UPDATE OR DELETE ON wallet_ledger_entries
      FOR EACH ROW EXECUTE FUNCTION reject_ledger_mutation()`);
    this.addSql(`CREATE TABLE inbox_messages (
      consumer_name text NOT NULL, message_id text NOT NULL, payload_hash text NOT NULL,
      received_at timestamptz NOT NULL DEFAULT now(), processed_at timestamptz,
      PRIMARY KEY (consumer_name,message_id)
    )`);
    this.addSql(`CREATE TABLE outbox_messages (
      id text PRIMARY KEY, aggregate_id text NOT NULL, event_type text NOT NULL,
      payload jsonb NOT NULL, occurred_at timestamptz NOT NULL,
      attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
      next_attempt_at timestamptz NOT NULL DEFAULT now(), published_at timestamptz
    )`);
    this.addSql(`CREATE INDEX outbox_due ON outbox_messages(next_attempt_at) WHERE published_at IS NULL`);
  }

  override async down(): Promise<void> {
    this.addSql('DROP TABLE outbox_messages');
    this.addSql('DROP TABLE inbox_messages');
    this.addSql('DROP TABLE wallet_ledger_entries');
    this.addSql('DROP FUNCTION reject_ledger_mutation()');
    this.addSql('DROP TABLE wager_transactions');
    this.addSql('DROP TABLE wallets');
  }
}
