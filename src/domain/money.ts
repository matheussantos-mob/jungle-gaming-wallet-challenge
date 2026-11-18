export interface MoneyProps {
  amount: string;
  currency: string;
}

const ISO_CURRENCIES = new Set(Intl.supportedValuesOf('currency'));

/** Exact, immutable amount in hundredths of a currency unit. */
export class Money {
  private constructor(private readonly minorUnits: bigint, public readonly currency: string) {
    if (minorUnits > 99999999999999999999n || minorUnits < -99999999999999999999n) throw new Error('AMOUNT_OUT_OF_RANGE');
  }

  static from(props: MoneyProps): Money {
    if (!props || typeof props.amount !== 'string' || typeof props.currency !== 'string') throw new Error('INVALID_AMOUNT');
    if (!/^[A-Z]{3}$/.test(props.currency) || !ISO_CURRENCIES.has(props.currency)) throw new Error('INVALID_CURRENCY');
    if (!/^-?(?:0|[1-9]\d{0,17})\.\d{2}$/.test(props.amount)) throw new Error('INVALID_AMOUNT');
    const negative = props.amount.startsWith('-');
    const digits = negative ? props.amount.slice(1) : props.amount;
    const [whole, fraction] = digits.split('.') as [string, string];
    const units = BigInt(whole) * 100n + BigInt(fraction);
    return new Money(negative ? -units : units, props.currency);
  }

  static zero(currency: string): Money { return Money.from({ amount: '0.00', currency }); }

  add(other: Money): Money { this.assertSameCurrency(other); return new Money(this.minorUnits + other.minorUnits, this.currency); }
  subtract(other: Money): Money { this.assertSameCurrency(other); return new Money(this.minorUnits - other.minorUnits, this.currency); }
  negate(): Money { return new Money(-this.minorUnits, this.currency); }
  isZero(): boolean { return this.minorUnits === 0n; }
  isPositive(): boolean { return this.minorUnits > 0n; }
  isNegative(): boolean { return this.minorUnits < 0n; }
  isLessThan(other: Money): boolean { this.assertSameCurrency(other); return this.minorUnits < other.minorUnits; }
  equals(other: Money): boolean { return this.currency === other.currency && this.minorUnits === other.minorUnits; }
  toJSON(): MoneyProps { return { amount: this.toString(), currency: this.currency }; }
  toString(): string {
    const abs = this.minorUnits < 0n ? -this.minorUnits : this.minorUnits;
    return `${this.minorUnits < 0n ? '-' : ''}${abs / 100n}.${(abs % 100n).toString().padStart(2, '0')}`;
  }
  private assertSameCurrency(other: Money): void { if (this.currency !== other.currency) throw new Error('CURRENCY_MISMATCH'); }
}
