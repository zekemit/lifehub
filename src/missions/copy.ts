/**
 * src/missions/copy.ts — the words on a Mission Card, built deterministically from a trigger's data_json.
 *
 * No model writes card copy. Every claim sentence returned here is attached to the facts that ground it (see
 * engine.ts), and every string passes Gate G7: no tax characterization ("deductible", "you can deduct", "enter on line").
 */

export class TaxLanguageError extends Error {
  override readonly name = 'TaxLanguageError';
}

/** Gate G7. Case-insensitive; throws so a card with such wording is never staged. */
export function assertNoTaxLanguage(...texts: readonly string[]): void {
  const banned = /\b(deductible|deductions?|you can deduct|enter on line|write[- ]off)\b/i;
  for (const t of texts) {
    if (banned.test(t)) throw new TaxLanguageError(`card text characterizes tax treatment: "${t.slice(0, 60)}"`);
  }
}

type Data = Record<string, unknown>;

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const DAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];

export function usd(cents: number): string {
  const dollars = cents / 100;
  return Number.isInteger(dollars)
    ? `$${dollars.toLocaleString('en-US')}`
    : `$${dollars.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

export function shortDate(iso: string): string {
  const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(iso);
  return m ? `${MONTHS[Number(m[2]) - 1] ?? '?'} ${Number(m[3])}` : iso;
}

export function weekdayOf(iso: string): string {
  return DAYS[new Date(`${iso.slice(0, 10)}T00:00:00Z`).getUTCDay()] ?? '';
}

const str = (v: unknown, fallback = ''): string => (typeof v === 'string' ? v : fallback);
const num = (v: unknown): number => (typeof v === 'number' ? v : 0);
const benefitName = (label: string): string => label.split(':')[0]?.trim() || label;

export interface CardCopy {
  title: string;
  why: string;
  /** Label of the R2 button. */
  r2_label: string;
  /** True when the card rests on data that cannot confirm it (credit periods without transaction history). */
  unconfirmed: boolean;
  /** Sentence per claim key. */
  claims: Record<string, string>;
}

export function buildCopy(triggerKey: string, d: Data): CardCopy {
  let copy: CardCopy;
  switch (triggerKey) {
    case 'T_EXPIRING_CREDIT': {
      const unconfirmed = str(d['status']) === 'unconfirmed';
      const name = benefitName(str(d['label']));
      const tail = unconfirmed
        ? "we can't see this card's spending yet"
        : num(d['used_cents']) === 0
          ? `${usd(0)} used this period`
          : `${usd(num(d['used_cents']))} used so far`;
      copy = {
        title: `Your ${usd(num(d['amount_cents']))} ${name} resets ${shortDate(str(d['period_end']))} — ${tail}`,
        why: `${usd(num(d['remaining_cents']))} is ${unconfirmed ? 'possibly ' : ''}still available on ${str(d['card_name'], 'your card')} until ${shortDate(str(d['period_end']))}.`,
        r2_label: 'Use this credit',
        unconfirmed,
        claims: { credit: `${str(d['card_name'], 'Your card')} includes a ${usd(num(d['amount_cents']))} ${name}` },
      };
      break;
    }
    case 'T_ANNUAL_FEE_ROI': {
      const has = d['has_history'] === true || d['has_history'] === 1;
      const card = str(d['card_name'], 'Your card');
      copy = {
        title: `${card} ${usd(num(d['fee_cents']))} fee posts ${shortDate(str(d['next_post']))} — credits used ${usd(num(d['credits_used_cents']))} of ${usd(num(d['fee_cents']))}`,
        why: has
          ? 'Decide before it posts: keep the card, ask for a retention offer, or downgrade.'
          : "We haven't seen this card's statements yet, so credits used may be higher. Decide before it posts: keep, ask for a retention offer, or downgrade.",
        r2_label: 'Stage retention-call script',
        unconfirmed: !has,
        claims: { fee: `${card} has a ${usd(num(d['fee_cents']))} annual fee that posts ${shortDate(str(d['next_post']))}` },
      };
      break;
    }
    case 'T_ANNIV_DINING': {
      const unconfirmed = str(d['credit_status']) === 'unconfirmed';
      const name = benefitName(str(d['credit_label']));
      copy = {
        title: `${str(d['occasion'])} is ${shortDate(str(d['occasion_date']))} — dinner can be covered by a ${usd(num(d['credit_remaining_cents']))} credit`,
        why: `You're free ${weekdayOf(str(d['window_date']))} evening ${shortDate(str(d['window_date']))}, your ${name} on ${str(d['card_name'], 'your card')} has ${usd(num(d['credit_remaining_cents']))} ${unconfirmed ? 'possibly ' : ''}left, and your dining budget has room.`,
        r2_label: 'Plan dinner',
        unconfirmed,
        claims: {
          occasion: `${str(d['occasion'])} is on ${shortDate(str(d['occasion_date']))}`,
          window: `You are free the evening of ${shortDate(str(d['window_date']))}`,
          credit: `${str(d['card_name'], 'Your card')} includes a ${name}`,
        },
      };
      break;
    }
    case 'T_SUBSCRIPTION_CREEP': {
      const sub = str(d['subscription']);
      if (str(d['kind']) === 'duplicate') {
        copy = {
          title: `You may be paying twice for ${sub}`,
          why: `${str(d['first'])} (${usd(num(d['first_cents']))}) and ${str(d['second'])} (${usd(num(d['second_cents']))}) both bill the same merchant.`,
          r2_label: 'Review both charges',
          unconfirmed: false,
          claims: { duplicate: `${str(d['first'])} and ${str(d['second'])} both charge ${sub}` },
        };
      } else {
        copy = {
          title: `${sub} went from ${usd(num(d['old_cents']))} to ${usd(num(d['new_cents']))}`,
          why: `The price changed on ${shortDate(str(d['since']))}. Decide whether it is still worth it.`,
          r2_label: 'Stage cancel script',
          unconfirmed: false,
          claims: { increase: `${sub} now charges ${usd(num(d['new_cents']))}, up from ${usd(num(d['old_cents']))}` },
        };
      }
      break;
    }
    case 'T_OCCASION_PLAN': {
      const kind = str(d['kind']);
      copy = {
        title: `${str(d['occasion'])} is ${shortDate(str(d['occasion_date']))} — plan something`,
        why: kind === 'trip' ? 'A trip is coming up. Line up dining and logistics before you go.' : 'Pick a dinner or a gift while there is still time.',
        r2_label: kind === 'trip' ? 'Plan the trip' : 'Plan it',
        unconfirmed: false,
        claims: { [kind === 'trip' ? 'trip' : 'occasion']: `${str(d['occasion'])} is on ${shortDate(str(d['occasion_date']))}` },
      };
      break;
    }
    case 'T_DROP_ALARM': {
      const venue = str(d['venue']);
      copy = {
        title: `${venue} is releasing tables for ${shortDate(str(d['reservation_date']))} at ${str(d['release_clock'])}`,
        why: `Party of ${num(d['party_size'])}. Tap to open the booking page with the date and party size filled in; you pick the table and finish the booking yourself.`,
        r2_label: 'Open the booking page',
        unconfirmed: false,
        claims: { alarm: `You asked to be alerted when ${venue} releases tables for ${shortDate(str(d['reservation_date']))}` },
      };
      break;
    }
    case 'T_PRICE_WATCH': {
      const trip = `${str(d['origin'])} to ${str(d['destination'])}`;
      const dates = str(d['return_date']) === '' ? shortDate(str(d['depart_date'])) : `${shortDate(str(d['depart_date']))} – ${shortDate(str(d['return_date']))}`;
      copy = {
        title: `${trip} fares dropped to ${usd(num(d['fare_cents']))} for ${dates}`,
        why: `That is under your ${usd(num(d['ceiling_cents']))} ceiling. Tap to open the booking page; you choose the flight and book it yourself.`,
        r2_label: 'Open the booking page',
        unconfirmed: false,
        claims: { watch: `You asked to watch ${trip} (${dates}) for fares under ${usd(num(d['ceiling_cents']))}` },
      };
      break;
    }
    default:
      throw new Error(`no card copy for trigger ${triggerKey}`);
  }
  assertNoTaxLanguage(copy.title, copy.why, copy.r2_label, ...Object.values(copy.claims));
  return copy;
}

export const UNLOCK_SCAN_COPY: CardCopy = {
  title: 'Forward 3 receipts to unlock the subscription scan',
  why: 'Forward a few recent receipts to your drop address, or drag in a bank or Monarch CSV.',
  r2_label: 'Show my drop address',
  unconfirmed: false,
  claims: {},
};
