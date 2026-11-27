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

/** 200 -> "3 h 20 min", 45 -> "45 min". */
export function duration(minutes: number): string {
  const m = Math.max(0, Math.round(minutes));
  const h = Math.floor(m / 60);
  return h === 0 ? `${m} min` : m % 60 === 0 ? `${h} h` : `${h} h ${m % 60} min`;
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
    case 'T_CLOSING_RETURN_OR_WARRANTY': {
      const product = str(d['product'], 'this item');
      if (str(d['kind']) === 'return') {
        copy = {
          title: `The return window for ${product} closes ${shortDate(str(d['closes_on']))} — ${usd(num(d['price_cents']))} is still returnable`,
          why: `Decide before ${shortDate(str(d['closes_on']))}: keep it, or return it for a refund.`,
          r2_label: 'Plan the return',
          unconfirmed: false,
          claims: { window: `The return window for ${product} closes on ${shortDate(str(d['closes_on']))}` },
        };
      } else {
        const serial = str(d['serial']);
        copy = {
          title: `The warranty on ${product} ends ${shortDate(str(d['expires_on']))}`,
          why: serial === '' ? 'If anything is wrong with it, now is the time. A claim packet can be drafted from your records.' : `If anything is wrong with it, now is the time. A claim packet can be drafted from your records, with serial number ${serial}.`,
          r2_label: 'Draft the claim packet',
          unconfirmed: false,
          claims: { warranty: `The warranty on ${product} ends on ${shortDate(str(d['expires_on']))}` },
        };
      }
      break;
    }
    case 'T_PRICE_DROP_OR_DELAY_CLAIM': {
      if (str(d['kind']) === 'delay') {
        const trip = str(d['trip'], 'your flight');
        const length = duration(num(d['delay_minutes']));
        copy = {
          title: `${trip} was delayed ${length} — you may be owed compensation`,
          why: 'Your booking and the airline\'s update are attached to a draft claim. Review it, then send it yourself.',
          r2_label: 'Review the claim',
          unconfirmed: false,
          claims: { booking: `You have a booking for ${trip}`, delay: `${trip} is delayed ${length}` },
        };
      } else {
        const product = str(d['product'], 'this item');
        copy = {
          title: `${product} dropped ${usd(num(d['drop_cents']))} — you may be owed a price adjustment`,
          why: `You paid ${usd(num(d['purchase_cents']))}, and the price fell inside your return window, which ends ${shortDate(str(d['window_ends']))}. A request is drafted from your receipt and the price notice.`,
          r2_label: 'Review the claim',
          unconfirmed: false,
          claims: { purchase: `You bought ${product} for ${usd(num(d['purchase_cents']))}`, drop: `The price of ${product} fell by ${usd(num(d['drop_cents']))}` },
        };
      }
      break;
    }
    case 'T_FAMILY_EVENT_AUTOPILOT': {
      const event = str(d['event'], 'An event');
      const items = Array.isArray(d['checklist']) ? (d['checklist'] as unknown[]).filter((x): x is string => typeof x === 'string' && x.trim() !== '') : [];
      const on = shortDate(str(d['registration_opens_on']));
      copy = {
        title: `${event}: registration opens ${on}`,
        why: items.length === 0 ? 'Put the date on your radar so the spots do not go without you.' : `Have these ready: ${items.join('; ')}.`,
        r2_label: 'Mark as handled',
        unconfirmed: false,
        claims: { plan: items.length === 0 ? `Registration for ${event} opens on ${on}` : `Registration for ${event} opens on ${on} and asks for: ${items.join('; ')}` },
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
