/**
 * src/travel/duffel.ts — flight SEARCH, nothing else (CLAUDE.md §1: "Duffel search only").
 *
 * One endpoint is called: POST /air/offer_requests?return_offers=true. The app never creates an order, never holds a payment method
 * with an airline, and never books: a found fare becomes a staged card and, once the member approves it, a deep link they finish
 * themselves. Calls go through the guarded fetch (src/net/egress.ts).
 */

export const DUFFEL_API = 'https://api.duffel.com';
export const DUFFEL_VERSION = 'v2';

export class DuffelError extends Error {
  override readonly name = 'DuffelError';
  constructor(readonly status: number, message: string) {
    super(message);
  }
}

export interface FlightQuery {
  /** IATA codes. */
  origin: string;
  destination: string;
  depart_date: string;
  return_date: string | null;
  adults: number;
}

export interface FlightOffer {
  offer_id: string;
  /** Total fare for all passengers, in US cents. */
  total_cents: number;
  carrier: string | null;
}

/** '540.00' -> 54000. Decimal string arithmetic only: no float rounding decides a price. Null for anything that is not a plain amount. */
export function amountToCents(amount: unknown): number | null {
  if (typeof amount !== 'string') return null;
  const m = /^(\d{1,9})(?:\.(\d{1,2}))?$/.exec(amount.trim());
  if (m === null) return null;
  return Number(m[1]) * 100 + Number((m[2] ?? '').padEnd(2, '0'));
}

export const IATA = /^[A-Z]{3}$/;

/** The cheapest USD offer for the query, or null when Duffel returned none. Throws DuffelError on an API error. */
export async function searchFlights(deps: { fetch: typeof fetch; apiKey: string }, q: FlightQuery): Promise<FlightOffer | null> {
  const slices = [{ origin: q.origin, destination: q.destination, departure_date: q.depart_date }];
  if (q.return_date !== null) slices.push({ origin: q.destination, destination: q.origin, departure_date: q.return_date });
  const res = await deps.fetch(`${DUFFEL_API}/air/offer_requests?return_offers=true`, {
    method: 'POST',
    headers: { authorization: `Bearer ${deps.apiKey}`, 'duffel-version': DUFFEL_VERSION, 'content-type': 'application/json', accept: 'application/json' },
    body: JSON.stringify({ data: { slices, passengers: Array.from({ length: q.adults }, () => ({ type: 'adult' })), cabin_class: 'economy' } }),
  });
  if (!res.ok) throw new DuffelError(res.status, `Duffel answered ${res.status}`);
  const body = (await res.json()) as { data?: { offers?: Array<{ id?: unknown; total_amount?: unknown; total_currency?: unknown; owner?: { name?: unknown } }> } };
  let best: FlightOffer | null = null;
  for (const offer of body.data?.offers ?? []) {
    if (offer.total_currency !== 'USD' || typeof offer.id !== 'string') continue;
    const cents = amountToCents(offer.total_amount);
    if (cents === null || cents <= 0) continue;
    if (best === null || cents < best.total_cents) best = { offer_id: offer.id, total_cents: cents, carrier: typeof offer.owner?.name === 'string' ? offer.owner.name.slice(0, 60) : null };
  }
  return best;
}
