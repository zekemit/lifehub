/**
 * src/workers/concierge.ts — Engine B for the signed-in member: drop alarms and flight price watches.
 *
 *   POST /api/drop-alarms                    {venue_entity_id, reservation_date, party_size}: starts DropAlarmWF (instance id = alarm id)
 *   POST /api/drop-alarms/<alarm_id>/cancel
 *   POST /api/price-watches                  {origin, destination, depart_date, return_date?, ceiling_cents, adults?, booking_domain?}
 *                                            starts PriceWatchWF (instance id = watch id); at most two active per household
 *   POST /api/price-watches/<watch_id>/stop
 */

import { createPriceWatch } from '../concierge/flights';
import { createDropAlarm } from '../concierge/venues';
import type { Env } from '../types';
import type { Auth } from './auth';
import { error, json } from './http';

const ALARM_ID = /^alm_[0-9a-f]{32}$/;
const WATCH_ID = /^pwt_[0-9a-f]{32}$/;

export function parseConciergePath(path: string): { kind: 'alarm' | 'watch'; id: string; action: 'cancel' | 'stop' } | null {
  const a = /^\/api\/drop-alarms\/([^/]+)\/cancel$/.exec(path);
  if (a !== null && ALARM_ID.test(a[1]!)) return { kind: 'alarm', id: a[1]!, action: 'cancel' };
  const w = /^\/api\/price-watches\/([^/]+)\/stop$/.exec(path);
  if (w !== null && WATCH_ID.test(w[1]!)) return { kind: 'watch', id: w[1]!, action: 'stop' };
  return null;
}

async function readJson(request: Request): Promise<Record<string, unknown> | null> {
  try {
    const body = (await request.json()) as unknown;
    return body !== null && typeof body === 'object' && !Array.isArray(body) ? (body as Record<string, unknown>) : null;
  } catch {
    return null;
  }
}

const ALARM_STATUS: Record<string, number> = { bad_input: 400, too_far: 400, unknown_venue: 404, no_release_rule: 409, unsafe_venue_url: 409, release_passed: 409, duplicate: 409 };

export async function handleCreateAlarm(request: Request, env: Pick<Env, 'DB' | 'DROP_ALARM_WF'>, auth: Auth, now: Date): Promise<Response> {
  const body = await readJson(request);
  if (body === null) return error(400, 'bad_input', 'Send a JSON body.');
  const res = await createDropAlarm(env.DB, auth.household_id, { venue_entity_id: body['venue_entity_id'], reservation_date: body['reservation_date'], party_size: body['party_size'] }, now);
  if (!res.ok) return error(ALARM_STATUS[res.code] ?? 400, res.code, res.message);
  try {
    await env.DROP_ALARM_WF.create({ id: res.alarm_id, params: { alarm_id: res.alarm_id, household_id: auth.household_id } });
  } catch (err) {
    await env.DB.prepare(`UPDATE drop_alarms SET status = 'canceled' WHERE alarm_id = ?`).bind(res.alarm_id).run();
    throw err;
  }
  return json(201, { alarm_id: res.alarm_id, release_at: res.release_at, wakes_in_ms: res.sleep_ms });
}

export async function handleCreateWatch(request: Request, env: Pick<Env, 'DB' | 'PRICE_WATCH_WF'>, auth: Auth, now: Date): Promise<Response> {
  const body = await readJson(request);
  if (body === null) return error(400, 'bad_input', 'Send a JSON body.');
  const res = await createPriceWatch(
    env.DB,
    auth.household_id,
    { origin: body['origin'], destination: body['destination'], depart_date: body['depart_date'], return_date: body['return_date'], adults: body['adults'], ceiling_cents: body['ceiling_cents'], booking_domain: body['booking_domain'] },
    now,
  );
  if (!res.ok) return error(res.code === 'watch_limit' ? 409 : 400, res.code, res.message);
  try {
    await env.PRICE_WATCH_WF.create({ id: res.watch_id, params: { watch_id: res.watch_id, household_id: auth.household_id } });
  } catch (err) {
    await env.DB.prepare('UPDATE price_watches SET active = 0 WHERE watch_id = ?').bind(res.watch_id).run();
    throw err;
  }
  return json(201, { watch_id: res.watch_id });
}

export async function handleConciergeAction(env: Pick<Env, 'DB'>, auth: Auth, route: NonNullable<ReturnType<typeof parseConciergePath>>): Promise<Response> {
  const res =
    route.kind === 'alarm'
      ? await env.DB.prepare(`UPDATE drop_alarms SET status = 'canceled' WHERE alarm_id = ? AND household_id = ? AND status IN ('scheduled', 'staged')`).bind(route.id, auth.household_id).run()
      : await env.DB.prepare('UPDATE price_watches SET active = 0 WHERE watch_id = ? AND household_id = ? AND active = 1').bind(route.id, auth.household_id).run();
  if ((res.meta.changes ?? 0) > 0) return json(200, { status: route.kind === 'alarm' ? 'canceled' : 'stopped', id: route.id });
  const exists =
    route.kind === 'alarm'
      ? await env.DB.prepare('SELECT 1 AS ok FROM drop_alarms WHERE alarm_id = ? AND household_id = ?').bind(route.id, auth.household_id).first<{ ok: number }>()
      : await env.DB.prepare('SELECT 1 AS ok FROM price_watches WHERE watch_id = ? AND household_id = ?').bind(route.id, auth.household_id).first<{ ok: number }>();
  return exists === null ? error(404, 'not_found', 'No such alarm or watch.') : json(200, { status: 'already_ended', id: route.id });
}
