/**
 * src/builder/recipeLogic.ts — the pure parts of the recipe app: search tokens, the query match, validation of a model-parsed recipe, and the
 * deterministic meal suggestions. No I/O, so the live app (D1) and the smoke test (in memory) run exactly the same rules.
 */

export interface RecipeData {
  ingredients: string[];
  steps: string[];
  tags: string[];
  prep_minutes: number;
  image_r2_key: string | null;
  source_quote: string;
}

export interface ParsedRecipe {
  title: string;
  ingredients: string[];
  steps: string[];
  tags: string[];
  prep_minutes: number;
}

const fold = (s: string): string => s.normalize('NFKD').replace(/[̀-ͯ]/g, '').toLowerCase();

export function words(s: string): string[] {
  return fold(s).split(/[^a-z0-9]+/).filter((w) => w.length >= 2);
}

export const tagToken = (tag: string): string => `#${fold(tag).trim().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '')}`;

/** ' ' + title, ingredient and tag words + ' ' (tags as '#tag'): every word can then be matched as ' word%' by a plain LIKE. */
export function searchTokens(r: Pick<ParsedRecipe, 'title' | 'ingredients' | 'tags'>): string {
  const all = new Set<string>([...words(r.title), ...r.ingredients.flatMap(words), ...r.tags.map(tagToken).filter((t) => t.length > 1)]);
  return ` ${[...all].slice(0, 300).join(' ')} `;
}

export const MAX_QUERY_WORDS = 8;
export const queryWords = (q: string): string[] => [...new Set(words(q))].slice(0, MAX_QUERY_WORDS);

/** A recipe matches when EVERY query word starts some stored word (the same rule the SQL LIKE implements). */
export function matchesQuery(tokens: string, q: string, tag: string): boolean {
  if (tag !== '' && !tokens.includes(` ${tagToken(tag)} `)) return false;
  return queryWords(q).every((w) => tokens.includes(` ${w}`));
}

/** LIKE patterns for the same rule; `%`, `_` and `\` in a word are escaped (words are alphanumeric, so none can occur, but the escape is the guard). */
export const likeEscape = (s: string): string => s.replace(/[\\%_]/g, '\\$&');

const clean = (v: unknown, max: number): string | null => (typeof v === 'string' && v.trim() !== '' ? v.replace(/\s+/g, ' ').trim().slice(0, max) : null);

function list(v: unknown, maxItems: number, maxLen: number): string[] | null {
  if (!Array.isArray(v)) return null;
  const out: string[] = [];
  for (const item of v.slice(0, maxItems)) {
    const s = clean(item, maxLen);
    if (s !== null) out.push(s);
  }
  return out;
}

/** The model's JSON, checked field by field. Null when it is not a usable recipe. */
export function normalizeParsed(raw: unknown): ParsedRecipe | null {
  if (raw === null || typeof raw !== 'object') return null;
  const o = raw as Record<string, unknown>;
  const title = clean(o['title'], 120);
  const ingredients = list(o['ingredients'], 60, 200);
  const steps = list(o['steps'], 40, 600);
  const tags = list(o['tags'], 12, 30);
  if (title === null || ingredients === null || steps === null || tags === null) return null;
  if (ingredients.length === 0 && steps.length === 0) return null;
  const minutes = typeof o['prep_minutes'] === 'number' && Number.isFinite(o['prep_minutes']) ? Math.round(o['prep_minutes']) : 0;
  return { title, ingredients, steps, tags: [...new Set(tags.map((t) => t.toLowerCase()))], prep_minutes: Math.min(1440, Math.max(0, minutes)) };
}

/** A verbatim piece of the (redacted) source that shows where the recipe came from: the line that holds the title, else the first line. */
export function sourceQuote(text: string, title: string): string {
  const lines = text.split('\n').map((l) => l.trim()).filter((l) => l !== '');
  const hit = lines.find((l) => fold(l).includes(fold(title)));
  return (hit ?? lines[0] ?? '').slice(0, 200);
}

/** Deterministic parse of "Title / Ingredients: a, b / Steps: 1. x" text, for the smoke test (no model, no network). */
export function heuristicParse(text: string): ParsedRecipe {
  const lines = text.split('\n').map((l) => l.trim()).filter((l) => l !== '');
  const title = (lines[0] ?? 'Untitled').slice(0, 120);
  const ing = lines.find((l) => /^ingredients?\s*:/i.test(l));
  const steps = lines.filter((l) => /^\d+[.)]\s+/.test(l)).map((l) => l.replace(/^\d+[.)]\s+/, ''));
  return {
    title,
    ingredients: ing === undefined ? [] : ing.replace(/^ingredients?\s*:/i, '').split(',').map((s) => s.trim()).filter((s) => s !== ''),
    steps,
    tags: [],
    prep_minutes: 0,
  };
}

// ─── suggestions ────────────────────────────────────────────────────────────

const CONFLICTS: Readonly<Record<string, RegExp>> = {
  'gluten-free': /\b(?:wheat|flour|bread|breadcrumbs?|pasta|noodles?|spaghetti|couscous|barley|rye|soy sauce|tortillas?|pita|crackers?|beer|seitan)\b/,
  vegetarian: /\b(?:chicken|beef|pork|lamb|turkey|bacon|ham|sausage|fish|salmon|tuna|shrimp|prawns?|anchov\w*|gelatin|steak|duck)\b/,
  vegan: /\b(?:chicken|beef|pork|lamb|turkey|bacon|ham|sausage|fish|salmon|tuna|shrimp|prawns?|anchov\w*|gelatin|steak|duck|egg|eggs|milk|cheese|butter|cream|yogurt|honey|ghee)\b/,
  'dairy-free': /\b(?:milk|cheese|butter|cream|yogurt|ghee|whey)\b/,
  'nut-free': /\b(?:nuts?|almonds?|cashews?|walnuts?|pecans?|pistachios?|peanuts?|hazelnuts?)\b/,
  'shellfish-free': /\b(?:shrimp|prawns?|crab|lobster|clams?|mussels?|oysters?|scallops?)\b/,
};

export const restrictionKey = (r: string): string => fold(r).trim().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');

/** Ingredients that are fine for the restriction even though a word in them looks like a conflict (rice noodles, tamari, ...). */
const safeText = (s: string): string => fold(s).replace(/\brice (?:noodles?|flour|paper|vermicelli)\b/g, 'rice').replace(/\bglass noodles?\b/g, 'rice').replace(/\btamari\b/g, 'tamari');

/** The restrictions this meal breaks, by name. Restrictions the table does not know are never reported as broken (and are named in the reason instead). */
export function brokenRestrictions(ingredients: readonly string[], title: string, restrictions: readonly string[]): string[] {
  const text = safeText(`${title} ${ingredients.join(' ')}`);
  return restrictions.filter((r) => CONFLICTS[restrictionKey(r)]?.test(text) === true);
}

interface Idea {
  title: string;
  tags: string[];
  ingredients: string[];
}

const IDEAS: Readonly<Record<string, readonly Idea[]>> = {
  thai: [
    { title: 'Thai basil chicken over jasmine rice', tags: ['thai'], ingredients: ['chicken', 'thai basil', 'garlic', 'chilies', 'fish sauce', 'jasmine rice'] },
    { title: 'Green curry with vegetables and rice noodles', tags: ['thai'], ingredients: ['green curry paste', 'coconut milk', 'vegetables', 'rice noodles'] },
    { title: 'Tom kha soup', tags: ['thai'], ingredients: ['coconut milk', 'galangal', 'mushrooms', 'lime', 'chicken stock'] },
  ],
  italian: [
    { title: 'Risotto with mushrooms and thyme', tags: ['italian'], ingredients: ['arborio rice', 'mushrooms', 'thyme', 'parmesan'] },
    { title: 'Caprese chicken with tomatoes and mozzarella', tags: ['italian'], ingredients: ['chicken', 'tomatoes', 'mozzarella', 'basil'] },
  ],
  mexican: [
    { title: 'Chicken and black bean rice bowls', tags: ['mexican'], ingredients: ['chicken', 'black beans', 'rice', 'lime', 'cilantro'] },
    { title: 'Corn tortilla tacos with roasted vegetables', tags: ['mexican'], ingredients: ['corn tortillas', 'peppers', 'onion', 'avocado'] },
  ],
  indian: [
    { title: 'Chana masala with basmati rice', tags: ['indian'], ingredients: ['chickpeas', 'tomatoes', 'onion', 'garam masala', 'basmati rice'] },
  ],
};

const GENERIC: readonly Idea[] = [
  { title: 'Sheet-pan chicken and vegetables', tags: [], ingredients: ['chicken', 'potatoes', 'carrots', 'olive oil'] },
  { title: 'Rice bowls with eggs and greens', tags: [], ingredients: ['rice', 'eggs', 'spinach', 'sesame oil'] },
  { title: 'Lentil and vegetable soup', tags: [], ingredients: ['lentils', 'carrots', 'celery', 'tomatoes'] },
  { title: 'Baked salmon with lemon and greens', tags: [], ingredients: ['salmon', 'lemon', 'green beans'] },
];

export interface Suggestion {
  title: string;
  reason: string;
  source: 'saved' | 'idea';
  record_id: string | null;
  tags: string[];
}

export interface SuggestInput {
  records: ReadonlyArray<{ record_id: string; title: string; created_at: string; data: Pick<RecipeData, 'ingredients' | 'tags'> }>;
  cuisines: readonly string[];
  restrictions: readonly string[];
}

/**
 * Three meal ideas: the household's saved recipes first, then ideas for its preferred cuisines, then plain ones. A meal that breaks one of its
 * dietary restrictions is never offered. Order is fixed by score, then newest, then title, so the same facts always give the same answer.
 */
export function suggestMeals(input: SuggestInput): Suggestion[] {
  const cuisines = input.cuisines.map(fold);
  const restrictions = [...new Set(input.restrictions.map((r) => r.trim()).filter((r) => r !== ''))];
  const unknown = restrictions.filter((r) => CONFLICTS[restrictionKey(r)] === undefined);
  const fits = (c: Idea): boolean => brokenRestrictions(c.ingredients, c.title, restrictions).length === 0;
  const reasonFor = (kind: string, cuisine: string | null): string => {
    const parts = [kind];
    if (cuisine !== null) parts.push(`fits your ${cuisine} preference`);
    const known = restrictions.filter((r) => CONFLICTS[restrictionKey(r)] !== undefined);
    if (known.length > 0) parts.push(`${known.join(', ')} safe`);
    if (unknown.length > 0) parts.push(`check it against ${unknown.join(', ')}`);
    return parts.join(' · ');
  };
  const matchCuisine = (c: Idea): string | null => input.cuisines.find((u) => c.tags.some((t) => fold(t) === fold(u)) || fold(c.title).includes(fold(u))) ?? null;

  type Cand = Suggestion & { score: number; created: string };
  const cands: Cand[] = [];
  for (const r of input.records) {
    const idea: Idea = { title: r.title, tags: r.data.tags, ingredients: r.data.ingredients };
    if (!fits(idea)) continue;
    const cuisine = matchCuisine(idea);
    cands.push({ title: r.title, reason: reasonFor('Saved recipe', cuisine), source: 'saved', record_id: r.record_id, tags: r.data.tags, score: 10 + (cuisine === null ? 0 : 5), created: r.created_at });
  }
  const bank = [...cuisines.flatMap((c) => IDEAS[c] ?? []), ...GENERIC];
  for (const idea of bank) {
    if (!fits(idea) || cands.some((c) => c.title === idea.title)) continue;
    const cuisine = matchCuisine(idea);
    cands.push({ title: idea.title, reason: reasonFor('Idea', cuisine), source: 'idea', record_id: null, tags: idea.tags, score: cuisine === null ? 1 : 3, created: '' });
  }
  cands.sort((a, b) => b.score - a.score || (a.created < b.created ? 1 : a.created > b.created ? -1 : 0) || a.title.localeCompare(b.title));
  return cands.slice(0, 3).map(({ score: _s, created: _c, ...s }) => s);
}
