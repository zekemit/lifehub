/**
 * src/builder/recipeTemplate.ts — the reference recipe-book bundle: a mobile-first single page (photo upload, instant search, tag filters, recipe
 * cards, a "What Should We Cook?" drawer) over the runtime's own routes. It is the worked example inside the code_gen prompt, so the model writes
 * to a known-good shape, and it is what the test suite's scripted model returns. It passes sandboxVerifier by construction (a test pins that).
 *
 * The page draws every recipe with textContent, never innerHTML, calls only literal relative 'api/...' paths, and carries the install token (?t=)
 * through to each call, because an embedded page has no session cookie.
 */

import { APP_TEMPLATES, type Bundle } from './sandboxVerifier';

const PAGE = String.raw`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title>
<style>
:root{color-scheme:light dark;--bg:#fffaf3;--fg:#2b2118;--card:#fff;--line:#e6d9c8;--accent:#b4491f}
@media (prefers-color-scheme:dark){:root{--bg:#1d1812;--fg:#f4ece0;--card:#2a231b;--line:#4a3f33;--accent:#f09a6b}}
*{box-sizing:border-box}body{margin:0;font:16px/1.45 system-ui,sans-serif;background:var(--bg);color:var(--fg)}
header,main{max-width:46rem;margin:0 auto;padding:1rem}h1{margin:.2rem 0 0;font-size:1.5rem}
section{margin:1rem 0}label{display:block;font-weight:600;margin:.4rem 0 .2rem}
input,textarea,button{font:inherit;width:100%;padding:.7rem;border:1px solid var(--line);border-radius:.6rem;background:var(--card);color:var(--fg)}
button{background:var(--accent);color:#fff;border:0;font-weight:600;min-height:44px;cursor:pointer}
button.tag{width:auto;display:inline-block;margin:.2rem .2rem 0 0;padding:.35rem .7rem;min-height:0;background:var(--card);color:var(--fg);border:1px solid var(--line)}
button.tag[aria-pressed="true"]{background:var(--accent);color:#fff}
ul{list-style:none;padding:0;margin:0}li.card{background:var(--card);border:1px solid var(--line);border-radius:.8rem;padding:.8rem;margin:.6rem 0}
li.card h3{margin:.1rem 0}.meta{opacity:.75;font-size:.9rem}#drawer{background:var(--card);border:1px solid var(--line);border-radius:.8rem;padding:.8rem;margin-top:.6rem}
#status{min-height:1.4rem}
</style>
</head>
<body>
<header><h1>__TITLE__</h1><p class="meta">Snap a recipe, keep it, find it again.</p></header>
<main>
<section aria-labelledby="add-h">
<h2 id="add-h">Add a recipe</h2>
<label for="photo">Photo of a recipe</label>
<input id="photo" type="file" accept="image/*" capture="environment" aria-label="Photo of a recipe">
<label for="ocr">Recipe text (typed, or copied from the photo)</label>
<textarea id="ocr" rows="5" aria-label="Recipe text"></textarea>
<button id="save" type="button">Save recipe</button>
<p id="status" role="status" aria-live="polite"></p>
</section>
<section aria-labelledby="find-h">
<h2 id="find-h">Find a recipe</h2>
<input id="q" type="search" placeholder="Ingredient, title or tag" aria-label="Search recipes by ingredient, title or tag">
<div id="tags" role="group" aria-label="Filter by tag"></div>
<ul id="results" aria-live="polite"></ul>
</section>
<section aria-labelledby="cook-h">
<h2 id="cook-h">Dinner ideas</h2>
<button id="cook" type="button" aria-expanded="false" aria-controls="drawer">What Should We Cook?</button>
<aside id="drawer" hidden><ul id="ideas"></ul></aside>
</section>
</main>
<script>
(function () {
  var token = new URLSearchParams(location.search).get('t');
  function tk() { return token ? '&t=' + encodeURIComponent(token) : ''; }
  function el(id) { return document.getElementById(id); }
  function node(tag, text, cls) { var n = document.createElement(tag); if (text !== undefined) n.textContent = text; if (cls) n.className = cls; return n; }
  var activeTag = '';
  function card(r) {
    var li = node('li', undefined, 'card');
    li.appendChild(node('h3', r.title));
    var d = r.data || {};
    li.appendChild(node('p', (d.prep_minutes ? d.prep_minutes + ' min · ' : '') + ((d.tags || []).join(', ')), 'meta'));
    var ing = node('p', 'Ingredients: ' + (d.ingredients || []).join(', '));
    li.appendChild(ing);
    var ol = node('ol');
    (d.steps || []).forEach(function (s) { ol.appendChild(node('li', s)); });
    li.appendChild(ol);
    return li;
  }
  function paint(records) {
    var ul = el('results'); ul.textContent = '';
    if (!records.length) { ul.appendChild(node('li', 'No recipes yet.', 'meta')); return; }
    records.forEach(function (r) { ul.appendChild(card(r)); });
    var seen = {}; records.forEach(function (r) { ((r.data || {}).tags || []).forEach(function (t) { seen[t] = 1; }); });
    var box = el('tags'); box.textContent = '';
    Object.keys(seen).sort().forEach(function (t) {
      var b = node('button', t, 'tag'); b.type = 'button'; b.setAttribute('aria-pressed', String(t === activeTag));
      b.addEventListener('click', function () { activeTag = activeTag === t ? '' : t; search(); });
      box.appendChild(b);
    });
  }
  function search() {
    window.fetch('api/search?q=' + encodeURIComponent(el('q').value) + (activeTag ? '&tag=' + encodeURIComponent(activeTag) : '') + tk())
      .then(function (r) { return r.json(); }).then(function (j) { paint(j.records || []); })
      .catch(function () { el('status').textContent = 'Search is unavailable right now.'; });
  }
  var timer = 0;
  el('q').addEventListener('input', function () { clearTimeout(timer); timer = setTimeout(search, 150); });
  function save(imageB64, imageType) {
    var body = { text: el('ocr').value };
    if (imageB64) { body.image_b64 = imageB64; body.image_type = imageType; }
    el('status').textContent = 'Reading the recipe…';
    window.fetch('api/parse-recipe' + (token ? '?t=' + encodeURIComponent(token) : ''), { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) })
      .then(function (r) { return r.json().then(function (j) { return { status: r.status, body: j }; }); })
      .then(function (x) {
        if (x.status === 429) { el('status').textContent = 'This month’s recipe-reading limit is used up.'; return; }
        if (x.status !== 200) { el('status').textContent = (x.body && x.body.message) || 'That recipe could not be read.'; return; }
        el('status').textContent = 'Saved: ' + x.body.record.title; el('ocr').value = ''; search();
      })
      .catch(function () { el('status').textContent = 'That recipe could not be saved.'; });
  }
  el('save').addEventListener('click', function () {
    var f = el('photo').files && el('photo').files[0];
    if (!f) { save(null, null); return; }
    var reader = new FileReader();
    reader.onload = function () { var s = String(reader.result); save(s.slice(s.indexOf(',') + 1), f.type); };
    reader.readAsDataURL(f);
  });
  el('cook').addEventListener('click', function () {
    var drawer = el('drawer'); var open = drawer.hidden;
    drawer.hidden = !open; el('cook').setAttribute('aria-expanded', String(open));
    if (!open) return;
    window.fetch('api/suggest?x=1' + tk()).then(function (r) { return r.json(); }).then(function (j) {
      var ul = el('ideas'); ul.textContent = '';
      (j.suggestions || []).forEach(function (s) { var li = node('li', s.title, 'card'); li.appendChild(node('p', s.reason, 'meta')); ul.appendChild(li); });
    }).catch(function () { el('status').textContent = 'Ideas are unavailable right now.'; });
  });
  search();
})();
</script>
</body>
</html>`;

const escapeHtml = (s: string): string => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

export function referenceRecipeBundle(o: { slug: string; title: string }): Bundle {
  const spec = APP_TEMPLATES['recipe_book']!;
  return {
    manifest: {
      version: 1,
      template: 'recipe_book',
      slug: o.slug,
      title: o.title,
      collections: [...spec.collections],
      routes: spec.routes.map((r) => ({ ...r })),
      form_actions: spec.form_actions.map((f) => ({ ...f })),
    },
    html: PAGE.replace(/__TITLE__/g, escapeHtml(o.title)),
  };
}
