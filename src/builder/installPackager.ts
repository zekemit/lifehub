/**
 * src/builder/installPackager.ts — the `package_install` artifact: everything the member needs to put a published app on their own site, as data.
 * It performs no installation and holds no credentials: nothing here calls a site, a DNS provider or GitHub. The member (or a connected repo, after
 * their approval) applies it.
 *
 *   embed_snippet           a responsive <iframe> and a small web component that builds the same iframe, for a homepage on Squarespace, WordPress,
 *                           Webflow or plain HTML. The URL carries the install token: read-only, scoped to exactly this app.
 *   cloudflare_pages_patch  the _redirects line and the CNAME for recipes.<target_domain>.
 *   github_pr_bundle        the file(s) and message for a pull request against the site's repo, ready to commit on approval.
 */

export interface InstallInput {
  /** The origin the app is served from (https://host). */
  base_url: string;
  household_id: string;
  slug: string;
  title: string;
  install_token: string;
  /** The member's own site, when the request named one. */
  target_domain: string | null;
}

export interface InstallArtifact {
  version: 1;
  app_url: string;
  embed_snippet: { iframe: string; web_component: string };
  cloudflare_pages_patch: { subdomain: string; redirects_file: { path: string; content: string }; dns: { type: 'CNAME'; name: string; target: string } };
  github_pr_bundle: { branch: string; commit_message: string; title: string; files: Array<{ path: string; content: string }> };
}

const attr = (s: string): string => s.replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
export const DOMAIN_PLACEHOLDER = 'your-domain.example';

/** A domain named in the member's request ("on my wife's site recipes-by-jo.com"), else null. */
export function domainFromPrompt(prompt: string): string | null {
  const m = /\b((?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+(?:[a-z]{2,24}))\b/i.exec(prompt.replace(/https?:\/\//gi, ' ').replace(/\b[\w.+-]+@[\w.-]+\b/g, ' '));
  return m === null ? null : m[1]!.toLowerCase().replace(/^www\./, '');
}

export function buildInstallArtifact(i: InstallInput): InstallArtifact {
  const base = i.base_url.replace(/\/+$/, '');
  const appUrl = `${base}/apps/${i.household_id}/${i.slug}`;
  const embedUrl = `${appUrl}?t=${encodeURIComponent(i.install_token)}`;
  const iframe = `<iframe src="${attr(embedUrl)}" title="${attr(i.title)}" loading="lazy" allow="camera" referrerpolicy="no-referrer" style="width:100%;min-height:680px;border:0;border-radius:12px"></iframe>`;
  const webComponent = [
    '<script>',
    '(function () {',
    "  if (customElements.get('hosted-app')) return;",
    "  customElements.define('hosted-app', class extends HTMLElement {",
    '    connectedCallback() {',
    "      var f = document.createElement('iframe');",
    "      f.src = this.getAttribute('src'); f.title = this.getAttribute('title') || '';",
    "      f.loading = 'lazy'; f.setAttribute('allow', 'camera'); f.referrerPolicy = 'no-referrer';",
    "      f.style.cssText = 'width:100%;min-height:680px;border:0;border-radius:12px';",
    '      this.appendChild(f);',
    '    }',
    '  });',
    '})();',
    '</script>',
    `<hosted-app src="${attr(embedUrl)}" title="${attr(i.title)}"></hosted-app>`,
  ].join('\n');
  const domain = i.target_domain ?? DOMAIN_PLACEHOLDER;
  const host = new URL(base).host;
  const page = `<!doctype html>\n<html lang="en">\n<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>${attr(i.title)}</title></head>\n<body>\n<main>\n<h1>${attr(i.title)}</h1>\n${iframe}\n</main>\n</body>\n</html>\n`;
  return {
    version: 1,
    app_url: appUrl,
    embed_snippet: { iframe, web_component: webComponent },
    cloudflare_pages_patch: {
      subdomain: `recipes.${domain}`,
      redirects_file: { path: '_redirects', content: `/recipes  ${embedUrl}  302\n/recipes/*  ${embedUrl}  302\n` },
      dns: { type: 'CNAME', name: `recipes.${domain}`, target: host },
    },
    github_pr_bundle: {
      branch: `add-${i.slug}`,
      commit_message: `Add the ${i.title} page`,
      title: `Add ${i.title}`,
      files: [{ path: `${i.slug}/index.html`, content: page }],
    },
  };
}
