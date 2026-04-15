from config import Config
from selectolax.lexbor import LexborHTMLParser
import string

# CSS selectors that control which DOM nodes are removed during HTML cleaning.
CSS_selectors = {
    # Always removed regardless of project or mode:
    "selectors": ["head", "script", "link", "meta", "base", '[class*="mw-empty-elt"]'],
    # Project-specific additions:
    "wiki_selectors": [],  # Reserved for future wiki-specific pruning
    "wiktionary_selectors": [
        'td[class*="audiometa"]',
        '[class*="sister-wikipedia"]',
        '[class*="NavFrame"]',
        '[class*="maintenance-line"]',
        '[class*="thumb"]',
        '[class*="maintenance-box"]',
        '[class*="floatright"]',
    ],
    # Additional selectors applied only in "greedy" mode (removes more boilerplate).
    "wiktionary_greedy": {
        "en": {
            "parent": [
                '[property*="mw:PageProp/toc"]',
                '[class*="disambig-see-also"]',
                '[class*="audiotable"]',
                '[id*="Derived_terms"]',
                '[id*="Descendants"]',
                '[id*="Related_terms"]',
                '[id*="See_also"]',
                '[id*="Anagrams"]',
                '[id*="Alternative_forms"]',
                '[id*="Translations"]',
                '[id*="Usage_notes"]',
                '[id*="Pronunciation_notes"]',
                '[id*="Particle"]',
                '[id*="Interjection"]',
                '[id*="Hyponyms"]',
                '[id*="Notes"]',
                '[id*="Further_reading"]',
                '[id*="Hypernyms"]',
            ],
            "extra_common": [],
        },
        "fr": {
            "parent": [
                '[property*="mw:PageProp/toc"]',
                '[id*="Notes"]',
                '[id*="Dérivés"]',
                '[id*="Apparentés_étymologiques"]',
                '[id*="Vocabulaire_apparenté_par_le_sens"]',
                '[id*="Proverbes_et_phrases_toutes_faites"]',
                '[id*="Traductions"]',
                '[id*="Traductions_à_trier"]',
                '[id*="Prononciation"]',
                '[id*="Anagrammes"]',
                '[id*="Variantes"]',
                '[id*="Voir_aussi"]',
                '[id*="Hyperonymes"]',
                '[id*="Hyponymes"]',
                '[id*="Homophones"]',
                '[id*="Holonymes"]',
                '[id*="Méronymes"]',
            ],
            "extra_common": [
                '[class*="bandeau"]',
                '[title*="Prononciation à préciser"]',
            ],
        },
    },
}


def is_good_char(s: str) -> bool:
    """
    Returns True if the entry name contains only characters valid for a wiktionary
    lookup key.
    """
    good_chars = string.ascii_lowercase + "ùûüÿàâæçéèêëïîôœ" + "'_- "
    s = s.lower()
    tmp = True
    for c in s:
        if c not in good_chars:
            tmp = False
            break
    return tmp


def process_link(url: str) -> str:
    """
    Normalises a Wikimedia image URL:
      - Ensures the scheme is 'https:'.
      - Converts thumbnail URLs (…/thumb/…/file.png/200px-file.png) to the
        full-resolution URL (…/file.png) by removing the /thumb/ segment and the
        final size-prefixed copy.
    """
    url = url.split()[-1]
    if not url.startswith("https:"):
        url = "https:" + url
    tmp = url.rsplit("/", 2)
    if ("/thumb/" in url) and (tmp[-2] in tmp[-1]):
        url = url.replace("/thumb/", "/")
        url = url.rsplit("/", 1)[0]
    return url


def process_img(tree: LexborHTMLParser) -> LexborHTMLParser:
    """
    Adds a 'truesrc' attribute to every <img> pointing to the full-resolution image.
    """
    for d in tree.css("img[src]"):
        d.attrs["truesrc"] = process_link(d.attrs["src"])
    return tree


def process_wikilink(tree: LexborHTMLParser) -> LexborHTMLParser:
    """
    Rewrites internal wiki hyperlinks to use the 'entry://' scheme so that the
    MDX reader application can resolve cross-references between dictionary entries.
    Falls back to the link's text content when no 'title' attribute is present.
    Namespace prefixes (e.g. "wikt:") are stripped from titles.
    """
    for d in tree.css('[rel*="mw:WikiLink"]'):
        if "title" in d.attributes:
            title = d.attrs["title"]
            title = title.split(":", 1)[1] if ":" in title else title
            d.attrs["href"] = f"entry://{title}"
        elif d.text():
            d.attrs["href"] = f"entry://{d.text(deep=False, strip=True)}"
    return tree


class HtmlFactory:
    """
    Transforms raw Wikimedia HTML (from the dump JSON) into a compact, self-contained
    HTML fragment for inclusion in an MDX dictionary entry.
    """

    def __init__(self, cfg: Config):
        self.cfg = cfg

    def get_modules(self, tree: LexborHTMLParser) -> dict:
        """
        Extracts the base URL and the load.php module URL from the page <head>.
        """
        base_url = tree.css_first("base")
        base_url = base_url.attrs["href"]
        module_url = tree.css_first(
            f'link[rel="stylesheet"][href*="load.php?lang={self.cfg.language_code}&modules="]'
        )
        module_url = module_url.attrs["href"]
        modules = {"base_url": base_url, "module_url": module_url}
        return modules

    def get_subtree_and_modules(
        self, entry_name: str, html: str
    ) -> list[dict, LexborHTMLParser] | None:
        """
        Parses the full page HTML and returns (modules_dict, relevant_subtree).

        For 'wiki': returns the entire parsed tree (full article).
        For 'wiktionary': extracts only the <section> containing the target language
          heading (e.g. <h2 id="English">), replaces the generic heading with one
          showing the entry name, and returns that section only.
          Returns None if the target language section isn't found or entry_name
          contains unsupported characters.
        """
        if self.cfg.project_code == "wiki":
            tree = LexborHTMLParser(html)
            modules = self.get_modules(tree)
            return [modules, tree]
        elif (self.cfg.project_code == "wiktionary") and is_good_char(entry_name):
            tree = LexborHTMLParser(html)
            modules = self.get_modules(tree)

            # Select the section whose direct child h2 matches the target language
            tree = tree.css_first(
                f'section[data-mw-section-id]:has(> h2[id="{self.cfg.language}"])'
            )
            if not tree:
                return

            # Replace the generic language heading with the actual entry name
            d = tree.css_first(
                f'section[data-mw-section-id] > h2[id="{self.cfg.language}"]'
            )
            new_node = LexborHTMLParser(
                f'<h2 id="{self.cfg.language}">{entry_name}</h2>'
            ).root
            d.insert_before(new_node)
            d.decompose()
            return [modules, tree]
        else:
            return

    def prune_tree(self, tree: LexborHTMLParser) -> LexborHTMLParser:
        """
        Removes unwanted DOM nodes according to CSS_selectors.
        In greedy mode, also removes parent section blocks that contain boilerplate
        headings (translations, synonyms, etc.) by using CSS :has() selectors.
        """
        selectors = list(CSS_selectors["selectors"])
        if self.cfg.project_code == "wiki":
            selectors += CSS_selectors["wiki_selectors"]
        elif self.cfg.project_code == "wiktionary":
            selectors += CSS_selectors["wiktionary_selectors"]
            if self.cfg.parse_mode == "greedy":
                lang_cfg = CSS_selectors["wiktionary_greedy"].get(
                    self.cfg.language_code
                )
                selectors += lang_cfg["extra_common"]

                # Remove any element whose first child matches a boilerplate heading
                selectors += [f":has(> :first-child{d})" for d in lang_cfg["parent"]]
        selectors = ",".join(selectors)
        for d in tree.css(selectors):
            d.decompose()
        return tree

    def process_html(self, entry_name: str, html: str) -> list[dict, str] | None:
        """
        Full processing pipeline for one page's HTML:
          1. Extract the relevant subtree and module metadata.
          2. Prune boilerplate nodes.
          3. Rewrite image srcs and wiki hyperlinks.
          4. Collapse whitespace and format as an MDX entry block:
               <entry_name>
               <css/js link tags>
               <html content>
               </>
        Returns None if the entry should be skipped.
        """
        tmp = self.get_subtree_and_modules(entry_name, html)
        if not tmp:
            return
        else:
            [modules, tree] = tmp
        css_js = [
            f'<link href="{self.cfg.prefix_lang_proj}.css" rel="stylesheet" type="text/css"/>',
            f'<script src="{self.cfg.prefix_lang_proj}.js" type="text/javascript"></script>',
        ]
        tree = self.prune_tree(tree)
        tree = process_img(tree)
        tree = process_wikilink(tree)
        content = tree.html

        # Collapse all whitespace/newlines into single spaces for compact output
        content = " ".join(line.strip() for line in content.split("\n") if line.strip())
        content = [entry_name, " ".join(css_js), content, "</>"]
        content = "\n".join(content)
        return [modules, content]

    def process_json(self, json: dict) -> dict | None:
        """
        Entry point called by NdjsonFactory for each JSON line in a shard.

        Extracts entry name and HTML from the dump record, delegates to process_html,
        then wraps the result in a record dict ready for orjson serialisation.
        The bin index is derived from (identifier % n_bins) for deterministic sharding.

        Returns [bin_index, record_dict] or None if the entry is filtered out.
        """
        html = json["article_body"]["html"]
        entry_name = json["name"]
        tmp = self.process_html(entry_name, html)
        if not tmp:
            return
        else:
            [modules, html] = tmp
        identifier = json["identifier"]
        bin = identifier % self.cfg.n_bins  # Stable hash for deduplication later
        data = {
            "entry_name": entry_name,
            "dateModified": json["date_modified"],
            "identifier": identifier,
            "html": html,
            "modules": modules,
        }
        return [bin, data]
