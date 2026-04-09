from config import *

CSS_selectors = {
    "selectors": ["head", "script", "link", "meta", "base", '[class*="mw-empty-elt"]'],
    "wiki_selectors": [],
    "wiktionary_selectors": [
        'td[class*="audiometa"]',
        '[class*="sister-wikipedia"]',
        '[class*="NavFrame"]',
        '[class*="maintenance-line"]',
        '[class*="thumb"]',
        '[class*="maintenance-box"]',
        '[class*="floatright"]',
    ],
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
    good_chars = string.ascii_lowercase + "ùûüÿàâæçéèêëïîôœ" + "'_- "
    s = s.lower()
    tmp = True
    for c in s:
        if c not in good_chars:
            tmp = False
            break
    return tmp


def process_link(url: str) -> str:
    url = url.split()[-1]
    if not url.startswith("https:"):
        url = "https:" + url
    tmp = url.rsplit("/", 2)
    if ("/thumb/" in url) and (tmp[-2] in tmp[-1]):
        url = url.replace("/thumb/", "/")
        url = url.rsplit("/", 1)[0]
    return url


def process_img(tree: LexborHTMLParser) -> LexborHTMLParser:
    for d in tree.css("img[src]"):
        d.attrs["truesrc"] = process_link(d.attrs["src"])
    return tree


def process_wikilink(tree: LexborHTMLParser) -> LexborHTMLParser:
    for d in tree.css('[rel*="mw:WikiLink"]'):
        if "title" in d.attributes:
            title = d.attrs["title"]
            title = title.split(":", 1)[1] if ":" in title else title
            d.attrs["href"] = f"entry://{title}"
        elif d.text():
            d.attrs["href"] = f"entry://{d.text(deep=False, strip=True)}"
    return tree


class HtmlFactory:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def get_modules(self, tree: LexborHTMLParser) -> dict:
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
        if self.cfg.project_code == "wiki":
            tree = LexborHTMLParser(html)
            modules = self.get_modules(tree)
            return [modules, tree]
        elif (self.cfg.project_code == "wiktionary") and is_good_char(entry_name):
            tree = LexborHTMLParser(html)
            modules = self.get_modules(tree)
            tree = tree.css_first(
                f'section[data-mw-section-id]:has(> h2[id="{self.cfg.language}"])'
            )
            if not tree:
                return
            for d in tree.css(
                f'section[data-mw-section-id] > h2[id="{self.cfg.language}"]'
            ):
                new_node = LexborHTMLParser(
                    f'<h2 id="{self.cfg.language}">{entry_name}</h2>'
                ).root
                d.insert_before(new_node)
                d.decompose()
            return [modules, tree]
        else:
            return

    def prune_tree(self, tree: LexborHTMLParser) -> LexborHTMLParser:
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
                selectors += [f":has(> :first-child{d})" for d in lang_cfg["parent"]]
        selectors = ",".join(selectors)
        for d in tree.css(selectors):
            d.decompose()
        return tree

    def process_html(self, entry_name: str, html: str) -> list[dict, str] | None:
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
        content = " ".join(line.strip() for line in content.split("\n") if line.strip())
        #        content = entry_name + '\n' + " ".join(css_js) + '\n' + content + '\n</>'
        content = [entry_name, " ".join(css_js), content, "</>"]
        content = "\n".join(content)
        return [modules, content]

    def process_json(self, json: dict) -> dict | None:
        """
        json is a json objectS
        """
        html = json["article_body"]["html"]
        entry_name = json["name"]
        tmp = self.process_html(entry_name, html)
        if not tmp:
            return
        else:
            [modules, html] = tmp
        identifier = json["identifier"]
        bin = identifier % self.cfg.n_bins
        data = {
            "entry_name": entry_name,
            "dateModified": json["date_modified"],
            "identifier": identifier,
            "html": html,
            "modules": modules,
        }
        return [bin, data]
