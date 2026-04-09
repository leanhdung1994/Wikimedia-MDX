from config import *

headers = {
    "Accept-Encoding": "gzip, deflate, sdch",
    "Accept-Language": "en-US,en;q=0.8",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
}

JS_selectors = {
    "wiki": {
        "en": [
            "References",
            "Sources",
            "Notes",
            "See_also",
            "Further_reading",
            "External_links",
            "Bibliography",
            "Sources_and_further_reading",
            "List_of_publications",
            "Selected_publications",
            "Publications",
            "Major_publications",
            "Works",
            "Honours",
            "Notable_awards",
            "Prizes",
            "Books",
            "Honours_and_decorations",
            "Awards",
            "Citations",
            "General_and_cited_sources",
            "Selected_works",
        ],
        "fr": [
            "Notes_et_références",
            "Annexes",
            "Voir_aussi",
            "Références",
            "Liens_externes",
            "Bibliographie",
            "Publications",
            "Sélection_de_publications",
            "Distinctions",
            "Distinctions_et_prix",
            "Récompenses_et_distinctions",
            "Sélection_de_travaux",
            "Discographie",
            "Articles_connexes",
            "Récompenses",
            "Note_et_référence",
            "Prix_et_distinctions",
            "Publications_notables",
        ],
        "ja": ["脚注", "関連項目", "外部リンク", "参考文献"],
        "zh": [
            "參見",
            "参考文献",
            "外部連結",
            "延伸阅读",
            "引文",
            "注释",
            "外部链接",
            "参见",
        ],
    },
    "wiktionary": {"en": [], "fr": []},
}


def split_modules(lst, n):
    tmp = [lst[i : i + n] for i in range(0, len(lst), n)]
    return tmp


def split_points() -> list:
    return ["&modules=", "%7C", "&"]


def process_module_url(module_url: str) -> list:
    splits = split_points()
    modules = module_url.split(splits[0], 1)[1]
    modules = modules.split(splits[2], 1)[0]
    modules = modules.split(splits[1])
    return modules


class CssJsFactory:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.con = duckdb.connect()
        self.base_dir = Path(__file__).parent / "css_js"

    def base_begin_end_url(self) -> list:
        splits = split_points()
        export_query = f"""
                        SELECT base_url, module_url
                        FROM read_parquet('{self.cfg.modules_path}')
                        LIMIT 1
                        """
        row = self.con.execute(export_query).fetchone()

        base_url, module_url = row
        if not base_url.startswith("https:"):
            base_url = "https:" + base_url

        begin_url = module_url.split(splits[0], 1)[0] + splits[0]
        module_url = module_url.split(splits[0], 1)[1]
        end_url = splits[2] + module_url.split(splits[2], 1)[1]
        return [base_url, begin_url, end_url]

    def collect_modules(self) -> list:
        all_modules = set()
        export_query = f"""
                        SELECT module_url
                        FROM read_parquet('{self.cfg.modules_path}')
                        """
        result = self.con.execute(export_query)

        while rows := result.fetchmany(100_000):
            for (module_url,) in rows:
                modules = process_module_url(module_url)
                all_modules.update(modules)
        return list(all_modules)

    def collect_css(self, export_csslink=False) -> None:
        print()
        print("===== Generate CSS and JS =====")

        [base_url, begin_url, end_url] = self.base_begin_end_url()
        splits = split_points()
        all_modules = self.collect_modules()
        list_modules = split_modules(all_modules, 20)
        modules, links = [], []
        modules.append("/* Official CSS from Wikimedia */")

        for lst in list_modules:
            module_url = begin_url + splits[1].join(lst) + end_url
            css_url = urljoin(base_url, module_url)
            response = requests.get(css_url, headers=headers)
            links.append(css_url)
            if response.status_code == 200:
                modules.append(response.text)
            else:
                modules.append("ERROR!")
            time.sleep(2)

        input_css_paths = [
            self.base_dir / "common.css",
            self.base_dir / f"{self.cfg.project_code}.css",
            self.base_dir / f"{self.cfg.lang_proj}.css",
        ]
        for path in input_css_paths:
            if path.is_file():
                tmp = path.read_text(encoding="utf-8")
                modules.append(tmp)

        data = "\n\n".join(modules)
        self.cfg.css_path.write_text(data, encoding="utf-8")

        data = "\n\n".join(links)
        if export_csslink:
            self.cfg.csslink_path.write_text(data, encoding="utf-8")

    def collect_js(self) -> None:
        input_js_paths = [
            self.base_dir / "common.js",
            self.base_dir / f"{self.cfg.project_code}.js",
            self.base_dir / f"{self.cfg.lang_proj}.js",
        ]
        data = []
        for path in input_js_paths:
            if path.is_file():
                tmp = path.read_text(encoding="utf-8")
                data.append(tmp)
        data = "\n\n".join(data)

        if self.cfg.project_code == "wiki":
            h2_id = JS_selectors["wiki"].get(self.cfg.language_code)
            h2_id = [f"{{headlineId: '{d}'}}" for d in h2_id]
            h2_id = ", ".join(h2_id)
            data = data.replace("REPLACETHIS", h2_id, 1)
        self.cfg.js_path.write_text(data, encoding="utf-8")

    def delete_parquet(self):
        if self.cfg.modules_path.is_file():
            self.cfg.modules_path.unlink(missing_ok=True)


def collect_css_and_js(cfg: Config, delete_parquet=False) -> None:
    if cfg.ndjson_names_left:
        return
    elif not cfg.modules_path.is_file():
        print("The file containing modules does not exist!")
        return
    p = CssJsFactory(cfg)
    p.collect_css()
    p.collect_js()
    if delete_parquet:
        p.delete_parquet()
    print("CSS and JS generation is complete")
