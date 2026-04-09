/* Click to hide/show collapsibles */
(function () {
    function makeBtn(startCollapsed) {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'mw-collapsible-toggle mw-collapsible-toggle-default'
            + (startCollapsed ? ' mw-collapsible-toggle-collapsed' : '');
        btn.setAttribute('aria-expanded', String(!startCollapsed));
        btn.tabIndex = 0;
        btn.innerHTML = '<span class="mw-collapsible-text">'
            + (startCollapsed ? 'show' : 'hide') + '</span>';
        return btn;
    }

    function attachToggle(btn, el, getCollapsed, setCollapsed) {
        btn.addEventListener('click', () => {
            const collapsed = !getCollapsed();
            setCollapsed(collapsed);
            el.classList.toggle('mw-collapsed', collapsed);
            el.classList.toggle('collapsed', collapsed);
            btn.querySelector('.mw-collapsible-text').textContent = collapsed ? 'show' : 'hide';
            btn.classList.toggle('mw-collapsible-toggle-collapsed', collapsed);
            btn.setAttribute('aria-expanded', String(!collapsed));
        });
    }

    function init() {
        /* ── div.mw-collapsible pattern (sidebar-list, collapsible-list, etc.) ── */
        document.querySelectorAll('div.mw-collapsible').forEach(el => {
            if (el.dataset.collapsibleInit) return;
            el.dataset.collapsibleInit = '1';

            const contentEl = el.querySelector('.mw-collapsible-content');
            if (!contentEl) return;

            // Title is the first child element that is not the content
            const titleEl = Array.from(el.children).find(c => !c.classList.contains('mw-collapsible-content'));
            if (!titleEl) return;

            const startCollapsed = el.classList.contains('mw-collapsed');
            const btn = makeBtn(startCollapsed);
            btn.style.float = 'right';
            btn.style.marginLeft = '0.5em';
            // Append into innermost div if title is itself a div wrapper
            const innerDiv = titleEl.tagName === 'DIV' ? (titleEl.querySelector('div') || titleEl) : titleEl;
            innerDiv.appendChild(btn);

            el.classList.remove('mw-collapsed');
            if (startCollapsed) contentEl.style.display = 'none';

            let collapsed = startCollapsed;
            attachToggle(btn, el,
                () => collapsed,
                v => { collapsed = v; contentEl.style.display = v ? 'none' : ''; }
            );
        });

        /* ── table[class*="collapsible"] pattern ── */
        document.querySelectorAll('table.mw-collapsible, table.collapsible').forEach(el => {
            if (el.dataset.collapsibleInit) return;
            el.dataset.collapsibleInit = '1';

            // Header cell is the first th in the first tr
            const headerCell = el.querySelector('tr:first-child > th');
            if (!headerCell) return;

            // Collapsible rows = all tr's after the first
            const rows = Array.from(el.querySelectorAll('tr')).slice(1);
            if (!rows.length) return;

            const startCollapsed = el.classList.contains('mw-collapsed') || el.classList.contains('collapsed');
            const btn = makeBtn(startCollapsed);
            btn.style.float = 'right';
            btn.style.marginLeft = '0.5em';
            // Title text may be wrapped in a div — append inside it so button sits on the same line
            const innerDiv = headerCell.querySelector('div');
            (innerDiv || headerCell).appendChild(btn);

            el.classList.remove('mw-collapsed');
            el.classList.remove('collapsed');
            if (startCollapsed) rows.forEach(r => r.style.display = 'none');

            let collapsed = startCollapsed;
            attachToggle(btn, el,
                () => collapsed,
                v => { collapsed = v; rows.forEach(r => r.style.display = v ? 'none' : ''); }
            );
        });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();

/* TOC */
(function () {
    'use strict';

    /* Capture synchronously — currentScript is null inside callbacks */
    var ROOT = (document.currentScript && document.currentScript.parentNode)
        || null;

    /* ── 1. Scope: prefer the captured root, fall back to first match ── */
    function getScope() {
        return ROOT
            || document.querySelector('.mw-parser-output')
            || document.body;
    }

    /* ── 2. Collect h2/h3/h4 that should appear in TOC ── */
    function collectHeadings(scope) {
        return Array.from(scope.querySelectorAll('h2, h3, h4')).filter(function (h) {
            return h.id;
        });
    }

    /* ── 3. Get visible text, ignoring <span typeof="mw:FallbackId"> ── */
    function headingText(h) {
        var clone = h.cloneNode(true);
        clone.querySelectorAll('span[typeof="mw:FallbackId"]').forEach(function (s) {
            s.remove();
        });
        return (clone.innerText || clone.textContent || '').trim();
    }

    /* ── 4. Assign section numbers and build flat item list ── */
    function buildItems(headings) {
        var counters = [0, 0, 0]; /* index 0=h2, 1=h3, 2=h4 */
        return headings.map(function (h) {
            var level = parseInt(h.tagName[1], 10); /* 2, 3, or 4 */
            var idx = level - 2;                  /* 0, 1, or 2 */
            counters[idx]++;
            for (var i = idx + 1; i < counters.length; i++) counters[i] = 0;
            var number = counters.slice(0, idx + 1).join('.');
            return { id: h.id, level: level, number: number, text: headingText(h) };
        });
    }

    /* ── 5. Build nested <ul> from flat item list ── */
    function buildList(items, depth) {
        var ul = document.createElement('ul');
        var i = 0;
        while (i < items.length) {
            var item = items[i];
            if (item.level < depth) break;
            if (item.level === depth) {
                var li = document.createElement('li');
                var a = document.createElement('a');
                a.href = '#' + item.id;
                a.textContent = item.number + '\u00a0' + item.text;
                li.appendChild(a);
                /* collect direct children (deeper levels) */
                var children = [], j = i + 1;
                while (j < items.length && items[j].level > depth) children.push(items[j++]);
                if (children.length) li.appendChild(buildList(children, depth + 1));
                ul.appendChild(li);
                i = j;
            } else {
                i++;
            }
        }
        return ul;
    }

    /* ── 6. Assemble the TOC element ── */
    function buildToc(items) {
        var toc = document.createElement('div');
        toc.id = 'toc';
        toc.className = 'toc';
        toc.setAttribute('role', 'navigation');
        toc.setAttribute('aria-labelledby', 'mw-toc-heading');

        /* Title bar: just the 📑 icon — expand/collapse is pure CSS :hover */
        var titleDiv = document.createElement('div');
        titleDiv.className = 'toctitle';
        var h2 = document.createElement('h2');
        h2.id = 'mw-toc-heading';
        h2.textContent = '\uD83D\uDCD1'; /* 📑 */
        titleDiv.appendChild(h2);

        toc.appendChild(titleDiv);
        toc.appendChild(buildList(items, items[0].level));
        return toc;
    }

    /* ── 7. Find insertion point ──
     * Parsoid: body > section[data-mw-section-id="0"] (lede)
     *               > section[data-mw-section-id="1"] (Etymology) ← insert before this
     * Classic: before the first h2's nearest ancestor that is a direct child of scope */
    function getInsertionPoint(scope) {
        /* Parsoid: insert just before section 1 — i.e. at the END of the lede */
        var firstSection = scope.querySelector('section[data-mw-section-id="1"]');
        if (firstSection) {
            return firstSection;
        }
        /* Classic fallback */
        var h2 = scope.querySelector('h2');
        if (h2) {
            var el = h2;
            while (el.parentElement && el.parentElement !== scope) el = el.parentElement;
            return el;
        }
        return null;
    }

    /* ── 8. Main ── */
    function init() {
        var scope = getScope();
        if (scope.querySelector('#toc, .toc')) return; /* already present */

        var headings = collectHeadings(scope);
        if (headings.length < 3) return;

        var items = buildItems(headings);
        var toc = buildToc(items);
        var ref = getInsertionPoint(scope);

        if (ref) ref.parentElement.insertBefore(toc, ref);
        else scope.prepend(toc);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
}());