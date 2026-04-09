/* Click to hide/show references */
(function () {
    /* Capture synchronously — currentScript is null inside callbacks */
    var container = (document.currentScript && document.currentScript.parentNode)
        || document.querySelector('.mw-parser-output')
        || document;

    function init() {
        const sections = [
            REPLACETHIS
        ];

        sections.forEach(function (config) {
            const headline = container.querySelector('#' + config.headlineId);
            if (!headline) return;

            const section = headline.closest('section[data-mw-section-id]');
            if (!section) return;
            const arrow = document.createElement('span');
            arrow.className = 'toggle-arrow';
            arrow.textContent = '▼';
            headline.appendChild(arrow); /* styling via .toggle-arrow CSS rule */
            headline.style.cursor = 'pointer';
            headline.addEventListener('click', function () {
                section.toggleAttribute('data-collapsed');
                arrow.classList.toggle('collapsed');
            });
            section.setAttribute('data-collapsed', '');
            arrow.classList.add('collapsed');
        });
    }
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();