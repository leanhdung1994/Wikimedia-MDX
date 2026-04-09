/* Click to hide/show NavFrames (boîtes déroulantes) */
(function () {
    function init() {
        document.querySelectorAll('.NavFrame').forEach(frame => {
            const head = frame.querySelector('.NavHead');
            const content = frame.querySelector('.NavContent');
            if (!head || !content) return;

            // Inject the toggle anchor if not already present
            let toggle = frame.querySelector('.NavToggle');
            if (!toggle) {
                toggle = document.createElement('a');
                toggle.className = 'NavToggle';
                toggle.href = 'javascript:void(0)';
                toggle.textContent = '[afficher]';
                head.prepend(toggle);
            }

            content.style.display = 'none';

            toggle.addEventListener('click', () => {
                const isHidden = content.style.display === 'none';
                content.style.display = isHidden ? '' : 'none';
                toggle.textContent = isHidden ? '[masquer]' : '[afficher]';
            });
        });
    }
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
}());