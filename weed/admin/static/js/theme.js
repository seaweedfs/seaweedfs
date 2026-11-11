// Applies the saved color theme (auto, light or dark) before the page paints.
(function () {
    var storageKey = 'admin-theme';
    var choices = ['auto', 'light', 'dark'];
    var media = window.matchMedia ? window.matchMedia('(prefers-color-scheme: dark)') : null;
    var root = document.documentElement;

    function savedChoice() {
        try {
            var value = localStorage.getItem(storageKey);
            return choices.indexOf(value) >= 0 ? value : 'auto';
        } catch (e) {
            return 'auto';
        }
    }

    function apply(choice) {
        var dark = choice === 'dark' || (choice === 'auto' && media && media.matches);
        root.setAttribute('data-bs-theme', dark ? 'dark' : 'light');
        root.setAttribute('data-theme-choice', choice);
    }

    var current = savedChoice();
    apply(current);

    if (media && media.addEventListener) {
        media.addEventListener('change', function () {
            apply(current);
        });
    }

    document.addEventListener('click', function (event) {
        var toggle = event.target.closest && event.target.closest('[data-theme-toggle]');
        if (!toggle) {
            return;
        }
        current = choices[(choices.indexOf(current) + 1) % choices.length];
        try {
            localStorage.setItem(storageKey, current);
        } catch (e) {}
        apply(current);
    });
})();
