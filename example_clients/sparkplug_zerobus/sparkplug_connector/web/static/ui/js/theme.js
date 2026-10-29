// Applies the saved light/dark choice before the page paints (loaded without defer). Default: follow the system.
(function () {
  'use strict';
  try {
    const theme = localStorage.getItem('connector.theme');
    if (theme === 'light' || theme === 'dark') {
      document.documentElement.setAttribute('data-theme', theme);
    }
  } catch (e) {
    // Storage unavailable (private window, blocked site data): keep the system theme.
  }
})();
