(() => {
  'use strict';
  const reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  document.documentElement.classList.toggle('gh-reduce-motion', reduceMotion);
  if (reduceMotion) return;

  const once = (element, className, duration = 850) => {
    if (!element) return;
    element.classList.remove(className);
    void element.offsetWidth;
    element.classList.add(className);
    window.setTimeout(() => element.classList.remove(className), duration);
  };

  const pageCards = document.querySelectorAll([
    '.gh-dashboard .gh-kpi', '.gh-dashboard .gh-panel', '.gh-dashboard .gh-mini-grid article',
    '.matchday-card', '[data-match-card]', '.saved-ticket-card'
  ].join(','));
  pageCards.forEach((card, index) => {
    card.classList.add('gh-motion-card');
    card.style.setProperty('--gh-enter-delay', `${Math.min(index, 10) * 45}ms`);
  });

  document.querySelectorAll('.gh-stack i, .top-rule-progress > *, .gh-league-list i b').forEach((bar) => {
    bar.classList.add('gh-motion-bar');
  });

  const parseAnimatedNumber = (text) => {
    const match = String(text).match(/-?[\d.]+(?:,\d+)?/);
    if (!match) return null;
    const normalized = match[0].replace(/\./g, '').replace(',', '.');
    const value = Number(normalized);
    return Number.isFinite(value) ? {value, raw: match[0]} : null;
  };
  document.querySelectorAll('.gh-dashboard .gh-kpi-copy strong, .gh-dashboard .gh-mini-grid strong, .gh-dashboard .gh-donut strong').forEach((node) => {
    const parsed = parseAnimatedNumber(node.textContent);
    if (!parsed || /:\d{2}/.test(node.textContent)) return;
    const original = node.textContent;
    const start = performance.now();
    const duration = 720;
    const decimals = parsed.raw.includes(',') ? parsed.raw.split(',')[1].length : 0;
    const draw = (now) => {
      const progress = Math.min(1, (now - start) / duration);
      const eased = 1 - Math.pow(1 - progress, 3);
      const shown = (parsed.value * eased).toFixed(decimals).replace('.', ',');
      node.textContent = original.replace(parsed.raw, shown);
      if (progress < 1) requestAnimationFrame(draw);
    };
    requestAnimationFrame(draw);
  });

  const observer = new MutationObserver((mutations) => {
    mutations.forEach((mutation) => {
      const target = mutation.target.nodeType === Node.TEXT_NODE ? mutation.target.parentElement : mutation.target;
      if (!(target instanceof Element)) return;
      if (target.closest('.live-game-card, [data-live-game], .matchday-card') && mutation.type === 'characterData') {
        once(target.closest('td, b, strong, span') || target, 'gh-live-updated');
      }
      if (target.matches('.status-badge, .gh-status') || target.closest('.status-badge, .gh-status')) {
        const badge = target.matches('.status-badge, .gh-status') ? target : target.closest('.status-badge, .gh-status');
        if (badge.matches('.status-green, .green, .status-red, .red')) once(badge, 'gh-status-changed', 900);
      }
      mutation.addedNodes.forEach((node) => {
        if (!(node instanceof Element)) return;
        if (node.matches('.alert') || node.querySelector('.alert')) once(node.matches('.alert') ? node : node.querySelector('.alert'), 'gh-alert-arrived', 1100);
        node.querySelectorAll?.('.matchday-ticket-bet365-odd, [class*="odd"]').forEach((odd) => once(odd, 'gh-odd-loaded', 950));
      });
    });
  });
  observer.observe(document.body, {subtree: true, childList: true, characterData: true});

  document.addEventListener('click', (event) => {
    const button = event.target.closest('button, .btn, a.nav-link');
    if (button) once(button, 'gh-button-pressed', 320);
    const favorite = event.target.closest('[data-live-league-favorite], [data-favorite], .live-league-chip, .matchday-league-star');
    if (favorite) once(favorite, 'gh-favorite-changed', 600);
    const tab = event.target.closest('[data-settings-tab], [data-bs-toggle="tab"], [data-filter-picker] button');
    if (tab) {
      const panel = document.querySelector(`[data-settings-panel="${tab.dataset.settingsTab || ''}"]:not([hidden])`);
      if (panel) once(panel, 'gh-panel-switched', 420);
    }
  });

  document.querySelectorAll('.alert').forEach((alert, index) => {
    alert.style.setProperty('--gh-alert-delay', `${index * 70}ms`);
    alert.classList.add('gh-alert-entry');
  });

  const celebration = document.querySelector('[data-green-celebration]');
  if (celebration) {
    const storageKey = `greenhunter-green-seen:${celebration.dataset.userId}`;
    const ticketId = celebration.dataset.ticketId;
    let seen = '';
    try { seen = localStorage.getItem(storageKey) || ''; } catch (_) {}
    if (seen !== ticketId) {
      celebration.hidden = false;
      requestAnimationFrame(() => celebration.classList.add('is-visible'));
      try { localStorage.setItem(storageKey, ticketId); } catch (_) {}
      window.setTimeout(() => celebration.classList.remove('is-visible'), 9000);
    }
    celebration.querySelector('[data-green-celebration-close]')?.addEventListener('click', () => celebration.classList.remove('is-visible'));
  }
})();
