/**
 * Navegación global coherente (chips + fila Labs).
 * Monta en #global-site-nav. Expone PredMarketArbNav.refresh() tras cambiar rutas o
 * window.__PM_DEFAULT_ML_SLUG__ (inicio: slug ML por defecto del servidor).
 */
(function () {
  var DEFAULT_ML = "crypto_5m_lgbm";

  function normalizedPath() {
    var p = window.location.pathname.replace(/\/+$/, "") || "/";
    return p;
  }

  function mlSlugFromPath() {
    var m = window.location.pathname.match(/^\/ml\/model\/([^/]+)/);
    return m ? decodeURIComponent(m[1]) : null;
  }

  function mlHref() {
    var override =
      typeof window !== "undefined" && window.__PM_DEFAULT_ML_SLUG__
        ? String(window.__PM_DEFAULT_ML_SLUG__).trim()
        : "";
    if (override) {
      return "/ml/model/" + encodeURIComponent(override);
    }
    var s = mlSlugFromPath();
    return "/ml/model/" + encodeURIComponent(s || DEFAULT_ML);
  }

  function navState() {
    var p = normalizedPath();
    var arbStrat = p.indexOf("/arb/strategy/") === 0;
    var slugPart = "";
    if (arbStrat) {
      var rest = p.slice("/arb/strategy/".length);
      slugPart = rest.split("/")[0] || "";
    }
    return {
      home: p === "/",
      catalog: p === "/ml",
      monitor: p.indexOf("/ml/model/") === 0,
      arb: p === "/arb",
      strategy: arbStrat,
      sixcycle: slugPart === "crypto_5m_sixcycle",
      latencySports: slugPart === "latency_arb_sports",
    };
  }

  function chip(text, href, active, opts) {
    opts = opts || {};
    var base =
      "inline-block px-3 py-1.5 rounded-lg text-sm focus:outline-none focus-visible:ring-2 focus-visible:ring-indigo-400/70";
    var idAttr = opts.id ? ' id="' + opts.id + '"' : "";
    if (active) {
      return (
        "<span" +
        idAttr +
        ' class="' +
        base +
        ' bg-indigo-900/50 text-indigo-200 ring-1 ring-indigo-700/50">' +
        text +
        "</span>"
      );
    }
    return (
      "<a href=\"" +
      href +
      '"' +
      idAttr +
      ' class="' +
      base +
      ' border border-slate-600 text-slate-300 hover:bg-slate-800">' +
      text +
      "</a>"
    );
  }

  function render() {
    var el = document.getElementById("global-site-nav");
    if (!el) return;

    var s = navState();
    var mlLinkHref = mlHref();

    var monitorHtml;
    if (s.monitor) {
      monitorHtml = chip("Monitor ML", "#", true, { id: "navDetailBadge" });
    } else {
      monitorHtml = chip("Monitor ML (5m)", mlLinkHref, false, { id: "linkMlMonitor" });
    }

    var motorActive = s.arb && !s.strategy;
    var row1 = [
      '<a href="#main-content" class="fixed left-3 top-0 z-[100] -translate-y-full rounded-lg bg-indigo-600 px-3 py-2 text-sm text-white shadow transition focus:translate-y-3 focus:outline-none focus:ring-2 focus:ring-white">Saltar al contenido principal</a>',
      '<div class="flex flex-wrap gap-2 items-center">',
      '<span class="text-slate-500 text-sm">Ir a:</span>',
      chip("Inicio", "/", s.home),
      chip("Catálogo ML", "/ml", s.catalog),
      monitorHtml,
      chip("Motor Arb (CLOB)", "/arb", motorActive),
      "</div>",
    ].join("");

    var sixActive = s.sixcycle;
    var latActive = s.latencySports;
    var sixEl = sixActive
      ? '<span class="inline-block px-3 py-1.5 rounded-lg text-sm bg-indigo-900/50 text-indigo-200 ring-1 ring-indigo-700/50">Sixcycle · vivo</span>'
      : '<a href="/arb/strategy/crypto_5m_sixcycle?view=live" class="inline-block px-3 py-1.5 rounded-lg text-sm border border-slate-600 text-slate-300 hover:bg-slate-800 focus:outline-none focus-visible:ring-2 focus-visible:ring-indigo-400/70">Sixcycle · vivo</a>';
    var latEl = latActive
      ? '<span class="inline-block px-3 py-1.5 rounded-lg text-sm bg-indigo-900/50 text-indigo-200 ring-1 ring-indigo-700/50">Latency sports</span>'
      : '<a href="/arb/strategy/latency_arb_sports" class="inline-block px-3 py-1.5 rounded-lg text-sm border border-slate-600 text-slate-300 hover:bg-slate-800 focus:outline-none focus-visible:ring-2 focus-visible:ring-indigo-400/70">Latency sports</a>';

    var row2 =
      '<div class="flex flex-wrap gap-2 items-center mt-2 pt-2 border-t border-slate-700/40">' +
      '<span class="text-slate-500 text-xs shrink-0">Labs:</span>' +
      sixEl +
      latEl +
      "</div>";

    el.innerHTML = row1 + row2;
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", render);
  } else {
    render();
  }

  var _authBlocked = false;
  var _authListeners = [];
  var _authBannerId = "pm-auth-expired-banner";
  var _authMsg =
    "Sesion expirada o sin autenticar. Recarga la pagina y vuelve a iniciar sesion para reanudar polling/SSE.";

  function authBanner() {
    var el = document.getElementById(_authBannerId);
    if (el) return el;
    el = document.createElement("div");
    el.id = _authBannerId;
    el.className =
      "fixed bottom-3 right-3 z-[120] max-w-md rounded-lg border border-rose-700/80 bg-rose-950/95 px-3 py-2 text-xs text-rose-100 shadow-xl";
    el.style.display = "none";
    document.body.appendChild(el);
    return el;
  }

  function emitAuthState() {
    _authListeners.forEach(function (fn) {
      try {
        fn(_authBlocked);
      } catch (err) {}
    });
  }

  function setAuthBlocked(blocked, message) {
    if (blocked === _authBlocked && (!blocked || !message)) return;
    _authBlocked = blocked;
    var el = authBanner();
    if (_authBlocked) {
      el.textContent = String(message || _authMsg);
      el.style.display = "block";
    } else {
      el.style.display = "none";
    }
    emitAuthState();
  }

  async function authFetch(input, init) {
    var res = await window.fetch(input, init);
    if (res.status === 401) {
      setAuthBlocked(true, _authMsg);
    } else if (res.ok && _authBlocked) {
      setAuthBlocked(false);
    }
    return res;
  }

  window.PMAuth = {
    fetch: authFetch,
    isBlocked: function () {
      return _authBlocked;
    },
    markUnauthorized: function (message) {
      setAuthBlocked(true, message || _authMsg);
    },
    clear: function () {
      setAuthBlocked(false);
    },
    onChange: function (fn) {
      if (typeof fn !== "function") return function () {};
      _authListeners.push(fn);
      return function () {
        _authListeners = _authListeners.filter(function (x) {
          return x !== fn;
        });
      };
    },
  };

  window.PredMarketArbNav = { refresh: render };
})();
