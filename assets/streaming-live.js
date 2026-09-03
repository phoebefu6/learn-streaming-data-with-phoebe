/* streaming-live.js - the live event-stream simulator for learn-streaming-data-with-phoebe.
   One deterministic day of coffee-shop order events; every displayed number is COMPUTED
   from those events with the semantics the levers select. The generator is a teaching
   simulation; the aggregation math is real.
   Host markup: <div class="streamsim" data-stage="1..6"></div>
   Stages unlock levers cumulatively:
     1 batch view only   2 +stream toggle   3 +dedupe   4 +windowing
     5 +watermark & replay   6 +stream-everything anti-lever
*/
(function () {
  "use strict";

  /* ---------- deterministic generator ---------- */
  function mulberry32(a) {
    return function () {
      a |= 0; a = (a + 0x6D2B79F5) | 0;
      var t = Math.imul(a ^ (a >>> 15), 1 | a);
      t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }

  var NOW = 840; /* the dashboard moment: 14:00 */
  var HOUR_W = [1,1,0,0,0,1,2,4,7,8,7,6,8,9,7,6,5,5,4,3,3,2,2,1]; /* orders per hour weight */

  function genEvents() {
    var rng = mulberry32(20260903);
    var events = [], id = 8412;
    for (var h = 0; h < 24; h++) {
      var n = HOUR_W[h] * 2; /* ~2x weight = order count that hour */
      for (var i = 0; i < n; i++) {
        var t = h * 60 + Math.floor(rng() * 60);
        var amt = Math.round((8 + rng() * 52) * 100) / 100;
        var r = rng();
        var delay;
        if (r < 0.03)      delay = 360 + Math.floor(rng() * 540); /* very late: 6-15h */
        else if (r < 0.10) delay = 20 + Math.floor(rng() * 160);  /* late: 20-180 min */
        else               delay = Math.floor(rng() * 3);          /* on time */
        var ev = { id: id++, t: t, amt: amt, arr: t + delay, dup: false };
        events.push(ev);
        if (rng() < 0.12) { /* producer retry: same order arrives again */
          events.push({ id: ev.id, t: t, amt: amt, arr: ev.arr + 1 + Math.floor(rng() * 4), dup: true });
        }
      }
    }
    events.sort(function (a, b) { return a.arr - b.arr; });
    return events;
  }
  var EVENTS = genEvents();

  /* ---------- real aggregation under chosen semantics ---------- */
  function hourly(fill) { var a = []; for (var i = 0; i < 24; i++) a.push(fill || 0); return a; }

  function truthAt(now) {
    var t = hourly();
    EVENTS.forEach(function (e) { if (!e.dup && e.t <= now) t[Math.floor(e.t / 60)] += e.amt; });
    return t;
  }

  /* levers: {stream, dedupe, window, lateAllow(min), replay} */
  function computeView(lv) {
    var now = lv.replay ? 1439 : NOW;
    var disp = hourly(), seen = {}, dupCounted = 0, lateDropped = 0, corrections = 0;
    if (lv.stream) {
      EVENTS.forEach(function (e) {
        if (!lv.replay && e.arr > now) return; /* replay reprocesses the complete log */
        if (lv.dedupe) {
          if (seen[e.id]) { return; }
          seen[e.id] = true;
        } else if (e.dup) { dupCounted++; }
        if (lv.window) {
          var bucket = Math.floor(e.t / 60);
          var windowClose = (bucket + 1) * 60 + (lv.replay ? 1e9 : lv.lateAllow);
          if (e.arr > windowClose) { lateDropped++; corrections++; return; }
          disp[bucket] += e.amt;
        } else {
          disp[Math.floor(e.arr / 60)] += e.amt;
        }
      });
    }
    var truth = truthAt(now);
    var sumT = 0, err = 0, sumD = 0;
    for (var h = 0; h < 24; h++) { sumT += truth[h]; err += Math.abs(disp[h] - truth[h]); sumD += disp[h]; }
    var acc = lv.stream ? Math.max(0, 100 * (1 - err / sumT)) : null;
    var lagMin = lv.stream ? 0 : NOW; /* nightly batch covers through 00:00; at 14:00 that is 14h */
    return { disp: disp, truth: truth, acc: acc, lagMin: lagMin, now: now,
             sumT: sumT, sumD: sumD, dupCounted: dupCounted, lateDropped: lateDropped,
             corrections: corrections };
  }

  /* modelled (not measured) cost figures for the anti-lever */
  function costModel(lv) {
    if (lv.everything) return { cost: 1980, pipes: 13 };
    if (lv.stream)     return { cost: 340,  pipes: 2 };
    return { cost: 140, pipes: 1 };
  }

  /* ---------- passport (stamps written by app.js quiz code) ---------- */
  function stamps() {
    try { return Object.keys(JSON.parse(localStorage.getItem("lwp-passport:streaming-data") || "{}")).length; }
    catch (e) { return 0; }
  }

  /* ---------- rendering ---------- */
  function el(tag, cls, txt) {
    var n = document.createElement(tag);
    if (cls) n.className = cls;
    if (txt != null) n.textContent = txt;
    return n;
  }
  function money(x) { return "$" + x.toFixed(0); }
  function fmtLag(min) {
    if (min <= 0) return "seconds";
    var h = Math.floor(min / 60), m = min % 60;
    return h ? h + "h" + (m ? " " + m + "m" : "") : m + "m";
  }

  var LEVERS = [
    { key: "stream",   stage: 2, name: "Stream it",        sub: "consume events as they arrive instead of a nightly batch" },
    { key: "dedupe",   stage: 3, name: "Idempotent consumer", sub: "drop a retry when its order id was already counted" },
    { key: "window",   stage: 4, name: "Event-time windows",  sub: "bucket by when the order happened, not when it arrived" },
    { key: "lateAllow", stage: 5, name: "Watermark: allow 3h lateness", sub: "keep a closing window open for stragglers", isLate: true },
    { key: "replay",   stage: 5, name: "End-of-day replay",  sub: "reprocess the full log after midnight", gated: true },
    { key: "everything", stage: 6, name: "Stream all 13 tables", sub: "products, staff, rent, weather... everything real-time", anti: true }
  ];

  function build(host) {
    var stage = parseInt(host.getAttribute("data-stage") || "6", 10);
    var lv = { stream: false, dedupe: false, window: false, lateAllow: 0, replay: false, everything: false };

    var wk = el("div", "wk streamsim-wk");
    var head = el("div", "wk-head");
    head.appendChild(el("strong", null, "⚡ The 2pm dashboard"));
    head.appendChild(el("span", "st-badge st-measured", "measured"));
    var sub = el("span", "st-headsub", "one deterministic day of order events - every number below is computed live");
    head.appendChild(sub);
    wk.appendChild(head);

    var body = el("div", "wk-body");

    /* headline pair */
    var hl = el("div", "st-headline");
    var boxLag = el("div", "st-hbox");
    boxLag.appendChild(el("span", "st-hlab", "Freshness lag"));
    var vLag = el("span", "st-hval"); boxLag.appendChild(vLag);
    var boxAcc = el("div", "st-hbox");
    boxAcc.appendChild(el("span", "st-hlab", "Today-view accuracy"));
    var vAcc = el("span", "st-hval"); boxAcc.appendChild(vAcc);
    hl.appendChild(boxLag); hl.appendChild(boxAcc);
    body.appendChild(hl);

    /* levers */
    var lvWrap = el("div", "st-levers");
    var inputs = {};
    LEVERS.forEach(function (L) {
      if (L.stage > stage) return;
      var row = el("label", "st-lever" + (L.anti ? " st-anti" : ""));
      var cb = document.createElement("input");
      cb.type = "checkbox"; inputs[L.key] = cb;
      row.appendChild(cb);
      var tx = el("span", "st-ltext");
      tx.appendChild(el("b", null, (L.anti ? "⚠ " : "") + L.name));
      tx.appendChild(el("i", null, L.sub));
      row.appendChild(tx);
      if (L.gated && stamps() < 4) {
        cb.disabled = true;
        var lock = el("span", "st-lock", "🎫 needs 4 passport stamps");
        var bypass = el("a", "st-bypass", "unlock anyway");
        bypass.href = "#";
        bypass.addEventListener("click", function (e) { e.preventDefault(); cb.disabled = false; lock.remove(); bypass.remove(); });
        row.appendChild(lock); row.appendChild(bypass);
      }
      cb.addEventListener("change", update);
      lvWrap.appendChild(row);
    });
    body.appendChild(lvWrap);

    /* chart */
    var chartWrap = el("div", "st-chart");
    body.appendChild(chartWrap);

    /* counters */
    var counters = el("div", "st-counters");
    body.appendChild(counters);

    /* cost strip (stage 6) */
    var costRow = null;
    if (stage >= 6) {
      costRow = el("div", "st-cost");
      body.appendChild(costRow);
    }

    var verdict = el("p", "st-verdict");
    body.appendChild(verdict);

    wk.appendChild(body);
    var foot = el("div", "wk-foot",
      "The order generator is a teaching simulation with a fixed seed. The aggregation - buckets, dedupe, windows, watermarks - is genuinely computed from those events. Cost figures are modelled, and carry their own badge.");
    wk.appendChild(foot);
    host.appendChild(wk);

    function update() {
      LEVERS.forEach(function (L) {
        if (!inputs[L.key]) return;
        if (L.isLate) lv.lateAllow = inputs[L.key].checked ? 180 : 0;
        else lv[L.key] = inputs[L.key].checked;
      });
      /* dependency: everything past "stream" needs the stream on */
      ["dedupe", "window"].forEach(function (k) { if (inputs[k]) inputs[k].disabled = !lv.stream; if (!lv.stream) { lv[k] = false; if (inputs[k]) inputs[k].checked = false; } });
      if (inputs.lateAllow) { inputs.lateAllow.disabled = !lv.window; if (!lv.window) { lv.lateAllow = 0; inputs.lateAllow.checked = false; } }
      if (inputs.replay && !inputs.replay.disabled) { if (!lv.window) { lv.replay = false; inputs.replay.checked = false; } inputs.replay.disabled = inputs.replay.disabled || !lv.window; }

      var v = computeView(lv);
      vLag.textContent = fmtLag(v.lagMin);
      vLag.className = "st-hval " + (v.lagMin > 60 ? "bad" : "good");
      if (v.acc == null) {
        vAcc.textContent = "showing yesterday";
        vAcc.className = "st-hval stale";
      } else {
        vAcc.textContent = v.acc.toFixed(1) + "%";
        vAcc.className = "st-hval " + (v.acc >= 99.95 ? "good" : v.acc >= 98 ? "mid" : "bad");
      }
      drawChart(chartWrap, v);
      counters.innerHTML = "";
      counter(counters, "Truth so far", money(v.sumT));
      counter(counters, "Dashboard shows", money(v.sumD));
      counter(counters, "Retries double-counted", String(v.dupCounted), v.dupCounted > 0);
      counter(counters, "Late events dropped", String(v.lateDropped), v.lateDropped > 0);
      if (costRow) {
        var c = costModel(lv);
        costRow.innerHTML = "";
        costRow.appendChild(el("span", "st-badge st-modelled", "modelled"));
        costRow.appendChild(el("span", "st-costtxt", "Infra cost: " + money(c.cost) + "/mo · pipelines to keep alive: " + c.pipes));
        costRow.appendChild(el("span", "st-costbar"));
        costRow.lastChild.style.width = Math.min(100, c.cost / 20) + "%";
      }
      verdict.textContent = verdictText(lv, v);
    }

    function counter(parent, lab, val, warn) {
      var c = el("div", "st-counter" + (warn ? " warn" : ""));
      c.appendChild(el("b", null, val));
      c.appendChild(el("span", null, lab));
      parent.appendChild(c);
    }

    update();
  }

  function verdictText(lv, v) {
    if (lv.everything) return "Nothing on the dashboard improved - the accuracy and lag are identical. You bought 11 more always-on pipelines for tables that change once a month.";
    if (lv.replay) return "Replay reprocessed the whole log with event-time windows: the day converges to 100%. Streaming answers now; batch-style replay makes the history exact. Most real platforms do both.";
    if (lv.window && lv.lateAllow > 0) return "Windows now wait 3h for stragglers before freezing. Only the truly-very-late events are still missing - and they are counted, not invisible.";
    if (lv.window) return "Buckets are by event time now, so the hourly shape is honest - but a window that closes at +0 drops every late arrival. Look at the dropped counter.";
    if (lv.dedupe) return "Retries no longer double-count. The total is close - but late orders still land in the wrong hour, because buckets follow arrival time.";
    if (lv.stream) return "Lag collapsed from 14 hours to seconds - and the number went wrong. Producer retries double-count, late events land in the wrong bucket. Fresh but sloppy is not yet a win.";
    return "The nightly batch is not wrong - it is old. At 2pm the dashboard still shows midnight. Everything it does show is exactly right.";
  }

  /* SVG hourly chart: truth outline vs displayed fill */
  function drawChart(wrap, v) {
    var W = 840, H = 190, pad = 30, bw = Math.floor((W - pad * 2) / 24) - 4;
    var max = 0;
    for (var h = 0; h < 24; h++) max = Math.max(max, v.truth[h], v.disp[h]);
    if (max === 0) max = 1;
    var s = '<svg viewBox="0 0 ' + W + " " + (H + 34) + '" role="img" aria-label="Hourly revenue: truth vs dashboard">';
    for (h = 0; h < 24; h++) {
      var x = pad + h * (bw + 4);
      var th = Math.round(v.truth[h] / max * (H - 20));
      var dh = Math.round(v.disp[h] / max * (H - 20));
      s += '<rect x="' + x + '" y="' + (H - th) + '" width="' + bw + '" height="' + th + '" fill="none" stroke="#A78BFA" stroke-width="1.5" stroke-dasharray="3 2"/>';
      s += '<rect x="' + x + '" y="' + (H - dh) + '" width="' + bw + '" height="' + dh + '" fill="#7C3AED" opacity="0.75"/>';
      if (h % 4 === 0) s += '<text x="' + (x + bw / 2) + '" y="' + (H + 14) + '" font-size="10" fill="#6E6787" text-anchor="middle">' + (h < 10 ? "0" : "") + h + ":00</text>";
    }
    s += '<rect x="' + pad + '" y="' + (H + 20) + '" width="10" height="10" fill="#7C3AED" opacity="0.75"/><text x="' + (pad + 16) + '" y="' + (H + 29) + '" font-size="10" fill="#6E6787">dashboard shows</text>';
    s += '<rect x="' + (pad + 130) + '" y="' + (H + 20) + '" width="10" height="10" fill="none" stroke="#A78BFA" stroke-width="1.5" stroke-dasharray="3 2"/><text x="' + (pad + 146) + '" y="' + (H + 29) + '" font-size="10" fill="#6E6787">what really happened (event time)</text>';
    s += "</svg>";
    wrap.innerHTML = s;
  }

  document.querySelectorAll(".streamsim").forEach(build);
})();
