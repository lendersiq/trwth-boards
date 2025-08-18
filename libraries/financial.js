// /libraries/financial.js
(function () {
  const reg = ((window.financial = window.financial || {}), (window.financial.functions = window.financial.functions || {}));

  // --- helpers (local to this file) ---
  function toNum(v) { const n = Number(v); return Number.isFinite(n) ? n : 0; }
  function todayInfo() {
    const now = new Date();
    const y = now.getFullYear();
    const isLeap = (y % 4 === 0 && y % 100 !== 0) || (y % 400 === 0);
    const daysInYear = isLeap ? 366 : 365;
    const start = new Date(y, 0, 0);
    const dayOfYear = Math.max(1, Math.floor((now - start) / (1000 * 60 * 60 * 24))); // clamp ≥1
    return { dayOfYear, daysInYear };
  }

  reg.ytdPerformance = {
    description: "Balance / Goal, annualized by day-of-year",
    paramNames: ["Balance", "Goal"],
    scope: "row",
    implementation(balance, goal) {
      const b = toNum(balance), g = toNum(goal);
      if (g <= 0) return 0;
      const { dayOfYear, daysInYear } = todayInfo();
      const perf = b / g;                     // current progress toward goal
      const annualized = (perf * (daysInYear / dayOfYear) * 100).toFixed(2);
      let result = annualized >= 100 ? `<span style="color: #66BB6A;">${Number(annualized).toLocaleString()}%</span>` :
        `<span style="color: #D32F2F;">${Number(annualized).toLocaleString()}%</span>`;
      return result;
    }
  };

  reg.ratio = {
    description: "simple ratio",
    paramNames: ["numerator", "denominator"],
    scope: "row",                
    implementation(numerator, denominator) {
      return numerator / denominator;
    }
  };
})();