(function () {
  const reg = ((window.financial = window.financial || {}), (window.financial.functions = window.financial.functions || {}));
  function toNum(v){ const n = Number(v); return Number.isFinite(n) ? n : 0; }
  reg.ytdPerformance = {
    description: "Balance / Goal → percent",
    paramNames: ["Average_Balance","Commercial_Goal"],
    implementation: function (balance, goal) {
      const b = toNum(balance), g = toNum(goal);
      if (g <= 0) return 0;
      return b / g;
    }
  };
})();
