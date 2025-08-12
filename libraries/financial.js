(function () {
  const reg = ((window.financial = window.financial || {}), (window.financial.functions = window.financial.functions || {}));
  function toNum(v){ const n = Number(v); return Number.isFinite(n) ? n : 0; }
  reg.ytdPerformance = {
    description: "Balance / Goal → percent",
    paramNames: ["Balance","Goal"],
    implementation: function (balance, goal) {
      const b = toNum(balance), g = toNum(goal);
      if (g <= 0) return 0;
      
      // Get current day of the year (1 to 365 or 366)
      const today = new Date();
      const year = today.getFullYear();
      const isLeapYear = (year % 4 === 0 && year % 100 !== 0) || (year % 400 === 0);
      const daysInYear = isLeapYear ? 366 : 365;
      
      const startOfYear = new Date(year, 0, 0);
      const diff = today - startOfYear;
      const oneDay = 1000 * 60 * 60 * 24;
      const dayOfYear = Math.floor(diff / oneDay);
      
      // Annualize the performance: (current progress / days so far) * days in year
      const performance = b / g;
      const annualized = (performance / dayOfYear) * daysInYear;
      
      return annualized;
    }
  };
})();