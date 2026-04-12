/* Generated from _static/ts/benchmark_charts.ts. */
(() => {
  const data = window.PYVERSUS_BENCHMARK_DATA;
  if (!data || !Array.isArray(data.rows) || !Array.isArray(data.series)) {
    return;
  }

  const themePalettes = {
    light: {
      axis: "#1f2937",
      grid: "rgba(31, 41, 55, 0.1)",
      label: "#1f2937",
      tooltipBg: "rgba(15, 23, 42, 0.94)",
      tooltipText: "#f8fafc",
      seriesColors: {
        py_pandas_dataframe: "#2f3e9e",
        py_parquet_scan: "#d98200",
      },
    },
    dark: {
      axis: "#f8fafc",
      grid: "rgba(248, 250, 252, 0.18)",
      label: "#f1f5f9",
      tooltipBg: "rgba(15, 23, 42, 0.94)",
      tooltipText: "#f8fafc",
      seriesColors: {
        py_pandas_dataframe: "#7c8dff",
        py_parquet_scan: "#ffc44d",
      },
    },
  };

  const resolveTheme = () => {
    const root = document.documentElement;
    const mode = root.getAttribute("data-mode");
    if (mode === "light" || mode === "dark") {
      return mode;
    }
    const theme = root.getAttribute("data-theme");
    if (theme === "light" || theme === "dark") {
      return theme;
    }
    const prefersDark = window.matchMedia?.("(prefers-color-scheme: dark)").matches ?? false;
    return prefersDark ? "dark" : "light";
  };

  const chartInstances = [];

  const applyTheme = (theme) => {
    const palette = themePalettes[theme];
    chartInstances.forEach((chart) => chart.setPalette(palette));
    chartInstances.forEach((chart) => chart.scheduleRender());
  };

  let themeQueued = false;
  const queueThemeRefresh = () => {
    if (themeQueued) {
      return;
    }
    themeQueued = true;
    window.requestAnimationFrame(() => {
      themeQueued = false;
      applyTheme(resolveTheme());
    });
  };

  const formatRowsShort = (value) => {
    const numericValue =
      typeof value === "number" ? value : Number.parseFloat(value.replace(/,/g, ""));
    if (!Number.isFinite(numericValue)) {
      return String(value);
    }
    if (numericValue >= 1_000_000) {
      const num = numericValue / 1_000_000;
      return `${num % 1 === 0 ? num.toFixed(0) : num.toFixed(1)}M`;
    }
    if (numericValue >= 1_000) {
      const num = numericValue / 1_000;
      return `${num % 1 === 0 ? num.toFixed(0) : num.toFixed(1)}k`;
    }
    return `${numericValue}`;
  };

  const formatTick = (value, step) => {
    const decimals = step < 1 ? Math.ceil(Math.abs(Math.log10(step))) : 0;
    return value.toLocaleString(undefined, {
      maximumFractionDigits: decimals,
      minimumFractionDigits: decimals,
    });
  };

  const formatTimeTick = (value, step) => {
    if (value === 0) {
      return "0";
    }
    return `${formatTick(value, step)}s`;
  };

  const formatTimeTooltip = (value) => {
    if (value >= 1) {
      return `${value.toFixed(1)}s`;
    }
    return `${value.toFixed(3)}s`;
  };

  const formatMemoryTick = (value, step) => {
    if (value === 0) {
      return "0";
    }
    if (value >= 1000) {
      const gbValue = value / 1000;
      const gbStep = step / 1000;
      const decimals = gbStep < 1 ? Math.ceil(Math.abs(Math.log10(gbStep))) : gbStep % 1 !== 0 ? 1 : 0;
      return `${gbValue.toLocaleString(undefined, {
        maximumFractionDigits: decimals,
        minimumFractionDigits: decimals,
      })}GB`;
    }
    const decimals = step < 1 ? Math.ceil(Math.abs(Math.log10(step))) : step % 1 !== 0 ? 1 : 0;
    return `${value.toLocaleString(undefined, {
      maximumFractionDigits: decimals,
      minimumFractionDigits: decimals,
    })}MB`;
  };

  const formatMemoryTooltip = (value) => {
    if (value >= 1000) {
      const gbValue = value / 1000;
      return `${gbValue.toFixed(1)}GB`;
    }
    return `${value.toFixed(1)}MB`;
  };

  const niceTicks = (maxVal, target = 6) => {
    if (maxVal <= 0) {
      return { ticks: [0, 1], max: 1, step: 1 };
    }
    const rawStep = maxVal / Math.max(target - 1, 1);
    const magnitude = 10 ** Math.floor(Math.log10(rawStep));
    const residual = rawStep / magnitude;
    let step = magnitude;
    if (residual <= 1) {
      step = 1 * magnitude;
    } else if (residual <= 2) {
      step = 2 * magnitude;
    } else if (residual <= 5) {
      step = 5 * magnitude;
    } else {
      step = 10 * magnitude;
    }
    const max = Math.ceil(maxVal / step) * step;
    const ticks = [];
    for (let val = 0; val <= max + step * 0.5; val += step) {
      ticks.push(val);
    }
    return { ticks, max, step };
  };

  const buildChart = (container) => {
    const kind = container.dataset.benchmarkChart;
    if (kind !== "time" && kind !== "memory") {
      return null;
    }
    const title = container.dataset.title ?? "";
    const yLabel = container.dataset.yLabel ?? "";
    const parsedHeight = Number.parseInt(container.dataset.height ?? "360", 10);
    const height = Number.isFinite(parsedHeight) ? parsedHeight : 360;

    container.innerHTML = "";
    container.classList.add("chart-mounted");

    const header = document.createElement("div");
    header.className = "chart-header";

    const titleEl = document.createElement("div");
    titleEl.className = "chart-title";
    titleEl.textContent = title;

    const subtitleEl = document.createElement("div");
    subtitleEl.className = "chart-subtitle";
    subtitleEl.textContent = yLabel;

    header.append(titleEl, subtitleEl);

    const body = document.createElement("div");
    body.className = "chart-body";

    const canvas = document.createElement("canvas");
    canvas.className = "chart-canvas";
    canvas.height = height;
    body.append(canvas);

    const tooltip = document.createElement("div");
    tooltip.className = "chart-tooltip";
    tooltip.setAttribute("role", "status");
    tooltip.setAttribute("aria-live", "polite");
    body.append(tooltip);

    const legend = document.createElement("div");
    legend.className = "chart-legend";

    container.append(header, body, legend);

    const rows = data.rows;
    const baseSeries = data.series.map((item) => ({
      id: item.id,
      label: item.label,
      values: kind === "time" ? item.time : item.memory,
      fallbackColor: item.color ?? "#3b82f6",
    }));

    const legendSwatches = [];
    baseSeries.forEach((item) => {
      const legendItem = document.createElement("div");
      legendItem.className = "chart-legend-item";
      const swatch = document.createElement("span");
      swatch.className = "chart-legend-swatch";
      const label = document.createElement("span");
      label.textContent = item.label;
      legendItem.append(swatch, label);
      legend.append(legendItem);
      legendSwatches.push({ swatch, id: item.id, fallback: item.fallbackColor });
    });

    let activePoint = null;
    let points = [];

    let currentPalette = themePalettes[resolveTheme()];

    const updateLegendColors = (palette) => {
      legendSwatches.forEach((entry) => {
        const color = palette.seriesColors[entry.id] ?? entry.fallback;
        entry.swatch.style.background = color;
      });
    };

    const applyPalette = (palette) => {
      currentPalette = palette;
      container.style.setProperty("--benchmark-axis", palette.axis);
      container.style.setProperty("--benchmark-grid", palette.grid);
      container.style.setProperty("--benchmark-label", palette.label);
      container.style.setProperty("--benchmark-tooltip-bg", palette.tooltipBg);
      container.style.setProperty("--benchmark-tooltip-text", palette.tooltipText);
      baseSeries.forEach((item) => {
        const color = palette.seriesColors[item.id] ?? item.fallbackColor;
        container.style.setProperty(`--benchmark-series-${item.id}`, color);
      });
      updateLegendColors(palette);
    };

    const resolveSeriesColors = () =>
      baseSeries.map((item) => ({
        ...item,
        color: currentPalette.seriesColors[item.id] ?? item.fallbackColor,
      }));

    const render = () => {
      const width = body.clientWidth;
      if (!width) {
        return;
      }
      const dpr = window.devicePixelRatio || 1;
      canvas.width = Math.max(1, Math.floor(width * dpr));
      canvas.height = Math.max(1, Math.floor(height * dpr));
      canvas.style.width = `${width}px`;
      canvas.style.height = `${height}px`;

      const ctx = canvas.getContext("2d");
      if (!ctx) {
        return;
      }
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, width, height);

      const fontFamily = getComputedStyle(container).fontFamily || "system-ui";
      const axisColor = currentPalette.axis;
      const gridColor = currentPalette.grid;
      const labelColor = currentPalette.label;

      const margin = {
        left: 98,
        right: 18,
        top: 10,
        bottom: 48,
      };
      const plot = {
        left: margin.left,
        right: width - margin.right,
        top: margin.top,
        bottom: height - margin.bottom,
      };
      const plotWidth = plot.right - plot.left;
      const plotHeight = plot.bottom - plot.top;

      const xPositions = rows.map((_, index) => {
        if (rows.length === 1) {
          return plot.left + plotWidth / 2;
        }
        return plot.left + (index * plotWidth) / (rows.length - 1);
      });

      const series = resolveSeriesColors();
      const allValues = [];
      series.forEach((item) => {
        item.values.forEach((value) => allValues.push(value));
      });

      const maxVal = Math.max(...allValues) * 1.06;
      const { ticks, max, step } = niceTicks(maxVal);

      const yScale = (value) => plot.bottom - (value / max) * plotHeight;

      ctx.lineWidth = 1;
      ctx.strokeStyle = gridColor;
      ticks.forEach((tick) => {
        const y = yScale(tick);
        ctx.beginPath();
        ctx.moveTo(plot.left, y);
        ctx.lineTo(plot.right, y);
        ctx.stroke();
      });

      ctx.strokeStyle = axisColor;
      ctx.lineWidth = 1.8;
      ctx.beginPath();
      ctx.moveTo(plot.left, plot.top);
      ctx.lineTo(plot.left, plot.bottom);
      ctx.lineTo(plot.right, plot.bottom);
      ctx.stroke();

      const tickFontSize = 13;
      const axisLabelFontSize = 16;

      ctx.fillStyle = labelColor;
      ctx.font = `${tickFontSize}px ${fontFamily}`;
      ctx.textAlign = "right";
      ctx.textBaseline = "middle";
      ticks.forEach((tick) => {
        const y = yScale(tick);
        const tickLabel =
          kind === "memory"
            ? formatMemoryTick(tick, step)
            : kind === "time"
              ? formatTimeTick(tick, step)
              : formatTick(tick, step);
        ctx.fillText(tickLabel, plot.left - 8, y);
      });

      ctx.textAlign = "center";
      ctx.textBaseline = "top";
      rows.forEach((row, index) => {
        const x = xPositions[index];
        ctx.fillText(formatRowsShort(row), x, plot.bottom + 8);
      });

      ctx.save();
      ctx.font = `${axisLabelFontSize}px ${fontFamily}`;
      ctx.translate(20, plot.top + plotHeight / 2);
      ctx.rotate(-Math.PI / 2);
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillText(yLabel, 0, 0);
      ctx.restore();

      ctx.textAlign = "center";
      ctx.textBaseline = "top";
      ctx.font = `${axisLabelFontSize}px ${fontFamily}`;
      ctx.fillText("Rows", plot.left + plotWidth / 2, height - 18);

      points = [];
      series.forEach((item, sIndex) => {
        ctx.strokeStyle = item.color;
        ctx.lineWidth = 2.2;
        ctx.beginPath();
        item.values.forEach((value, index) => {
          const x = xPositions[index];
          const y = yScale(value);
          if (index === 0) {
            ctx.moveTo(x, y);
          } else {
            ctx.lineTo(x, y);
          }
          points.push({
            x,
            y,
            series: item,
            row: rows[index],
            value,
            index,
            seriesIndex: sIndex,
          });
        });
        ctx.stroke();
      });

      points.forEach((point) => {
        const isActive =
          activePoint &&
          activePoint.seriesIndex === point.seriesIndex &&
          activePoint.index === point.index;
        ctx.beginPath();
        ctx.fillStyle = point.series.color;
        ctx.arc(point.x, point.y, isActive ? 5 : 3.5, 0, Math.PI * 2);
        ctx.fill();
        if (isActive) {
          ctx.strokeStyle = "#ffffff";
          ctx.lineWidth = 1.2;
          ctx.stroke();
        }
      });
    };

    const showTooltip = (point) => {
      if (!point) {
        tooltip.classList.remove("is-visible");
        return;
      }
      const label = point.series.label;
      const rowsText = formatRowsShort(point.row);
      const valueText =
        kind === "time" ? formatTimeTooltip(point.value) : formatMemoryTooltip(point.value);
      tooltip.innerHTML = `<div class="tooltip-title">${label}</div><div class="tooltip-body">${rowsText} rows - ${valueText}</div>`;
      tooltip.classList.add("is-visible");

      const bodyRect = body.getBoundingClientRect();
      const tooltipRect = tooltip.getBoundingClientRect();

      let left = point.x;
      let top = point.y - 12;
      const minLeft = tooltipRect.width / 2 + 6;
      const maxLeft = bodyRect.width - tooltipRect.width / 2 - 6;
      if (left < minLeft) {
        left = minLeft;
      } else if (left > maxLeft) {
        left = maxLeft;
      }
      if (top < tooltipRect.height + 10) {
        top = point.y + 16;
      }
      tooltip.style.left = `${left}px`;
      tooltip.style.top = `${top}px`;
    };

    const pickPoint = (event) => {
      const rect = canvas.getBoundingClientRect();
      const x = event.clientX - rect.left;
      const y = event.clientY - rect.top;
      let closest = null;
      let minDist = Infinity;
      points.forEach((point) => {
        const dx = point.x - x;
        const dy = point.y - y;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < 10 && dist < minDist) {
          closest = point;
          minDist = dist;
        }
      });
      if (
        closest &&
        (!activePoint ||
          activePoint.seriesIndex !== closest.seriesIndex ||
          activePoint.index !== closest.index)
      ) {
        activePoint = closest;
        render();
        showTooltip(closest);
      } else if (!closest && activePoint) {
        activePoint = null;
        render();
        showTooltip(null);
      } else if (closest) {
        showTooltip(closest);
      }
    };

    canvas.addEventListener("mousemove", pickPoint);
    canvas.addEventListener("mouseleave", () => {
      activePoint = null;
      render();
      showTooltip(null);
    });

    const resizeObserver = new ResizeObserver(() => {
      scheduleRender();
    });
    resizeObserver.observe(container);

    let rafId = 0;
    const scheduleRender = () => {
      if (rafId) {
        return;
      }
      rafId = window.requestAnimationFrame(() => {
        rafId = 0;
        render();
      });
    };

    applyPalette(currentPalette);
    render();

    return {
      scheduleRender,
      setPalette: applyPalette,
    };
  };

  document.querySelectorAll(".benchmark-chart[data-benchmark-chart]").forEach((container) => {
    const chart = buildChart(container);
    if (chart) {
      chartInstances.push(chart);
    }
  });

  const setupThemeListeners = () => {
    if (window.__pyversusBenchmarkThemeListeners) {
      return;
    }
    window.__pyversusBenchmarkThemeListeners = true;

    const themeObserver = new MutationObserver(() => {
      queueThemeRefresh();
    });
    themeObserver.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["data-theme", "data-mode"],
    });

    document.addEventListener("click", (event) => {
      const target = event.target;
      if (target instanceof Element && target.closest(".theme-switch-button")) {
        queueThemeRefresh();
      }
    });

    const media = window.matchMedia?.("(prefers-color-scheme: dark)");
    if (media?.addEventListener) {
      media.addEventListener("change", queueThemeRefresh);
    } else if (media?.addListener) {
      media.addListener(queueThemeRefresh);
    }
  };

  setupThemeListeners();
  queueThemeRefresh();
})();
