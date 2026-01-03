(() => {
  const data = window.PYVERSUS_BENCHMARK_DATA;
  if (!data || !Array.isArray(data.rows) || !Array.isArray(data.series)) {
    return;
  }

  const formatRowsShort = (value) => {
    if (value >= 1_000_000) {
      const num = value / 1_000_000;
      return `${num % 1 === 0 ? num.toFixed(0) : num.toFixed(1)}M`;
    }
    if (value >= 1_000) {
      const num = value / 1_000;
      return `${num % 1 === 0 ? num.toFixed(0) : num.toFixed(1)}k`;
    }
    return `${value}`;
  };

  const formatTick = (value, step) => {
    const decimals = step < 1 ? Math.ceil(Math.abs(Math.log10(step))) : 0;
    return value.toLocaleString(undefined, {
      maximumFractionDigits: decimals,
      minimumFractionDigits: decimals,
    });
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
    if (!kind) {
      return;
    }
    const title = container.dataset.title || "";
    const yLabel = container.dataset.yLabel || "";
    const height = Number.parseInt(container.dataset.height || "360", 10);

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
    const computedStyle = getComputedStyle(container);
    const series = data.series.map((item) => {
      const cssColor = computedStyle
        .getPropertyValue(`--benchmark-series-${item.id}`)
        .trim();
      return {
        id: item.id,
        label: item.label,
        color: cssColor || item.color || "#3b82f6",
        values: item[kind],
      };
    });

    series.forEach((item) => {
      const legendItem = document.createElement("div");
      legendItem.className = "chart-legend-item";
      const swatch = document.createElement("span");
      swatch.className = "chart-legend-swatch";
      swatch.style.background = item.color;
      const label = document.createElement("span");
      label.textContent = item.label;
      legendItem.append(swatch, label);
      legend.append(legendItem);
    });

    let activePoint = null;
    let points = [];

    const render = () => {
      const width = body.clientWidth;
      const dpr = window.devicePixelRatio || 1;
      canvas.width = Math.max(1, Math.floor(width * dpr));
      canvas.height = Math.max(1, Math.floor(height * dpr));
      canvas.style.width = `${width}px`;
      canvas.style.height = `${height}px`;

      const ctx = canvas.getContext("2d");
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, width, height);

      const style = getComputedStyle(container);
      const axisColor = style.getPropertyValue("--benchmark-axis").trim() || "#1f2933";
      const gridColor = style.getPropertyValue("--benchmark-grid").trim() || "rgba(0,0,0,0.08)";
      const labelColor = style.getPropertyValue("--benchmark-label").trim() || "#374151";
      const fontFamily = style.fontFamily || "system-ui";

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
        ctx.fillText(formatTick(tick, step), plot.left - 8, y);
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
      const rowsText = point.row.toLocaleString();
      const valueText =
        kind === "time"
          ? `${point.value.toFixed(3)} s`
          : `${point.value.toFixed(1)} MB`;
      tooltip.innerHTML = `<div class="tooltip-title">${label}</div><div class="tooltip-body">${rowsText} rows · ${valueText}</div>`;
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

    const resizeObserver = new ResizeObserver(render);
    resizeObserver.observe(container);

    const themeObserver = new MutationObserver((mutations) => {
      for (const mutation of mutations) {
        if (mutation.type === "attributes" && mutation.attributeName === "data-theme") {
          activePoint = null;
          showTooltip(null);
          render();
          break;
        }
      }
    });
    themeObserver.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["data-theme"],
    });

    render();
  };

  document.querySelectorAll(".benchmark-chart[data-benchmark-chart]").forEach((container) => {
    buildChart(container);
  });
})();
