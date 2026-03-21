import io
import re
import statistics
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from fpdf import FPDF

from config import MSK_TZ
from services import classify_bp

FONT_DIR = Path(__file__).resolve().parent / "fonts"


def _fmt_date(timestamp: str, with_time: bool = True) -> str:
    """Преобразует 'YYYY-MM-DD HH:MM' в 'ДД.ММ.ГГ HH:MM' или 'ДД.ММ.ГГ'."""
    try:
        dt = datetime.strptime(timestamp[:16], "%Y-%m-%d %H:%M")
        return dt.strftime("%d.%m.%y %H:%M") if with_time else dt.strftime("%d.%m.%y")
    except (ValueError, IndexError):
        return timestamp


def _calc_map(sys_val: int, dia_val: int) -> int:
    """Среднее артериальное давление: MAP = DP + (SP - DP) / 3."""
    return round(dia_val + (sys_val - dia_val) / 3)


class BPReport(FPDF):
    def __init__(self):
        super().__init__()
        self.add_font("DejaVu", "", str(FONT_DIR / "DejaVuSans.ttf"), uni=True)
        self.add_font("DejaVu", "B", str(FONT_DIR / "DejaVuSans-Bold.ttf"), uni=True)

    def _set(self, style="", size=10, color=(0, 0, 0)):
        self.set_font("DejaVu", style, size)
        self.set_text_color(*color)

    def _section_title(self, title: str):
        self._set("B", 13, (44, 62, 80))
        self.cell(0, 8, title, new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(52, 152, 219)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(4)

    def _table_header(self, columns: list[tuple[str, int]], size=9):
        self._set("B", size)
        self.set_fill_color(236, 240, 241)
        for label, width in columns:
            self.cell(width, 7, label, border=1, fill=True, align="C")
        self.ln()

    def header(self):
        pass

    def footer(self):
        self.set_y(-15)
        self._set("", 8, (150, 150, 150))
        self.cell(0, 10, f"Страница {self.page_no()}/{{nb}}", align="C")


def _parse_measurement(measurement: str):
    match = re.match(r"(\d{2,3})/(\d{2,3})", measurement)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None


def _parse_bp_timeseries(bp_records):
    """Извлекает временные ряды из записей АД."""
    dates, sys_vals, dia_vals, pulse_vals, pulse_dates = [], [], [], [], []
    for timestamp, measurement, pulse, _wellbeing in bp_records:
        parsed = _parse_measurement(measurement)
        if not parsed:
            continue
        try:
            dt = datetime.strptime(timestamp[:16], "%Y-%m-%d %H:%M")
        except ValueError:
            continue
        dates.append(dt)
        sys_vals.append(parsed[0])
        dia_vals.append(parsed[1])
        if pulse:
            pulse_vals.append(pulse)
            pulse_dates.append(dt)
    return dates, sys_vals, dia_vals, pulse_vals, pulse_dates


def _parse_med_times(med_records):
    """Извлекает время приёма лекарств с названиями."""
    med_times = []
    for timestamp, med_name, _dosage in med_records:
        try:
            dt = datetime.strptime(timestamp[:16], "%Y-%m-%d %H:%M")
            short_name = med_name[:6] if len(med_name) > 6 else med_name
            med_times.append((dt, short_name))
        except (ValueError, IndexError):
            continue
    return med_times


def _build_bp_chart(bp_records, base_sys: int, base_dia: int, med_records=None) -> bytes | None:
    dates, sys_vals, dia_vals, pulse_vals, pulse_dates = _parse_bp_timeseries(bp_records)
    if len(dates) < 2:
        return None

    has_pulse = len(pulse_vals) > 0
    fig, axes = plt.subplots(
        2 if has_pulse else 1, 1,
        figsize=(10, 6 if has_pulse else 4),
        sharex=True,
        gridspec_kw={"height_ratios": [3, 1]} if has_pulse else None,
    )
    ax_bp = axes[0] if has_pulse else axes

    # --- График АД ---
    ax_bp.plot(dates, sys_vals, "o-", color="#e74c3c", linewidth=1.5, markersize=4, label="Систолическое")
    ax_bp.plot(dates, dia_vals, "o-", color="#3498db", linewidth=1.5, markersize=4, label="Диастолическое")
    ax_bp.fill_between(dates, sys_vals, dia_vals, color="#9b59b6", alpha=0.12, label="Пульсовое давление")

    if base_sys > 0:
        ax_bp.axhline(y=base_sys, color="#e74c3c", linestyle="--", alpha=0.4, linewidth=1)
        ax_bp.axhline(y=base_dia, color="#3498db", linestyle="--", alpha=0.4, linewidth=1)
        ax_bp.text(dates[0], base_sys + 1, f"Норма {base_sys}", fontsize=7, color="#e74c3c", alpha=0.6)
        ax_bp.text(dates[0], base_dia + 1, f"Норма {base_dia}", fontsize=7, color="#3498db", alpha=0.6)

    # Маркеры лекарств (точки внизу графика АД)
    if med_records:
        med_times = _parse_med_times(med_records)
        if med_times:
            y_bottom = min(dia_vals) - 8
            med_by_name = defaultdict(list)
            for dt, name in med_times:
                med_by_name[name].append(dt)
            colors = ["#8e44ad", "#d35400", "#16a085", "#2c3e50", "#c0392b"]
            for idx, (name, times) in enumerate(med_by_name.items()):
                color = colors[idx % len(colors)]
                ys = [y_bottom] * len(times)
                ax_bp.scatter(times, ys, marker="D", color=color, s=20, zorder=5, label=f"{name}")

    ax_bp.set_ylabel("АД (мм рт. ст.)")
    ax_bp.grid(True, alpha=0.3)
    ax_bp.legend(loc="upper left", fontsize=7, ncol=2)

    # --- График пульса ---
    if has_pulse:
        ax_pulse = axes[1]
        ax_pulse.plot(pulse_dates, pulse_vals, "s-", color="#2ecc71", linewidth=1.2, markersize=3, label="Пульс")
        ax_pulse.set_ylabel("Пульс (уд/мин)")
        ax_pulse.grid(True, alpha=0.3)
        ax_pulse.legend(loc="upper left", fontsize=7)

    # Общая ось X
    bottom_ax = axes[1] if has_pulse else ax_bp
    bottom_ax.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m"))
    bottom_ax.xaxis.set_major_locator(mdates.AutoDateLocator())

    fig.autofmt_xdate()
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150)
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


def _build_overview_chart(bp_records, base_sys: int, base_dia: int) -> bytes | None:
    """Обзорный график для длинных периодов: дневные средние + коридор min-max."""
    dates, sys_vals, dia_vals, pulse_vals, pulse_dates = _parse_bp_timeseries(bp_records)
    if len(dates) < 3:
        return None

    # Группировка по дням
    daily = defaultdict(lambda: {"sys": [], "dia": [], "pulse": []})
    for i, dt in enumerate(dates):
        day = dt.date()
        daily[day]["sys"].append(sys_vals[i])
        daily[day]["dia"].append(dia_vals[i])
    for i, dt in enumerate(pulse_dates):
        daily[dt.date()]["pulse"].append(pulse_vals[i])

    if len(daily) < 3:
        return None

    sorted_days = sorted(daily.keys())
    day_dates = [datetime.combine(d, datetime.min.time()) for d in sorted_days]
    avg_sys = [round(statistics.mean(daily[d]["sys"])) for d in sorted_days]
    avg_dia = [round(statistics.mean(daily[d]["dia"])) for d in sorted_days]
    min_sys = [min(daily[d]["sys"]) for d in sorted_days]
    max_sys = [max(daily[d]["sys"]) for d in sorted_days]
    min_dia = [min(daily[d]["dia"]) for d in sorted_days]
    max_dia = [max(daily[d]["dia"]) for d in sorted_days]

    has_pulse = any(daily[d]["pulse"] for d in sorted_days)
    fig, axes = plt.subplots(
        2 if has_pulse else 1, 1,
        figsize=(10, 5 if has_pulse else 3.5),
        sharex=True,
        gridspec_kw={"height_ratios": [3, 1]} if has_pulse else None,
    )
    ax_bp = axes[0] if has_pulse else axes

    ax_bp.plot(day_dates, avg_sys, "-", color="#e74c3c", linewidth=2, label="Систолическое (ср.)")
    ax_bp.fill_between(day_dates, min_sys, max_sys, color="#e74c3c", alpha=0.1)
    ax_bp.plot(day_dates, avg_dia, "-", color="#3498db", linewidth=2, label="Диастолическое (ср.)")
    ax_bp.fill_between(day_dates, min_dia, max_dia, color="#3498db", alpha=0.1)

    if base_sys > 0:
        ax_bp.axhline(y=base_sys, color="#e74c3c", linestyle="--", alpha=0.4, linewidth=1)
        ax_bp.axhline(y=base_dia, color="#3498db", linestyle="--", alpha=0.4, linewidth=1)

    ax_bp.set_ylabel("АД (мм рт. ст.)")
    ax_bp.grid(True, alpha=0.3)
    ax_bp.legend(loc="upper left", fontsize=7)

    if has_pulse:
        ax_pulse = axes[1]
        p_days = [datetime.combine(d, datetime.min.time()) for d in sorted_days if daily[d]["pulse"]]
        p_avg = [round(statistics.mean(daily[d]["pulse"])) for d in sorted_days if daily[d]["pulse"]]
        ax_pulse.plot(p_days, p_avg, "s-", color="#2ecc71", linewidth=1.2, markersize=3, label="Пульс (ср.)")
        ax_pulse.set_ylabel("Пульс")
        ax_pulse.grid(True, alpha=0.3)
        ax_pulse.legend(loc="upper left", fontsize=7)

    bottom_ax = axes[1] if has_pulse else ax_bp
    bottom_ax.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m"))
    bottom_ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    fig.autofmt_xdate()
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150)
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


def _build_weekly_charts(bp_records, base_sys, base_dia, med_records=None) -> list[tuple[str, bytes]]:
    """Разбивает данные по неделям и строит детальный график для каждой."""
    dates, sys_vals, dia_vals, pulse_vals, pulse_dates = _parse_bp_timeseries(bp_records)
    if not dates:
        return []

    # Группировка записей по неделям (пн-вс)
    weeks = defaultdict(list)
    for rec in bp_records:
        try:
            dt = datetime.strptime(rec[0][:16], "%Y-%m-%d %H:%M")
            week_start = dt.date() - timedelta(days=dt.weekday())
            weeks[week_start].append(rec)
        except (ValueError, IndexError):
            continue

    if len(weeks) <= 1:
        return []

    # Группировка лекарств по неделям
    med_by_week = defaultdict(list)
    if med_records:
        for rec in med_records:
            try:
                dt = datetime.strptime(rec[0][:16], "%Y-%m-%d %H:%M")
                week_start = dt.date() - timedelta(days=dt.weekday())
                med_by_week[week_start].append(rec)
            except (ValueError, IndexError):
                continue

    results = []
    for week_start in sorted(weeks.keys()):
        week_end = week_start + timedelta(days=6)
        label = f"{week_start.strftime('%d.%m')} — {week_end.strftime('%d.%m.%y')}"
        week_meds = med_by_week.get(week_start)
        chart = _build_bp_chart(weeks[week_start], base_sys, base_dia, med_records=week_meds)
        if chart:
            results.append((label, chart))

    return results


def _analyze_bp_wellbeing(bp_records):
    ranges = defaultdict(lambda: defaultdict(int))

    for _timestamp, measurement, _pulse, wellbeing in bp_records:
        if not wellbeing:
            continue
        parsed = _parse_measurement(measurement)
        if not parsed:
            continue
        sys_val, dia_val = parsed
        sys_range = (sys_val // 10) * 10
        dia_range = (dia_val // 10) * 10
        key = f"{sys_range}-{sys_range+10}/{dia_range}-{dia_range+10}"
        ranges[key][wellbeing] += 1

    results = []
    feeling_names = {"good": "Хорошо", "ok": "Норм.", "bad": "Плохо"}

    for bp_range, feelings in sorted(ranges.items()):
        total = sum(feelings.values())
        if total < 2:
            continue
        breakdown = []
        best_feeling = max(feelings, key=feelings.get)
        for f in ["good", "ok", "bad"]:
            if f in feelings:
                pct = round(feelings[f] / total * 100)
                breakdown.append(f"{feeling_names[f]} {pct}%")
        results.append((bp_range, total, " | ".join(breakdown), best_feeling))

    return results


def _analyze_time_of_day(bp_records):
    periods = {
        "Утро (08-12)": (8, 12),
        "День (12-18)": (12, 18),
        "Вечер (18-00)": (18, 24),
        "Ночь (00-08)": (0, 8),
    }
    data = {name: {"sys": [], "dia": [], "pulse": []} for name in periods}

    for timestamp, measurement, pulse, _wellbeing in bp_records:
        parsed = _parse_measurement(measurement)
        if not parsed:
            continue
        try:
            hour = int(timestamp[11:13])
        except (ValueError, IndexError):
            continue
        for name, (start, end) in periods.items():
            if start <= hour < end:
                data[name]["sys"].append(parsed[0])
                data[name]["dia"].append(parsed[1])
                if pulse:
                    data[name]["pulse"].append(pulse)
                break

    results = []
    for name in periods:
        d = data[name]
        if not d["sys"]:
            continue
        avg_sys = round(statistics.mean(d["sys"]))
        avg_dia = round(statistics.mean(d["dia"]))
        avg_pulse = round(statistics.mean(d["pulse"])) if d["pulse"] else None
        results.append((name, len(d["sys"]), avg_sys, avg_dia, avg_pulse))

    return results


def _summarize_medication_adherence(med_records, period_days: int):
    """Сводка приёма лекарств: какие, как часто, в какие периоды."""
    if not med_records:
        return []

    meds = defaultdict(lambda: {"dosage": "", "dates": []})
    for timestamp, med_name, dosage in med_records:
        meds[med_name]["dosage"] = dosage
        meds[med_name]["dates"].append(timestamp[:10])

    results = []
    for med_name, info in meds.items():
        dates = sorted(set(info["dates"]))
        total_days = len(dates)
        first_date = dates[0]
        last_date = dates[-1]
        # Регулярность: кол-во дней приёма / кол-во дней в периоде
        try:
            span = (datetime.strptime(last_date, "%Y-%m-%d") - datetime.strptime(first_date, "%Y-%m-%d")).days + 1
        except ValueError:
            span = total_days
        adherence_pct = round(total_days / span * 100) if span > 0 else 0
        results.append((med_name, info["dosage"], total_days, first_date, last_date, adherence_pct))

    return results


# --- Статусы без эмодзи для PDF ---
_STATUS_LABELS = {
    "🟢 В норме": "В норме",
    "🟡 Повышенное": "Повышенное",
    "🟠 Высокое": "Высокое",
    "🔴 Крит. высокая": "Крит. высокая",
    "🔵 Пониженное": "Пониженное",
}

_STATUS_COLORS = {
    "🟢 В норме": (39, 174, 96),
    "🟡 Повышенное": (241, 196, 15),
    "🟠 Высокое": (230, 126, 34),
    "🔴 Крит. высокая": (192, 57, 43),
    "🔵 Пониженное": (52, 152, 219),
}

_FEELING_DISPLAY = {"good": "Хорошо", "ok": "Норм.", "bad": "Плохо"}


def generate_pdf_report(
    bp_records: list,
    med_records: list,
    base_sys: int,
    base_dia: int,
    is_auto: int,
    period_days: int,
) -> bytes:
    pdf = BPReport()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()

    now_str = datetime.now(MSK_TZ).strftime("%d.%m.%Y %H:%M")

    # === ЗАГОЛОВОК ===
    pdf._set("B", 18, (44, 62, 80))
    pdf.cell(0, 12, "Отчёт по артериальному давлению", align="C", new_x="LMARGIN", new_y="NEXT")

    pdf._set("", 10, (127, 140, 141))
    baseline_type = "авто" if is_auto else "ручная"
    period_text = f"Период: {period_days} дн." if period_days > 0 else "Все данные"
    pdf.cell(0, 6, f"{period_text} | Норма: {base_sys}/{base_dia} ({baseline_type})", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 6, f"Сгенерировано: {now_str}", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(8)

    # === СВОДКА ===
    total_bp = len(bp_records)
    status_counts = defaultdict(int)
    feeling_counts = defaultdict(int)
    all_pulse = []
    all_pulse_pressure = []
    all_map = []

    for _ts, measurement, pulse, wellbeing in bp_records:
        parsed = _parse_measurement(measurement)
        if parsed:
            status = classify_bp(parsed[0], parsed[1], base_sys, base_dia)
            status_counts[status] += 1
            all_pulse_pressure.append(parsed[0] - parsed[1])
            all_map.append(_calc_map(parsed[0], parsed[1]))
        if wellbeing:
            feeling_counts[wellbeing] += 1
        if pulse:
            all_pulse.append(pulse)

    pdf._section_title("Сводка")
    pdf._set("", 10)

    pdf.cell(0, 6, f"Замеров: {total_bp}", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 6, f"Приёмов лекарств: {len(med_records)}", new_x="LMARGIN", new_y="NEXT")
    if all_pulse:
        avg_pulse = round(statistics.mean(all_pulse))
        min_p, max_p = min(all_pulse), max(all_pulse)
        pdf.cell(0, 6, f"Пульс: средний {avg_pulse}, мин {min_p}, макс {max_p}", new_x="LMARGIN", new_y="NEXT")
    if all_pulse_pressure:
        avg_pp = round(statistics.mean(all_pulse_pressure))
        min_pp, max_pp = min(all_pulse_pressure), max(all_pulse_pressure)
        pp_note = ""
        if avg_pp > 60:
            pp_note = " (повышено)"
        elif avg_pp < 30:
            pp_note = " (снижено)"
        pdf.cell(0, 6, f"Пульсовое давление: среднее {avg_pp}, мин {min_pp}, макс {max_pp}{pp_note}", new_x="LMARGIN", new_y="NEXT")
    if all_map:
        avg_map = round(statistics.mean(all_map))
        min_map, max_map = min(all_map), max(all_map)
        map_note = ""
        if avg_map < 70:
            map_note = " (снижено)"
        elif avg_map > 105:
            map_note = " (повышено)"
        pdf.cell(0, 6, f"САД (MAP): среднее {avg_map}, мин {min_map}, макс {max_map}{map_note}", new_x="LMARGIN", new_y="NEXT")

    pdf.ln(2)
    for original_status in ["🟢 В норме", "🟡 Повышенное", "🟠 Высокое", "🔴 Крит. высокая", "🔵 Пониженное"]:
        count = status_counts.get(original_status, 0)
        if count > 0:
            pct = round(count / total_bp * 100) if total_bp else 0
            label = _STATUS_LABELS[original_status]
            color = _STATUS_COLORS[original_status]
            pdf._set("", 10, color)
            pdf.cell(0, 6, f"  ● {label}: {count} ({pct}%)", new_x="LMARGIN", new_y="NEXT")
    pdf._set("", 10)

    if feeling_counts:
        pdf.ln(2)
        total_feelings = sum(feeling_counts.values())
        pdf.cell(0, 6, "Самочувствие:", new_x="LMARGIN", new_y="NEXT")
        for key, name in [("good", "Хорошо"), ("ok", "Нормально"), ("bad", "Плохо")]:
            count = feeling_counts.get(key, 0)
            if count > 0:
                pct = round(count / total_feelings * 100)
                pdf.cell(0, 6, f"  {name}: {count} ({pct}%)", new_x="LMARGIN", new_y="NEXT")

    # === ГРАФИКИ ===
    is_long_period = period_days >= 30

    if is_long_period:
        # Обзорный график (дневные средние) для длинных периодов
        overview_png = _build_overview_chart(bp_records, base_sys, base_dia)
        if overview_png:
            pdf.ln(6)
            pdf._section_title("Обзорный график (дневные средние)")
            pdf.image(io.BytesIO(overview_png), x=10, w=190)
            pdf.ln(4)

        # Недельные детальные графики
        weekly_charts = _build_weekly_charts(bp_records, base_sys, base_dia, med_records)
        if weekly_charts:
            pdf.add_page()
            pdf._section_title("Графики по неделям")
            for week_label, chart_data in weekly_charts:
                if pdf.get_y() > 160:
                    pdf.add_page()
                pdf._set("B", 10, (44, 62, 80))
                pdf.cell(0, 7, week_label, new_x="LMARGIN", new_y="NEXT")
                pdf.image(io.BytesIO(chart_data), x=10, w=190)
                pdf.ln(4)
    else:
        # Один детальный график для коротких периодов
        chart_png = _build_bp_chart(bp_records, base_sys, base_dia, med_records)
        if chart_png:
            pdf.ln(6)
            pdf._section_title("График давления")
            pdf.image(io.BytesIO(chart_png), x=10, w=190)
            pdf.ln(4)

    # === АНАЛИТИКА: давление и самочувствие ===
    bp_wellbeing = _analyze_bp_wellbeing(bp_records)
    if bp_wellbeing:
        pdf.add_page()
        pdf._section_title("Аналитика: давление и самочувствие")

        pdf._table_header([("Диапазон", 50), ("Замеров", 25), ("Самочувствие", 115)])
        pdf._set("", 9)
        for bp_range, total, breakdown, _best in bp_wellbeing:
            pdf.cell(50, 7, bp_range, border=1)
            pdf.cell(25, 7, str(total), border=1, align="C")
            pdf.cell(115, 7, breakdown, border=1, new_x="LMARGIN", new_y="NEXT")

        good_ranges = [r for r in bp_wellbeing if r[3] == "good" and r[1] >= 3]
        if good_ranges:
            best_range = max(good_ranges, key=lambda r: r[1])
            pdf.ln(3)
            pdf._set("", 9, (39, 174, 96))
            pdf.cell(0, 6, f"→ Лучшее самочувствие в диапазоне {best_range[0]}", new_x="LMARGIN", new_y="NEXT")

    # === АНАЛИТИКА: время суток ===
    time_analysis = _analyze_time_of_day(bp_records)
    if time_analysis:
        pdf.ln(6)
        pdf._section_title("Аналитика: давление по времени суток")

        pdf._table_header([("Время", 45), ("Замеров", 25), ("Ср. давление", 35), ("Ср. пульс", 25)])
        pdf._set("", 9)
        for name, count, avg_sys, avg_dia, avg_pulse in time_analysis:
            pdf.cell(45, 7, name, border=1)
            pdf.cell(25, 7, str(count), border=1, align="C")
            pdf.cell(35, 7, f"{avg_sys}/{avg_dia}", border=1, align="C")
            pdf.cell(25, 7, str(avg_pulse) if avg_pulse else "—", border=1, align="C")
            pdf.ln()

        if len(time_analysis) > 1:
            best_time = min(time_analysis, key=lambda x: x[2])
            worst_time = max(time_analysis, key=lambda x: x[2])
            pdf.ln(3)
            pdf._set("", 9, (39, 174, 96))
            pdf.cell(0, 6, f"→ Ниже всего: {best_time[0]} ({best_time[2]}/{best_time[3]})", new_x="LMARGIN", new_y="NEXT")
            pdf._set("", 9, (192, 57, 43))
            pdf.cell(0, 6, f"→ Выше всего: {worst_time[0]} ({worst_time[2]}/{worst_time[3]})", new_x="LMARGIN", new_y="NEXT")

    # === СВОДКА ПО ЛЕКАРСТВАМ ===
    med_summary = _summarize_medication_adherence(med_records, period_days)
    if med_summary:
        pdf.ln(6)
        pdf._section_title("Приём лекарств: сводка для врача")

        pdf._table_header([
            ("Лекарство", 45), ("Доза", 25), ("Дней приёма", 25),
            ("Период", 55), ("Регулярность", 30),
        ], size=8)
        pdf._set("", 8)

        for med_name, dosage, total_days, first_date, last_date, adherence_pct in med_summary:
            period_str = f"{_fmt_date(first_date, False)} — {_fmt_date(last_date, False)}"
            adherence_str = f"{adherence_pct}%"
            # Подсветка: <70% — красным
            if adherence_pct < 70:
                color = (192, 57, 43)
            elif adherence_pct < 90:
                color = (241, 196, 15)
            else:
                color = (39, 174, 96)

            pdf.cell(45, 6, med_name, border=1)
            pdf.cell(25, 6, dosage, border=1, align="C")
            pdf.cell(25, 6, str(total_days), border=1, align="C")
            pdf.cell(55, 6, period_str, border=1, align="C")
            pdf._set("B", 8, color)
            pdf.cell(30, 6, adherence_str, border=1, align="C")
            pdf._set("", 8)
            pdf.ln()

    # === ТАБЛИЦА ЗАМЕРОВ ===
    if bp_records:
        pdf.add_page()
        pdf._section_title("Замеры давления")

        cols = [("Дата", 30), ("АД", 22), ("Пульс", 16), ("ПД", 14), ("САД", 14), ("Статус", 32), ("Самочув.", 28)]
        pdf._table_header(cols, size=8)
        pdf._set("", 8)

        for timestamp, measurement, pulse, wellbeing in bp_records:
            if pdf.get_y() > 270:
                pdf.add_page()
                pdf._table_header(cols, size=8)
                pdf._set("", 8)

            parsed = _parse_measurement(measurement)
            status_text = ""
            pp_str = "—"
            map_str = "—"
            if parsed:
                status_full = classify_bp(parsed[0], parsed[1], base_sys, base_dia)
                status_text = _STATUS_LABELS.get(status_full, "")
                pp_str = str(parsed[0] - parsed[1])
                map_str = str(_calc_map(parsed[0], parsed[1]))

            date_str = _fmt_date(timestamp)
            pulse_str = str(pulse) if pulse else "—"
            feel_str = _FEELING_DISPLAY.get(wellbeing, "—") if wellbeing else "—"

            pdf.cell(30, 6, date_str, border=1)
            pdf.cell(22, 6, measurement, border=1, align="C")
            pdf.cell(16, 6, pulse_str, border=1, align="C")
            pdf.cell(14, 6, pp_str, border=1, align="C")
            pdf.cell(14, 6, map_str, border=1, align="C")
            pdf.cell(32, 6, status_text, border=1, align="C")
            pdf.cell(28, 6, feel_str, border=1, align="C")
            pdf.ln()

    # === ТАБЛИЦА ЛЕКАРСТВ ===
    if med_records:
        pdf.add_page()
        pdf._section_title("Приём лекарств")

        med_cols = [("Дата", 40), ("Лекарство", 70), ("Доза", 40)]
        pdf._table_header(med_cols)
        pdf._set("", 9)

        sorted_meds = sorted(med_records, key=lambda r: r[0])
        for timestamp, med_name, dosage in sorted_meds:
            if pdf.get_y() > 270:
                pdf.add_page()
                pdf._table_header(med_cols)
                pdf._set("", 9)

            date_str = _fmt_date(timestamp)
            pdf.cell(40, 7, date_str, border=1)
            pdf.cell(70, 7, med_name, border=1)
            pdf.cell(40, 7, dosage, border=1)
            pdf.ln()

    return pdf.output()
