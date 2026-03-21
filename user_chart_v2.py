"""
Пользовательский график давления v2 — Area Chart с градиентами и тёмной темой.
"""

import io
import re
import statistics
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import numpy as np
from scipy.interpolate import PchipInterpolator
from PIL import Image as PILImage

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.patches import FancyBboxPatch, Polygon
from matplotlib.offsetbox import OffsetImage, AnnotationBbox


# --- Палитра (чёрная тема) ---
_BG_COLOR = "#0a0a0a"
_PLOT_BG = "#111111"
_TEXT_COLOR = "#d0d0d0"
_GRID_COLOR = "#222222"
_SPINE_COLOR = "#222222"

# Цвета статусов
_STATUS_COLORS = {
    "low":      "#418af7",   # синий
    "normal":   "#65f562",   # зелёный
    "elevated": "#dff13c",   # жёлтый
    "high":     "#f3c93d",   # оранжевый
    "crisis":   "#f32b2b",   # красный
}

_STATUS_ORDER = ["low", "normal", "elevated", "high", "crisis"]

# Загрузка PNG-иконок самочувствия
_ASSETS_DIR = Path(__file__).resolve().parent / "assets"
_FACE_IMAGES = {}


def _load_face_images():
    global _FACE_IMAGES
    if _FACE_IMAGES:
        return
    _FACE_FILES = {
        "good": "smiling-face_263a-fe0f.png",
        "ok":   "slightly-smiling-face_1f642.png",
        "bad":  "frowning-face_2639-fe0f.png",
    }
    target_size = 48  # промежуточный размер — LANCZOS сгладит края
    for mood, filename in _FACE_FILES.items():
        path = _ASSETS_DIR / filename
        if path.exists():
            img = PILImage.open(path).convert("RGBA")
            img = img.resize((target_size, target_size), PILImage.LANCZOS)
            _FACE_IMAGES[mood] = np.array(img)


def _classify_status(systolic: int, diastolic: int, base_sys: int, base_dia: int) -> str:
    if systolic >= 160 or diastolic >= 100:
        return "crisis"
    if systolic >= 140 or diastolic >= 90:
        return "high"
    if systolic >= base_sys + 10 or diastolic >= base_dia + 10:
        return "elevated"
    if systolic < base_sys - 20 or diastolic < base_dia - 15:
        return "low"
    return "normal"


def _status_to_num(status: str) -> int:
    return _STATUS_ORDER.index(status)


def _hex_to_hsl(hex_color: str) -> tuple[float, float, float]:
    """Конвертация HEX → HSL."""
    r, g, b = mcolors.to_rgb(hex_color)
    max_c = max(r, g, b)
    min_c = min(r, g, b)
    l = (max_c + min_c) / 2

    if max_c == min_c:
        h = s = 0.0
    else:
        d = max_c - min_c
        s = d / (2 - max_c - min_c) if l > 0.5 else d / (max_c + min_c)
        if max_c == r:
            h = (g - b) / d + (6 if g < b else 0)
        elif max_c == g:
            h = (b - r) / d + 2
        else:
            h = (r - g) / d + 4
        h /= 6

    return h, s, l


def _hsl_to_rgb(h: float, s: float, l: float) -> tuple[float, float, float]:
    """Конвертация HSL → RGB."""
    if s == 0:
        return l, l, l

    def hue2rgb(p, q, t):
        if t < 0: t += 1
        if t > 1: t -= 1
        if t < 1/6: return p + (q - p) * 6 * t
        if t < 1/2: return q
        if t < 2/3: return p + (q - p) * (2/3 - t) * 6
        return p

    q = l * (1 + s) if l < 0.5 else l + s - l * s
    p = 2 * l - q
    return hue2rgb(p, q, h + 1/3), hue2rgb(p, q, h), hue2rgb(p, q, h - 1/3)


def _num_to_color(val: float) -> np.ndarray:
    """Интерполяция в HSL — чистые переходы без коричневой грязи."""
    val = np.clip(val, 0, len(_STATUS_ORDER) - 1)
    idx_lo = int(np.floor(val))
    idx_hi = min(idx_lo + 1, len(_STATUS_ORDER) - 1)
    frac = val - idx_lo

    hsl_lo = _hex_to_hsl(_STATUS_COLORS[_STATUS_ORDER[idx_lo]])
    hsl_hi = _hex_to_hsl(_STATUS_COLORS[_STATUS_ORDER[idx_hi]])

    # Интерполяция hue по короткому пути
    h_lo, h_hi = hsl_lo[0], hsl_hi[0]
    if abs(h_hi - h_lo) > 0.5:
        if h_lo > h_hi:
            h_hi += 1
        else:
            h_lo += 1
    h = (h_lo * (1 - frac) + h_hi * frac) % 1.0
    s = hsl_lo[1] * (1 - frac) + hsl_hi[1] * frac
    l = hsl_lo[2] * (1 - frac) + hsl_hi[2] * frac

    r, g, b = _hsl_to_rgb(h, s, l)
    return np.array([r, g, b, 1.0])


def _build_gradient_image(
    status_nums: list[float],
    x_positions: np.ndarray,
    x_range: tuple[float, float],
    width_px: int = 1200,
) -> np.ndarray:
    x_min, x_max = x_range
    pixel_x = np.linspace(x_min, x_max, width_px)
    interp_nums = np.interp(pixel_x, x_positions, status_nums)

    img = np.zeros((1, width_px, 4))
    for i in range(width_px):
        img[0, i] = _num_to_color(interp_nums[i])

    return img


def _parse_measurement(measurement: str):
    match = re.match(r"(\d{2,3})/(\d{2,3})", measurement)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None


def generate_user_chart_v2(
    bp_records: list,
    base_sys: int,
    base_dia: int,
    period_days: int = 7,
) -> bytes | None:
    """
    Генерирует PNG-картинку с пользовательским графиком давления v2.

    bp_records: [(timestamp, measurement, pulse, wellbeing), ...]
    Возвращает bytes PNG или None если данных мало.
    """
    _load_face_images()

    # --- Парсинг данных ---
    dates = []
    sys_vals = []
    dia_vals = []
    pulses = []
    wellbeing_list = []

    for timestamp, measurement, pulse, wellbeing in bp_records:
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
        pulses.append(pulse)
        wellbeing_list.append(wellbeing)

    if len(dates) < 2:
        return None

    # --- Группировка по дням для длинных периодов ---
    use_daily = period_days >= 14 and len(dates) > 14
    if use_daily:
        daily = defaultdict(lambda: {"sys": [], "dia": [], "pulse": [], "wb": []})
        for i, dt in enumerate(dates):
            day = dt.date()
            daily[day]["sys"].append(sys_vals[i])
            daily[day]["dia"].append(dia_vals[i])
            if pulses[i]:
                daily[day]["pulse"].append(pulses[i])
            daily[day]["wb"].append(wellbeing_list[i])

        sorted_days = sorted(daily.keys())
        dates = [datetime.combine(d, datetime.min.time().replace(hour=12)) for d in sorted_days]

        def _dominant_wb(wb_list):
            counts = defaultdict(int)
            for w in wb_list:
                if w:
                    counts[w] += 1
            return max(counts, key=counts.get) if counts else None

        wellbeing_list = [_dominant_wb(daily[d]["wb"]) for d in sorted_days]
        sys_vals = [round(statistics.mean(daily[d]["sys"])) for d in sorted_days]
        dia_vals = [round(statistics.mean(daily[d]["dia"])) for d in sorted_days]
        pulses = [
            round(statistics.mean(daily[d]["pulse"])) if daily[d]["pulse"] else None
            for d in sorted_days
        ]

    n = len(dates)

    # --- Равномерное распределение по индексу ---
    x_points = np.arange(n, dtype=float)

    # --- PCHIP интерполяция ---
    n_smooth = max(n * 50, 500)
    x_smooth = np.linspace(0, n - 1, n_smooth)

    pchip_sys = PchipInterpolator(x_points, sys_vals)
    pchip_dia = PchipInterpolator(x_points, dia_vals)
    sys_smooth = pchip_sys(x_smooth)
    dia_smooth = pchip_dia(x_smooth)

    # --- Границы ---
    y_min = max(min(dia_vals) - 20, 40)
    y_max = max(sys_vals) + 35

    # --- Подготовка фигуры ---
    fig, ax = plt.subplots(figsize=(10, 5), facecolor=_BG_COLOR)
    ax.set_facecolor(_PLOT_BG)

    # --- Горизонтальный градиент (HSL интерполяция) через imshow + clipping mask ---
    statuses = [_classify_status(s, d, base_sys, base_dia) for s, d in zip(sys_vals, dia_vals)]
    status_nums = [float(_status_to_num(s)) for s in statuses]

    grad_img = _build_gradient_image(
        status_nums, x_points,
        (x_smooth[0], x_smooth[-1]),
        width_px=1200,
    )

    im = ax.imshow(
        grad_img,
        aspect="auto",
        extent=[x_smooth[0], x_smooth[-1], y_min, y_max],
        origin="lower",
        zorder=2,
        alpha=1.0,
    )

    # Полигон-маска
    poly_verts = (
        list(zip(x_smooth, sys_smooth))
        + list(zip(x_smooth[::-1], dia_smooth[::-1]))
    )
    clip_polygon = Polygon(poly_verts, closed=True, transform=ax.transData)
    im.set_clip_path(clip_polygon)

    # --- Тонкая обводка ленты ---
    ax.plot(x_smooth, sys_smooth, color="white", linewidth=0.8, alpha=0.3, zorder=3)
    ax.plot(x_smooth, dia_smooth, color="white", linewidth=0.8, alpha=0.2, zorder=3)

    # --- Якорь "Рабочая норма" ---
    _norm_color = _STATUS_COLORS["normal"]
    anchor_x = -1.0
    anchor_width = 0.6
    anchor = FancyBboxPatch(
        (anchor_x, base_dia), anchor_width, base_sys - base_dia,
        boxstyle="round,pad=0.05",
        facecolor=_norm_color, alpha=0.18,
        edgecolor=_norm_color, linewidth=1.2,
        zorder=2,
    )
    ax.add_patch(anchor)
    ax.text(
        anchor_x + anchor_width / 2, base_sys + 2,
        f"{base_sys}", fontsize=7, color=_norm_color,
        ha="center", va="bottom", zorder=5,
    )
    ax.text(
        anchor_x + anchor_width / 2, base_dia - 2,
        f"{base_dia}", fontsize=7, color=_norm_color,
        ha="center", va="top", zorder=5,
    )
    ax.text(
        anchor_x + anchor_width / 2, (base_sys + base_dia) / 2,
        "норма", fontsize=6, color=_norm_color,
        ha="center", va="center", rotation=90, zorder=5,
    )

    # --- Оси и сетка ---
    ax.set_ylim(y_min, y_max)
    ax.set_xlim(anchor_x - 0.4, n - 0.5)

    ax.set_ylabel("мм рт. ст.", fontsize=10, color=_TEXT_COLOR, labelpad=10)
    ax.tick_params(axis="y", colors=_TEXT_COLOR, labelsize=9)

    # --- Форматирование оси X: двухуровневое ---
    _MONTHS_RU = {
        "January": "января", "February": "февраля", "March": "марта",
        "April": "апреля", "May": "мая", "June": "июня",
        "July": "июля", "August": "августа", "September": "сентября",
        "October": "октября", "November": "ноября", "December": "декабря",
    }

    ax.set_xticks(x_points)

    if use_daily:
        # Дневной режим: метки — даты
        date_labels = []
        for dt in dates:
            month_en = dt.strftime("%B")
            month_ru = _MONTHS_RU.get(month_en, month_en)
            date_labels.append(f"{dt.day} {month_ru}")
        ax.set_xticklabels(date_labels, fontsize=7, color=_TEXT_COLOR, rotation=45, ha="right")
        ax.tick_params(axis="x", length=3, color=_SPINE_COLOR, pad=3)

        # Эмодзи под датами
        for i in range(n):
            wb = wellbeing_list[i]
            if wb is None or wb not in _FACE_IMAGES:
                continue
            imagebox = OffsetImage(_FACE_IMAGES[wb], zoom=0.25)
            ab = AnnotationBbox(
                imagebox,
                (x_points[i], 0),
                xycoords=("data", "axes fraction"),
                box_alignment=(0.5, 2.0),
                frameon=False, zorder=5,
                annotation_clip=False,
            )
            ax.add_artist(ab)
    else:
        # Короткий период: время + эмодзи + даты внизу
        time_labels = [dt.strftime("%H:%M") for dt in dates]
        ax.set_xticklabels(time_labels, fontsize=7, color=_TEXT_COLOR, rotation=0)
        ax.tick_params(axis="x", length=3, color=_SPINE_COLOR, pad=3)

        # PNG-эмодзи рядом с меткой времени
        for i in range(n):
            wb = wellbeing_list[i]
            if wb is None or wb not in _FACE_IMAGES:
                continue
            imagebox = OffsetImage(_FACE_IMAGES[wb], zoom=0.25)
            ab = AnnotationBbox(
                imagebox,
                (x_points[i], 0),
                xycoords=("data", "axes fraction"),
                box_alignment=(-0.8, 0.5),
                frameon=False, zorder=5,
                annotation_clip=False,
            )
            ax.add_artist(ab)

        # Группировка по дням — даты под временем
        day_groups = defaultdict(list)
        for i, dt in enumerate(dates):
            day_key = dt.date()
            day_groups[day_key].append((i, dt))

        processed_days = sorted(day_groups.items())
        for day_idx, (day_date, points) in enumerate(processed_days):
            indices = [p[0] for p in points]
            center = (min(indices) + max(indices)) / 2
            dt = points[0][1]
            month_en = dt.strftime("%B")
            month_ru = _MONTHS_RU.get(month_en, month_en)
            day_label = f"{dt.day} {month_ru}"

            ax.text(
                center, y_min - (y_max - y_min) * 0.1,
                day_label,
                fontsize=9, fontweight="bold", color=_TEXT_COLOR,
                ha="center", va="top",
                transform=ax.transData,
                clip_on=False,
            )

            if day_idx > 0:
                prev_indices = [p[0] for p in processed_days[day_idx - 1][1]]
                separator_x = (max(prev_indices) + min(indices)) / 2
                ax.axvline(
                    separator_x, color=_TEXT_COLOR, alpha=0.12,
                    linewidth=1, linestyle="--", zorder=1,
                )

    for spine in ax.spines.values():
        spine.set_visible(False)

    ax.grid(axis="y", color=_GRID_COLOR, linewidth=0.5, alpha=0.5, zorder=0)
    ax.grid(axis="x", visible=False)

    # --- Нижняя информационная панель ---
    avg_sys = round(statistics.mean(sys_vals))
    avg_dia = round(statistics.mean(dia_vals))
    valid_pulses = [p for p in pulses if p]
    avg_pulse = round(statistics.mean(valid_pulses)) if valid_pulses else None

    info_parts = [f"Среднее: {avg_sys}/{avg_dia}"]
    if avg_pulse:
        info_parts.append(f"Пульс: {avg_pulse}")
    info_parts.append(f"Замеров: {n}")
    info_text = "  ·  ".join(info_parts)

    fig.text(
        0.5, 0.02, info_text,
        ha="center", fontsize=9, color=_TEXT_COLOR,
        bbox=dict(
            boxstyle="round,pad=0.4",
            facecolor=_PLOT_BG,
            edgecolor=_GRID_COLOR,
            alpha=0.9,
        ),
    )

    # --- Заголовок ---
    period_label = f"за {period_days} дн." if period_days > 0 else ""
    ax.set_title(
        f"Давление {period_label}",
        fontsize=14, fontweight="bold", color=_TEXT_COLOR, pad=15,
    )

    plt.tight_layout(rect=[0, 0.12, 1, 0.95])

    # --- Сохранение ---
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, facecolor=_BG_COLOR)
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


# --- Тестовый прогон ---
if __name__ == "__main__":
    import random
    random.seed(42)

    # Генерация 14 дней тестовых данных (2-3 замера в день)
    test_records = []
    base_date = datetime(2025, 3, 5)
    for day in range(14):
        dt = base_date.replace(day=base_date.day + day)
        n_measures = random.choice([2, 3])
        hours = sorted(random.sample([8, 9, 12, 14, 17, 18, 19, 20], n_measures))
        for h in hours:
            sys = random.randint(95, 165)
            dia = random.randint(55, 100)
            pulse = random.randint(58, 95)
            wb = random.choice(["good", "ok", "bad"])
            test_records.append((f"{dt.strftime('%Y-%m-%d')} {h:02d}:00", f"{sys}/{dia}", pulse, wb))

    png_data = generate_user_chart_v2(
        bp_records=test_records,
        base_sys=120,
        base_dia=80,
        period_days=14,
    )

    if png_data:
        out_path = "test_chart_v2.png"
        with open(out_path, "wb") as f:
            f.write(png_data)
        print(f"Сохранено: {out_path} ({len(png_data)} байт)")
    else:
        print("Недостаточно данных")
