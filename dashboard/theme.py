"""Accessible visual tokens shared by dashboard HTML and charts."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ThemeTokens:
    """Small semantic token set for one dashboard color mode."""

    background: str
    surface: str
    text: str
    muted: str
    border: str
    primary: str
    positive: str
    warning: str
    grid: str
    category_palette: tuple[str, ...]


THEMES: dict[str, ThemeTokens] = {
    "light": ThemeTokens(
        background="#F5F7FB",
        surface="#FFFFFF",
        text="#172033",
        muted="#526079",
        border="#D7DEEA",
        primary="#255FCC",
        positive="#14733D",
        warning="#8A4B08",
        grid="#E4E9F1",
        category_palette=(
            "#255FCC",
            "#14733D",
            "#9A4F00",
            "#7C3AED",
            "#A61B4D",
            "#087F8C",
        ),
    ),
    "dark": ThemeTokens(
        background="#0F172A",
        surface="#172033",
        text="#F1F5F9",
        muted="#CBD5E1",
        border="#3B4A63",
        primary="#7CB3FF",
        positive="#5EE38D",
        warning="#FDCB6E",
        grid="#334155",
        category_palette=(
            "#7CB3FF",
            "#5EE38D",
            "#FDCB6E",
            "#C4A7FF",
            "#FF91B8",
            "#67E8F9",
        ),
    ),
}


def theme_tokens(theme: str) -> ThemeTokens:
    """Return tokens for a supported theme."""

    try:
        return THEMES[theme]
    except KeyError as error:
        raise ValueError(f"Unsupported theme: {theme}") from error


def theme_css(tokens: ThemeTokens) -> str:
    """Build scoped application CSS without relying on private DOM selectors."""

    return f"""
    <style>
      .stApp {{
        background: {tokens.background};
        color: {tokens.text};
        color-scheme: {"dark" if tokens.background == THEMES["dark"].background else "light"};
      }}
      .stApp h1, .stApp h2, .stApp h3, .stApp p, .stApp label {{
        color: {tokens.text};
      }}
      .stApp label p {{
        color: {tokens.muted};
      }}
      .stApp input, .stApp [role="combobox"], .stApp button {{
        background-color: {tokens.surface};
        border-color: {tokens.border};
        color: {tokens.text};
      }}
      .stApp input::placeholder {{ color: {tokens.muted}; }}
      .stApp button:hover {{
        border-color: {tokens.primary};
        color: {tokens.primary};
      }}
      .dashboard-hero {{
        border: 1px solid {tokens.border};
        border-radius: 18px;
        background: {tokens.surface};
        padding: 1.35rem 1.5rem;
        margin: 0.25rem 0 1rem;
      }}
      .dashboard-eyebrow {{
        color: {tokens.primary};
        font-size: 0.78rem;
        font-weight: 700;
        letter-spacing: 0.08em;
        margin-bottom: 0.35rem;
        text-transform: uppercase;
      }}
      .dashboard-hero h1 {{
        color: {tokens.text};
        font-size: clamp(1.7rem, 3vw, 2.55rem);
        line-height: 1.12;
        margin: 0;
      }}
      .dashboard-hero p {{
        color: {tokens.muted};
        font-size: 1rem;
        margin: 0.55rem 0 0;
      }}
      .dashboard-meta {{
        color: {tokens.muted};
        display: flex;
        flex-wrap: wrap;
        font-size: 0.87rem;
        gap: 0.5rem 1.5rem;
        margin-top: 1rem;
      }}
      .kpi-grid {{
        display: grid;
        gap: 0.85rem;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        margin: 0.5rem 0 1.6rem;
      }}
      .kpi-card, .status-card {{
        background: {tokens.surface};
        border: 1px solid {tokens.border};
        border-radius: 14px;
        padding: 1rem 1.1rem;
      }}
      .kpi-label, .status-label {{
        color: {tokens.muted};
        font-size: 0.84rem;
        font-weight: 600;
      }}
      .kpi-value {{
        color: {tokens.text};
        font-size: clamp(1.35rem, 2vw, 1.9rem);
        font-weight: 750;
        letter-spacing: -0.025em;
        margin-top: 0.25rem;
      }}
      .status-grid {{
        display: grid;
        gap: 0.85rem;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        margin: 0.5rem 0 1rem;
      }}
      .status-value {{
        color: {tokens.text};
        font-size: 1.05rem;
        font-weight: 700;
        margin-top: 0.25rem;
      }}
      .status-ready {{ color: {tokens.positive}; }}
      .status-attention {{ color: {tokens.warning}; }}
      @media (max-width: 900px) {{
        .kpi-grid, .status-grid {{ grid-template-columns: repeat(2, 1fr); }}
      }}
      @media (max-width: 540px) {{
        .dashboard-hero {{ padding: 1rem; }}
        .kpi-grid, .status-grid {{ grid-template-columns: 1fr; }}
      }}
    </style>
    """


def _relative_luminance(color: str) -> float:
    channels = [int(color[index : index + 2], 16) / 255 for index in (1, 3, 5)]
    linear = [
        value / 12.92
        if value <= 0.04045
        else ((value + 0.055) / 1.055) ** 2.4
        for value in channels
    ]
    return 0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2]


def contrast_ratio(foreground: str, background: str) -> float:
    """Return the WCAG contrast ratio for two six-digit hex colors."""

    lighter, darker = sorted(
        (_relative_luminance(foreground), _relative_luminance(background)),
        reverse=True,
    )
    return (lighter + 0.05) / (darker + 0.05)
