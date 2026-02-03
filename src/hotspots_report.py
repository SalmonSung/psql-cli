# hotspots_report.py
from __future__ import annotations

import re
import os
from pathlib import Path
from urllib.parse import quote
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional
from jinja2 import Environment, BaseLoader
from markupsafe import Markup, escape

import config as config

try:
    # Plotly provides this helper to inline plotly.js as a string (offline, single-file)
    from plotly.offline import get_plotlyjs  # type: ignore
except Exception as e:  # pragma: no cover
    get_plotlyjs = None  # type: ignore


mother_dir = os.path.dirname(os.path.abspath(__file__))
html_template_path = os.path.join(mother_dir, "figure_logic", "hotspots_report_template.html")

with open(html_template_path, "r", encoding="utf-8") as f:
    _HTML_TEMPLATE = f.read()


def _slugify(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s or "figure"


@dataclass
class FigureEntry:
    category: str
    title: str
    figure_html: str
    notes: List[str] = field(default_factory=list)
    id: Optional[str] = None

    def normalized_id(self) -> str:
        if self.id and self.id.strip():
            return _slugify(self.id)
        return _slugify(f"{self.category}-{self.title}")


class HotspotsReport:
    """
    Single-file, fully-offline HTML report generator using Jinja2 + Plotly fragments.

    You provide Plotly figure fragments with include_plotlyjs=False, full_html=False.
    This class embeds plotly.js ONCE inline, so the final HTML is standalone offline.
    """

    def __init__(
        self,
        *,
        version: str,
        system_info: Dict[str, str],
        report_title_base: str = "PostgreSQL Hotspots",
        report_type: str = "General Report"
    ) -> None:
        self.version = str(version)
        self.system_info = {str(k): str(v) for k, v in system_info.items()}
        self.report_title_base = str(report_title_base)
        self.report_type = str(report_type)
        self._figures: List[FigureEntry] = []

    def add_figure(self, entry: FigureEntry | Dict[str, Any]) -> str:
        """Add a figure entry; returns the assigned unique figure id."""
        if isinstance(entry, dict):
            entry = FigureEntry(
                category=entry["category"],
                title=entry["title"],
                figure_html=entry["figure_html"],
                notes=list(entry.get("notes", [])),
                id=entry.get("id"),
            )

        if not entry.category or not entry.title:
            raise ValueError("Figure entry must have non-empty 'category' and 'title'.")
        if not isinstance(entry.figure_html, str) or not entry.figure_html.strip():
            raise ValueError("Figure entry must have non-empty 'figure_html' string.")
        if entry.notes is None:
            entry.notes = []
        entry.notes = [str(x) for x in entry.notes]

        self._figures.append(entry)
        # ensure unique ids after adding
        ids = self._unique_ids()
        return ids[-1]

    def add_figures(self, entries: Iterable[FigureEntry | Dict[str, Any]]) -> List[str]:
        ids: List[str] = []
        for e in entries:
            ids.append(self.add_figure(e))
        return ids

    @staticmethod
    def plotly_fragment(fig: Any, *, div_id: Optional[str] = None) -> str:
        """
        Convert a plotly.graph_objects.Figure to an embeddable HTML fragment.
        This MUST NOT include plotly.js because the report embeds it once.
        """
        # Avoid importing plotly.graph_objects types here; keep it duck-typed.
        to_html = getattr(fig, "to_html", None)
        if not callable(to_html):
            raise TypeError("fig must be a Plotly figure with a .to_html(...) method")

        # div_id is optional; Plotly will generate one if None
        return str(
            fig.to_html(
                full_html=False,
                include_plotlyjs=False,
                div_id=div_id,
                config={"responsive": True},
            )
        )

    def render(self, output_path: str) -> None:
        html = self.render_string()
        output_path = config.OUTPUT_DIR_PATH / output_path
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)

    def render_string(self) -> str:
        if get_plotlyjs is None:
            raise RuntimeError(
                "plotly.offline.get_plotlyjs is not available. "
                "Ensure 'plotly' is installed in this environment."
            )

        figures = self._build_figures_payload()

        # Build category grouping for the sidebar
        cat_map: Dict[str, List[Dict[str, str]]] = {}
        for f in figures:
            cat_map.setdefault(f["category"], []).append({"id": f["id"], "title": f["title"]})

        categories = [{"name": cat, "items": items} for cat, items in cat_map.items()]

        # Preserve ordering by first appearance of category
        seen = set()
        ordered_categories = []
        for f in figures:
            cat = f["category"]
            if cat not in seen:
                seen.add(cat)
                ordered_categories.append(cat)
        categories.sort(key=lambda c: ordered_categories.index(c["name"]) if c["name"] in ordered_categories else 10**9)

        # System info: keep insertion order as provided (python 3.7+ dict order)
        system_info_items = list(self.system_info.items())

        # Plotly JS once (offline)
        plotly_js = get_plotlyjs()

        # Jinja2 render
        env = Environment(loader=BaseLoader(), autoescape=True)
        tmpl = env.from_string(_HTML_TEMPLATE)

        # figures_index_json is used by JS for menus/dropdowns
        figures_index_json = _to_json([{"id": f["id"], "title": f["title"], "category": f["category"]} for f in figures])

        page_title = f"PostgreSQL Hotspots {self.version}".strip()


        rendered = tmpl.render(
            page_title=page_title,
            report_title_base=self.report_title_base,
            report_type=self.report_type,
            version=self.version,
            system_info_items=[(escape(k), escape(v)) for (k, v) in system_info_items],
            categories=categories,
            figures=[{
                "id": f["id"],
                "category": f["category"],
                "title": f["title"],
                # Mark figure_html as safe HTML (it contains <div> and <script>)
                "figure_html": Markup(f["figure_html"]),
                "notes": [_note_to_markup(n) for n in f["notes"]],
            } for f in figures],
            plotly_js=Markup(plotly_js),
            figures_index_json=Markup(figures_index_json),
        )
        return rendered

    def _unique_ids(self) -> List[str]:
        used: Dict[str, int] = {}
        result: List[str] = []
        for e in self._figures:
            base = e.normalized_id()
            n = used.get(base, 0)
            if n == 0:
                used[base] = 1
                result.append(base)
            else:
                used[base] = n + 1
                result.append(f"{base}-{n+1}")
        return result

    def _build_figures_payload(self) -> List[Dict[str, Any]]:
        ids = self._unique_ids()
        payload: List[Dict[str, Any]] = []
        for idx, e in enumerate(self._figures):
            payload.append(
                {
                    "id": ids[idx],
                    "category": str(e.category),
                    "title": str(e.title),
                    "figure_html": str(e.figure_html),
                    "notes": list(e.notes or []),
                }
            )
        return payload


def _to_json(obj: Any) -> str:
    # tiny local helper to avoid requiring extra deps
    import json
    return json.dumps(obj, ensure_ascii=False)

_NOTE_LINK_RE = re.compile(r'^\s*\[\[(?P<label>[^|\]]+)\|(?P<file>[^\]]+)\]\]\s*$')

def _note_to_markup(note: str) -> Markup:
    """
    Convert note strings into either:
      - a safe clickable link to a local sibling text file, or
      - plain escaped text

    Supported formats:
      1) "analysis.txt"                     -> link with label "analysis.txt"
      2) "[[Open analysis|analysis.txt]]"   -> link with label "Open analysis"
    """
    s = str(note).strip()
    if not s:
        return Markup("")

    label: str | None = None
    filename: str | None = None

    m = _NOTE_LINK_RE.match(s)
    if m:
        label = m.group("label").strip()
        filename = m.group("file").strip()
    else:
        # If it looks like a simple safe filename, treat it as a link
        if re.fullmatch(r"[A-Za-z0-9._-]+\.(txt|log|md)", s, flags=re.IGNORECASE):
            label = s
            filename = s

    # Not a link → render as plain escaped text
    if not filename:
        return Markup(escape(s))

    # Security: force "same folder" only (no paths)
    safe_name = os.path.basename(filename)
    if safe_name != filename:
        # Path traversal attempt → render as plain text
        return Markup(escape(s))

    # URL-encode filename for spaces etc.
    href = quote(safe_name)

    safe_label = escape(label or safe_name)
    return Markup(f'<a href="{href}" target="_blank" rel="noopener noreferrer">{safe_label}</a>')
