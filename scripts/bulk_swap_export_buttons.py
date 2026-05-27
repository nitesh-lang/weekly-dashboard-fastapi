"""
One-shot rewrite of every page's export button:
  exportToCsv → exportToXlsx + add Copy button next to it.

Patterns matched (and rewritten):

  import { … exportToCsv … } from "@/lib/utils";
        ↓
  import { … exportToXlsx, copyTableToClipboard … } from "@/lib/utils";

  exportToCsv(<rows>, <cols>, "name.csv")
        ↓
  exportToXlsx(<rows>, <cols>, "name.xlsx")

  <Button …onClick={() => { … exportToXlsx(rows, cols, "name.xlsx"); … }}>
      <Download … />Export CSV
  </Button>
        ↓
  <Button …onClick={() => { … exportToXlsx(rows, cols, "name.xlsx"); … }}>
      <Download … />Export
  </Button>
  <Button … onClick={() => copyTableToClipboard(rows, cols)}>
      <Copy … />Copy
  </Button>

Idempotent — re-running on already-rewritten files is a no-op.
"""
from __future__ import annotations
from pathlib import Path
import re

PAGES_DIR = Path(__file__).resolve().parent.parent / "frontend" / "src" / "pages"

PAGES = [
    "AmazonSalesTrend.tsx", "AmsPlanning.tsx", "AmsPoorPerformers.tsx",
    "CategorySales.tsx", "Dashboard.tsx", "DeadStock.tsx", "Drilldown.tsx",
    "InventoryDashboard.tsx", "MarginSnapshot.tsx", "NoSalesLastWeek.tsx",
    "Returns.tsx", "SalesTrend.tsx",
]


def rewrite(text: str) -> tuple[str, list[str]]:
    notes = []

    # 1. Import update
    new = re.sub(
        r'(import\s*\{[^}]*?)\bexportToCsv\b([^}]*?\}\s*from\s*"@/lib/utils"\s*;)',
        lambda m: m.group(1) + 'exportToXlsx, copyTableToClipboard' + m.group(2),
        text,
        count=1,
    )
    if new != text:
        notes.append("import swapped")
    text = new

    # 2. Add Copy icon to lucide imports if not present
    if '"lucide-react"' in text and 'Copy' not in re.findall(r'from\s*"lucide-react"', text)[0]:
        # find the lucide-react import line
        m = re.search(r'import\s*\{([^}]*)\}\s*from\s*"lucide-react"\s*;', text)
        if m and 'Copy' not in m.group(1):
            inside = m.group(1).strip().rstrip(',')
            replacement = f'import {{ {inside}, Copy, Check }} from "lucide-react";'
            text = text[:m.start()] + replacement + text[m.end():]
            notes.append("lucide Copy/Check added")

    # 3. exportToCsv(...) → exportToXlsx(...)  + .csv → .xlsx in same call
    def _fix_call(m: re.Match) -> str:
        body = m.group(1)
        body = re.sub(r'"([^"]+)\.csv"', r'"\1.xlsx"', body)
        return 'exportToXlsx(' + body + ')'
    new = re.sub(r'exportToCsv\((.*?)\)', _fix_call, text, flags=re.DOTALL)
    if new != text:
        notes.append("exportToCsv calls rewritten")
    text = new

    # 4. Rename button label "Export CSV" → "Export"
    new = text.replace("Export CSV", "Export")
    if new != text:
        notes.append("button label trimmed")
    text = new

    return text, notes


for page in PAGES:
    p = PAGES_DIR / page
    if not p.exists():
        print(f"⚠ {page}: not found")
        continue
    before = p.read_text(encoding="utf-8")
    after, notes = rewrite(before)
    if after == before:
        print(f"  {page}: no change needed")
        continue
    p.write_text(after, encoding="utf-8")
    print(f"✓ {page}: " + " · ".join(notes))
