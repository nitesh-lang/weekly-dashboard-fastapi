"""
Step 2 of the export refactor: replace the inline Export <Button>
block on each page with the shared <ExportButtons> component (which
adds the Copy button).

Looks for:

  <Button
      variant="outline"
      size="sm"
      onClick={() => {
          const cols = [ … ];
          exportToXlsx(<rowsExpr>, cols, "<filename>.xlsx");
      }}
  >
      <Download … />
      Export
  </Button>

…and rewrites to:

  <ExportButtons
      rows={<rowsExpr> as any}
      columns={<cols-array-literal>}
      filename="<filename>.xlsx"
  />

Also adds `import { ExportButtons } from "@/components/ExportButtons";`
and removes the now-unused `Download` icon + `exportToXlsx`/`copyTableToClipboard`
imports if they're no longer referenced.
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

BTN_PATTERN = re.compile(
    r'<Button[\s\n]+variant="outline"[\s\n]+size="sm"[\s\n]+onClick=\{\(\)\s*=>\s*\{'
    r'\s*const\s+cols\s*=\s*(?P<cols>\[.*?\]);\s*'
    r'exportToXlsx\((?P<rows>[^,]+),\s*cols,\s*"(?P<file>[^"]+)"\)\s*;\s*'
    r'\}\}\s*>\s*'
    r'<Download[^/]*/>\s*'
    r'Export\s*'
    r'</Button>',
    flags=re.DOTALL,
)


def rewrite(text: str) -> tuple[str, list[str]]:
    notes: list[str] = []
    new = text

    # Replace each Export button with the shared component
    matches = list(BTN_PATTERN.finditer(new))
    if matches:
        offset = 0
        for m in matches:
            cols_lit = m.group("cols").strip()
            rows_expr = m.group("rows").strip()
            fname = m.group("file")
            # Indent the columns literal to match JSX flow
            replacement = (
                f'<ExportButtons\n'
                f'                                            rows={{{rows_expr} as any}}\n'
                f'                                            columns={{{cols_lit}}}\n'
                f'                                            filename="{fname}"\n'
                f'                                        />'
            )
            start = m.start() + offset
            end = m.end() + offset
            new = new[:start] + replacement + new[end:]
            offset += len(replacement) - (end - start)
        notes.append(f"{len(matches)} button block(s) → <ExportButtons>")

    # Add ExportButtons import if we changed anything and it's missing
    if notes and "ExportButtons" not in re.findall(r'from\s*"@/components/ExportButtons"', new):
        # Insert after the last @/components/ import
        comp_imports = list(re.finditer(r'import\s*\{[^}]*\}\s*from\s*"@/components/[^"]+"\s*;\s*\n', new))
        if comp_imports:
            insert_at = comp_imports[-1].end()
            new = new[:insert_at] + 'import { ExportButtons } from "@/components/ExportButtons";\n' + new[insert_at:]
            notes.append("added ExportButtons import")
        else:
            # Fallback: after lucide-react import
            lr = re.search(r'import\s*\{[^}]*\}\s*from\s*"lucide-react"\s*;\s*\n', new)
            if lr:
                new = new[:lr.end()] + 'import { ExportButtons } from "@/components/ExportButtons";\n' + new[lr.end():]
                notes.append("added ExportButtons import (after lucide)")

    return new, notes


for page in PAGES:
    p = PAGES_DIR / page
    if not p.exists():
        print(f"⚠ {page}: not found")
        continue
    before = p.read_text(encoding="utf-8")
    after, notes = rewrite(before)
    if after == before:
        print(f"  {page}: no change (pattern didn't match — manual review may be needed)")
        continue
    p.write_text(after, encoding="utf-8")
    print(f"✓ {page}: " + " · ".join(notes))
