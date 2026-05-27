"""
For pages where the Export <Button> was rewritten in place (rather
than swapped to <ExportButtons>), inject a sibling Copy <Button>
right after it.  Resilient to different formatting / indentation.

The Export button has shape:

  <Button …onClick={() => exportToXlsx(<rows>, [<cols>], "<file>.xlsx", …)}>
      <Download … />
      Export
  </Button>

We capture (rows expression, cols-array literal, filename) and emit
an identical Button right after, calling copyTableToClipboard.
"""
from __future__ import annotations
from pathlib import Path
import re

PAGES_DIR = Path(__file__).resolve().parent.parent / "frontend" / "src" / "pages"
PAGES = [
    "AmsPlanning.tsx", "CategorySales.tsx", "Dashboard.tsx", "Drilldown.tsx",
    "InventoryDashboard.tsx", "MarginSnapshot.tsx", "Returns.tsx",
]

# Permissive: any whitespace, optional const-cols variant, multi-line.
# We need to find a Button whose onClick invokes exportToXlsx.
PATTERN = re.compile(
    r'(?P<indent>[ \t]*)<Button[\s\n]+variant="outline"[\s\n]+size="sm"[\s\n]*'
    r'(?P<extra_attrs>(?:disabled=\{[^}]+\}[\s\n]*)?)'
    r'onClick=\{(?:async\s*)?\(\)\s*=>\s*'
    r'(?:\{[\s\n]*(?:const\s+cols\s*=\s*(?P<cols_in_block>\[[\s\S]*?\]);[\s\n]*(?:const\s+\w+\s*=[\s\S]*?;[\s\n]*)*)?'
    r'exportToXlsx\((?P<args_in_block>[\s\S]*?)\)[\s\n]*;?[\s\n]*\}'
    r'|exportToXlsx\((?P<args_direct>[\s\S]*?)\))\s*\}\s*>[\s\n]*'
    r'<Download[^/]*/>\s*'
    r'Export\s*'
    r'</Button>',
    flags=re.DOTALL,
)


def _build_copy_button(indent: str, rows_expr: str, cols_expr: str,
                       disabled_attr: str) -> str:
    extra = (disabled_attr + "\n" + indent).rstrip() if disabled_attr.strip() else ""
    return (
        f'\n{indent}<Button\n'
        f'{indent}    variant="outline"\n'
        f'{indent}    size="sm"\n'
        + (f'{indent}    {extra}\n' if extra else '')
        + f'{indent}    onClick={{() => copyTableToClipboard({rows_expr}, {cols_expr})}}\n'
        f'{indent}>\n'
        f'{indent}    <Copy className="h-3.5 w-3.5" />\n'
        f'{indent}    Copy\n'
        f'{indent}</Button>'
    )


def _split_args(args: str) -> tuple[str, str, str] | None:
    """Args look like:   rowsExpr,  [colsArray],  "file.xlsx"
    Need to split at the top-level commas, respecting brackets."""
    depth = 0
    parts = []
    last = 0
    for i, ch in enumerate(args):
        if ch in "[({": depth += 1
        elif ch in "])}": depth -= 1
        elif ch == "," and depth == 0:
            parts.append(args[last:i].strip())
            last = i + 1
    parts.append(args[last:].strip())
    if len(parts) < 3:
        return None
    return parts[0], parts[1], parts[2]


for page in PAGES:
    p = PAGES_DIR / page
    if not p.exists():
        print(f"⚠ {page}: not found")
        continue
    text = p.read_text(encoding="utf-8")

    # Skip if a Copy <Button> already exists in JSX (already done by hand?)
    if re.search(r'>\s*Copy\s*</Button>', text):
        print(f"  {page}: Copy button already present, skipping")
        continue

    matches = list(PATTERN.finditer(text))
    if not matches:
        print(f"  {page}: pattern didn't match — needs manual edit")
        continue

    # Walk matches in reverse so insertion offsets don't shift earlier ones
    new = text
    inserted = 0
    for m in reversed(matches):
        cols_text = m.group("cols_in_block")
        rows_expr = None
        cols_expr = None
        # Two flavours of onClick:
        #   () => { const cols=[…]; exportToXlsx(rows, cols, "x.xlsx"); }
        #   () => exportToXlsx(rows, [<inline cols>], "x.xlsx")
        if cols_text:
            # cols was extracted as a const — rows is the first arg, filename the third
            args_text = m.group("args_in_block")
            parts = _split_args(args_text)
            if not parts: continue
            rows_expr, _, fname = parts
            cols_expr = cols_text
        else:
            args_text = m.group("args_direct")
            if not args_text: continue
            parts = _split_args(args_text)
            if not parts: continue
            rows_expr, cols_expr, fname = parts

        indent = m.group("indent") or ""
        disabled_attr = (m.group("extra_attrs") or "").strip()
        copy_btn = _build_copy_button(indent, rows_expr, cols_expr, disabled_attr)
        # Insert right AFTER the closing </Button>
        insert_at = m.end()
        new = new[:insert_at] + copy_btn + new[insert_at:]
        inserted += 1

    if inserted:
        p.write_text(new, encoding="utf-8")
        print(f"✓ {page}: {inserted} Copy button(s) added")
