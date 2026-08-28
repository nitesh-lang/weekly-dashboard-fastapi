# ============================================================
#  deploy_bsr.ps1  —  validate, build, commit, push BSR data
#  For days when you DROP the Keepa CSVs in manually.
#  (Use run_bsr_daily.ps1 instead when the Keepa API fetch runs.)
#
#  Save as:  <repo>\data\deploy_bsr.ps1
#  Run as:   .\data\deploy_bsr.ps1
#            .\data\deploy_bsr.ps1 -Date 26-07-29     (backfill a past day)
#            .\data\deploy_bsr.ps1 -Force             (push even if a brand is missing)
# ============================================================

param(
    [string]$Date = (Get-Date -Format "yy-MM-dd"),
    [switch]$Force
)

$ErrorActionPreference = "Stop"

# Anchor to repo root (this script lives in <root>\data)
$ProjectDir = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectDir

Write-Host ""
Write-Host "=== BSR deploy: $Date ===" -ForegroundColor Cyan
Write-Host "repo: $ProjectDir"
Write-Host ""

# ---- sanity: are we actually in the repo? -------------------
if (-not (Test-Path ".git")) {
    Write-Host "ABORT: no .git here. Is the script in <repo>\data\ ?" -ForegroundColor Red
    exit 1
}

# ---- 1. validate the dropped files --------------------------
$brands  = @("Audio Array", "Nexlev", "Tonor", "White Mulberry")
$missing = @()
$rowInfo = @()

foreach ($b in $brands) {
    $p = Join-Path "data\BSR\$b" "$Date.csv"
    if (-not (Test-Path $p)) {
        $missing += $b
        Write-Host ("  {0,-16} ** MISSING **" -f $b) -ForegroundColor Red
        continue
    }

    $rows = @(Import-Csv $p)
    $n    = $rows.Count
    $cols = if ($n -gt 0) { $rows[0].PSObject.Properties.Name.Count } else { 0 }

    # blank / duplicate ASIN checks
    $asins = @($rows | ForEach-Object { $_.ASIN } | Where-Object { $_ -and $_.Trim() })
    $uniq  = @($asins | Select-Object -Unique).Count
    $dupes = $asins.Count - $uniq

    $flag = ""
    if ($n -eq 0)      { $flag = "  <-- EMPTY FILE" }
    elseif ($dupes -gt 0) { $flag = "  <-- $dupes DUPLICATE ASINs" }
    elseif ($cols -lt 12) { $flag = "  <-- only $cols cols, expected 13" }

    $colour = if ($flag) { "Yellow" } else { "Green" }
    Write-Host ("  {0,-16} rows={1,-5} cols={2,-3} unique={3}{4}" -f $b, $n, $cols, $uniq, $flag) -ForegroundColor $colour
    $rowInfo += "$b=$n"
}

Write-Host ""

if ($missing.Count -gt 0 -and -not $Force) {
    Write-Host "ABORT: no file for $Date -> $($missing -join ', ')" -ForegroundColor Red
    Write-Host "Drop the CSV into data\BSR\<brand>\$Date.csv and re-run." -ForegroundColor Yellow
    Write-Host "(or re-run with -Force to push the brands you do have)" -ForegroundColor DarkGray
    exit 1
}

# ---- 2. rebuild snapshots -----------------------------------
Write-Host "building snapshots..." -ForegroundColor Cyan
python Scripts\build_bsr_snapshots.py
if ($LASTEXITCODE -ne 0) { Write-Host "ABORT: build_bsr_snapshots.py failed" -ForegroundColor Red; exit 1 }

# confirm this date actually landed in the JSON
$snap = Get-Content "src\bsr_snapshots.json" -Raw
$iso  = "20" + $Date.Replace("-", "-")   # 26-07-31 -> 2026-07-31
if ($snap -notmatch [regex]::Escape($iso)) {
    Write-Host "ABORT: $iso is not in bsr_snapshots.json - filename date may be wrong" -ForegroundColor Red
    exit 1
}
Write-Host "  ok: $iso present in snapshots" -ForegroundColor Green

# ---- 3. vite build ------------------------------------------
Write-Host "npm run build..." -ForegroundColor Cyan
npm run build
if ($LASTEXITCODE -ne 0) { Write-Host "ABORT: npm run build failed" -ForegroundColor Red; exit 1 }

# ---- 4. stage ONLY what matters (avoids CRLF churn) ---------
git add "data/BSR/*/$Date.csv" "src/bsr_snapshots.json" "src/raw_data.json"

$staged = @(git diff --cached --name-only)
Write-Host ""
Write-Host "staged $($staged.Count) file(s):" -ForegroundColor Cyan
$staged | ForEach-Object { Write-Host "  $_" }

if ($staged.Count -eq 0) {
    Write-Host ""
    Write-Host "Nothing to commit - this data is already committed. Deploy is up to date." -ForegroundColor Yellow
    exit 0
}
if ($staged.Count -gt 20) {
    Write-Host ""
    Write-Host "ABORT: $($staged.Count) files staged - looks like line-ending churn." -ForegroundColor Red
    Write-Host "Run 'git reset' and check 'git status' before pushing." -ForegroundColor Yellow
    exit 1
}

# ---- 5. commit + push (Render auto-deploys on push) ---------
git commit -m "Add BSR snapshot 20$Date"
if ($LASTEXITCODE -ne 0) { Write-Host "ABORT: commit failed" -ForegroundColor Red; exit 1 }

git push
if ($LASTEXITCODE -ne 0) { Write-Host "ABORT: push failed - nothing deployed" -ForegroundColor Red; exit 1 }

Write-Host ""
Write-Host "=== PUSHED OK ===" -ForegroundColor Green
Write-Host "  $($rowInfo -join '  ')"
Write-Host "  commit: $(git rev-parse --short HEAD)"
Write-Host "  Render will auto-deploy. Check:" -ForegroundColor DarkGray
Write-Host "  https://dashboard.render.com/static/srv-d7ae959r0fns738eltqg/events" -ForegroundColor DarkGray
Write-Host ""
