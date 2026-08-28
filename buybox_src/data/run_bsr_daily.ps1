# ============================================================
# Daily BSR pipeline — fetch, snapshot, build, commit, push
# Runs from Task Scheduler at 8:00 AM. Logs to logs\bsr_YYYY-MM-DD.log
# ============================================================

# Project root = parent of this script's folder (this file lives in <root>\data)
$ProjectDir = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectDir

# --- logging ---
$LogDir = Join-Path $ProjectDir "logs"
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }
$LogFile = Join-Path $LogDir ("bsr_" + (Get-Date -Format "yyyy-MM-dd") + ".log")

function Log($msg) {
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')  $msg"
    Write-Output $line
    Add-Content -Path $LogFile -Value $line
}

Log "=== BSR daily run started ==="

try {
    Log "Step 1/5: fetch_keepa_bsr.py"
    python Scripts/fetch_keepa_bsr.py 2>&1 | Tee-Object -FilePath $LogFile -Append
    if ($LASTEXITCODE -ne 0) { throw "fetch_keepa_bsr.py failed (exit $LASTEXITCODE)" }

    Log "Step 2/5: build_bsr_snapshots.py"
    python Scripts/build_bsr_snapshots.py 2>&1 | Tee-Object -FilePath $LogFile -Append
    if ($LASTEXITCODE -ne 0) { throw "build_bsr_snapshots.py failed (exit $LASTEXITCODE)" }

    Log "Step 3/5: npm run build"
    npm run build 2>&1 | Tee-Object -FilePath $LogFile -Append
    if ($LASTEXITCODE -ne 0) { throw "npm run build failed (exit $LASTEXITCODE)" }

    Log "Step 4/5: git add + commit"
    git add . 2>&1 | Tee-Object -FilePath $LogFile -Append
    $stamp = Get-Date -Format "yyyy-MM-dd"
    git commit -m "Add BSR snapshot $stamp" 2>&1 | Tee-Object -FilePath $LogFile -Append
    # commit returns non-zero if nothing changed — that's OK, don't fail the run

    Log "Step 5/5: git push"
    git push 2>&1 | Tee-Object -FilePath $LogFile -Append
    if ($LASTEXITCODE -ne 0) { throw "git push failed (exit $LASTEXITCODE)" }

    Log "=== BSR daily run FINISHED OK ==="
}
catch {
    Log "!!! ERROR: $_"
    Log "=== BSR daily run FAILED ==="
    exit 1
}
