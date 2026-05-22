# Build the React SPA locally and copy the bundle into weekly_app/static/spa/
# so uvicorn can serve it.  Works around the Google-Drive EBADF issue that
# breaks `npm install`/`npm run build` when run directly against Drive paths.
#
# Usage (from FastAPI repo root):
#   pwsh scripts/build-spa-local.ps1
#
# Pre-reqs:
#   - C:\Users\Admin\weekly-frontend\ exists and has node_modules installed
#     (the mirror used for `npm run dev`).
#   - Source files in FastAPI/frontend/src have been mirrored to that path
#     already (see other scripts / the dev workflow).

$ErrorActionPreference = "Stop"

$mirror = "C:\Users\Admin\weekly-frontend"
$repoRoot = (Get-Location).Path
$target = Join-Path $repoRoot "weekly_app\static\spa"

if (-not (Test-Path $mirror)) {
    Write-Error "Local Vite mirror not found at $mirror"
    exit 1
}

Write-Host "Building SPA in $mirror ..." -ForegroundColor Cyan
Push-Location $mirror
try {
    $env:NODE_ENV = "production"
    npm run build
    if ($LASTEXITCODE -ne 0) {
        Write-Error "npm run build failed"
        exit 1
    }
} finally {
    Pop-Location
}

$dist = Join-Path $mirror "dist"
if (-not (Test-Path $dist)) {
    Write-Error "Build produced no dist/ directory at $dist"
    exit 1
}

Write-Host "Copying bundle to $target ..." -ForegroundColor Cyan
# Atomic-ish deploy: build into a sibling directory then swap.  Without this,
# the live SPA's hashed chunks 404 for the ~100ms between `rm` and `cp`, which
# breaks any browser session in mid-navigation (React.lazy fetches a chunk
# during that window and never recovers).
$staging = "$target.new"
$retired = "$target.old"
$leafName = [System.IO.Path]::GetFileName($target)

if (Test-Path $staging) { Remove-Item -Path $staging -Recurse -Force }
if (Test-Path $retired) { Remove-Item -Path $retired -Recurse -Force }

New-Item -ItemType Directory -Path $staging -Force | Out-Null
Copy-Item -Path "$dist\*" -Destination $staging -Recurse -Force

if (Test-Path $target) { Rename-Item -Path $target -NewName "$leafName.old" }
Rename-Item -Path $staging -NewName "$leafName"
if (Test-Path $retired) { Remove-Item -Path $retired -Recurse -Force }

Write-Host "SPA bundle ready at $target" -ForegroundColor Green
Write-Host ""
Write-Host "Next: from FastAPI root run -> uvicorn weekly_app.main:app --port 8000" -ForegroundColor Yellow
