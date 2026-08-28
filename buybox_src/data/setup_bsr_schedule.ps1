# ============================================================
# RUN THIS ONCE to schedule the daily 8:00 AM BSR run.
# Right-click > Run with PowerShell  (or run in a normal PowerShell window)
# ============================================================

$ScriptPath = Join-Path $PSScriptRoot "run_bsr_daily.ps1"
$TaskName   = "BSR Daily 8AM"

# Action: run PowerShell hidden, execute our pipeline script
$Action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$ScriptPath`""

# Trigger: every day at 08:00
$Trigger = New-ScheduledTaskTrigger -Daily -At 8:00AM

# If the PC was asleep/off at 8, run as soon as it's back on
$Settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -DontStopOnIdleEnd `
    -ExecutionTimeLimit (New-TimeSpan -Hours 3)

# Run as the current user, only when logged on (uses your git credentials)
Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Description "Fetch Keepa BSR, build snapshots, build site, commit & push." `
    -Force

Write-Host ""
Write-Host "Task '$TaskName' registered. It will run every day at 8:00 AM." -ForegroundColor Green
Write-Host "Test it now with:  Start-ScheduledTask -TaskName `"$TaskName`"" -ForegroundColor Yellow
