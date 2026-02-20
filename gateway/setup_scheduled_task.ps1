# Setup Windows Task Scheduler for daily IBKR data fetch
# Run this script once as Administrator:
#   powershell -ExecutionPolicy Bypass -File gateway\setup_scheduled_task.ps1
#
# The task will run daily at 5:00 PM ET (after market close).
# The daily_fetch.py script will wait for IB Gateway to be available
# before starting (up to 5 minutes), so Gateway can still be restarting.

$TaskName = "AutoTrade-SPY Daily Fetch"
$Description = "Fetch daily IBKR market data (1min, 5sec bars + news) after market close"
$BatchFile = "C:\Users\claw\auto-trade\autotrade-data\gateway\daily_fetch.bat"
$WorkingDir = "C:\Users\claw\auto-trade\autotrade-data"

# Remove existing task if present
$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "Removing existing task '$TaskName'..."
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

# Trigger: daily at 5:00 PM (Eastern Time — local time on this machine)
$Trigger = New-ScheduledTaskTrigger -Daily -At "17:00"

# Action: run the batch file
$Action = New-ScheduledTaskAction `
    -Execute $BatchFile `
    -WorkingDirectory $WorkingDir

# Settings: allow task to run on battery, don't stop on battery, retry on failure
$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RestartCount 2 `
    -RestartInterval (New-TimeSpan -Minutes 10) `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2)

# Register the task (runs as current user)
Register-ScheduledTask `
    -TaskName $TaskName `
    -Description $Description `
    -Trigger $Trigger `
    -Action $Action `
    -Settings $Settings

Write-Host ""
Write-Host "Scheduled task '$TaskName' created successfully!" -ForegroundColor Green
Write-Host "  Schedule: Daily at 5:00 PM"
Write-Host "  Action:   $BatchFile"
Write-Host ""
Write-Host "To verify: Get-ScheduledTask -TaskName '$TaskName'"
Write-Host "To run now: Start-ScheduledTask -TaskName '$TaskName'"
Write-Host "To remove:  Unregister-ScheduledTask -TaskName '$TaskName'"
