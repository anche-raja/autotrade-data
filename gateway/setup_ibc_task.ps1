# Setup Windows Task Scheduler for IB Gateway auto-start via IBC
# ================================================================
# Run once as Administrator:
#   powershell -ExecutionPolicy Bypass -File gateway\setup_ibc_task.ps1
#
# This creates a task that:
#   - Starts IB Gateway at user logon
#   - Retries every 10 minutes if Gateway crashes
#   - Does NOT start a duplicate if already running

$TaskName = "AutoTrade-SPY IB Gateway"
$Description = "Start IB Gateway via IBC for automated data fetching"
$BatchFile = "C:\Users\claw\autotrade-data\gateway\start_gateway.bat"
$WorkingDir = "C:\Users\claw\autotrade-data"

# Remove existing task if present
$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "Removing existing task '$TaskName'..."
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

# Trigger: at user logon
$Trigger = New-ScheduledTaskTrigger -AtLogOn

# Action: run the gateway start script with /INLINE for Task Scheduler
$Action = New-ScheduledTaskAction `
    -Execute $BatchFile `
    -Argument "/INLINE" `
    -WorkingDirectory $WorkingDir

# Settings:
#   - Allow on battery
#   - Don't stop on battery switch
#   - Restart on failure every 10 minutes (up to 144 times = 24 hours)
#   - Don't auto-terminate (Gateway runs continuously)
#   - Don't start new instance if already running
$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RestartCount 144 `
    -RestartInterval (New-TimeSpan -Minutes 10) `
    -ExecutionTimeLimit (New-TimeSpan -Days 0) `
    -MultipleInstances IgnoreNew

# Register the task
Register-ScheduledTask `
    -TaskName $TaskName `
    -Description $Description `
    -Trigger $Trigger `
    -Action $Action `
    -Settings $Settings

Write-Host ""
Write-Host "Scheduled task '$TaskName' created successfully!" -ForegroundColor Green
Write-Host "  Trigger:  At user logon"
Write-Host "  Recovery: Retry every 10 min if Gateway stops"
Write-Host "  Action:   $BatchFile /INLINE"
Write-Host ""
Write-Host "To verify:  Get-ScheduledTask -TaskName '$TaskName'"
Write-Host "To run now: Start-ScheduledTask -TaskName '$TaskName'"
Write-Host "To remove:  Unregister-ScheduledTask -TaskName '$TaskName'"
