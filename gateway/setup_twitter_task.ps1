# Setup Windows Task Scheduler for Twitter/X feed collection
# Run this script once as Administrator:
#   powershell -ExecutionPolicy Bypass -File gateway\setup_twitter_task.ps1
#
# The task starts at user logon and restarts on failure.

$TaskName = "AutoTrade-SPY Twitter Collection"
$Description = "Twitter/X feed collection from tech and trading influencers"
$BatchFile = "C:\Users\claw\auto-trade\autotrade-data\gateway\twitter_service.bat"
$WorkingDir = "C:\Users\claw\auto-trade\autotrade-data"

# Remove existing task if present
$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "Removing existing task '$TaskName'..."
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

# Trigger: at user logon
$Trigger = New-ScheduledTaskTrigger -AtLogOn

# Action: run the twitter service batch file
$Action = New-ScheduledTaskAction `
    -Execute $BatchFile `
    -WorkingDirectory $WorkingDir

# Settings:
#   - Allow on battery
#   - Don't stop on battery switch
#   - Restart on failure every 5 minutes (up to 288 times = 24 hours)
#   - Don't auto-terminate (service manages its own lifecycle)
#   - Don't start new instance if already running
$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RestartCount 288 `
    -RestartInterval (New-TimeSpan -Minutes 5) `
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
Write-Host "  Recovery: Retry every 5 min if service stops"
Write-Host "  Action:   $BatchFile"
Write-Host ""
Write-Host "To verify:  Get-ScheduledTask -TaskName '$TaskName'"
Write-Host "To run now: Start-ScheduledTask -TaskName '$TaskName'"
Write-Host "To remove:  Unregister-ScheduledTask -TaskName '$TaskName'"
