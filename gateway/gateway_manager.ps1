<#
.SYNOPSIS
    IB Gateway lifecycle manager — check, start, wait, verify.

.DESCRIPTION
    Ensures IB Gateway is running via IBC. Handles:
    - Status check (process + port)
    - Start Gateway if not running
    - Wait for API port to accept connections
    - Stop/restart Gateway
    - Optionally run daily data fetch after startup

.EXAMPLE
    .\gateway_manager.ps1 status
    .\gateway_manager.ps1 start
    .\gateway_manager.ps1 start -Fetch
    .\gateway_manager.ps1 start -Fetch -FetchDays 7
    .\gateway_manager.ps1 stop
    .\gateway_manager.ps1 restart
#>

param(
    [Parameter(Position = 0, Mandatory = $true)]
    [ValidateSet("status", "start", "stop", "restart")]
    [string]$Command,

    [switch]$Fetch,
    [int]$FetchDays = 3,
    [int]$Timeout = 120,
    [int]$GracePeriod = 30,
    [string]$LogLevel = "INFO",
    [switch]$Docker
)

# --- Configuration ---
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $PSCommandPath)
$GatewayDir = Split-Path -Parent $PSCommandPath
$StartScript = Join-Path $GatewayDir "start_gateway.bat"
$LogDir = Join-Path $ProjectRoot "data\logs"
$LogFile = Join-Path $LogDir "gateway_manager.log"
$Host_ = "127.0.0.1"
$Port = 7497
$PollInterval = 5

# Try to read port from config
try {
    $ConfigFile = Join-Path $ProjectRoot "configs\default.yaml"
    if (Test-Path $ConfigFile) {
        $content = Get-Content $ConfigFile -Raw
        if ($content -match 'ib_port:\s*(\d+)') {
            $Port = [int]$Matches[1]
        }
    }
} catch { }

# --- Logging ---
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir -Force | Out-Null }

function Write-Log {
    param([string]$Level, [string]$Message)
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "$ts $($Level.PadRight(5)) gateway_manager: $Message"
    Write-Host $line
    Add-Content -Path $LogFile -Value $line -ErrorAction SilentlyContinue
}

# --- Port Check ---
function Test-PortOpen {
    param([string]$H, [int]$P, [int]$TimeoutMs = 3000)
    try {
        $tcp = New-Object System.Net.Sockets.TcpClient
        $result = $tcp.BeginConnect($H, $P, $null, $null)
        $wait = $result.AsyncWaitHandle.WaitOne($TimeoutMs, $false)
        if ($wait -and $tcp.Connected) {
            $tcp.Close()
            return $true
        }
        $tcp.Close()
        return $false
    } catch {
        return $false
    }
}

# --- Process Detection ---
function Find-GatewayProcess {
    # Look for java processes with ibgateway in the command line
    $procs = Get-CimInstance Win32_Process -Filter "Name='java.exe'" -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -match 'ibgateway|ibcalpha' }
    if ($procs) {
        return ($procs | Select-Object -First 1).ProcessId
    }
    return $null
}

# --- Status ---
function Get-GatewayStatus {
    $pid_ = Find-GatewayProcess
    $portOpen = Test-PortOpen -H $Host_ -P $Port
    return @{
        Running   = ($null -ne $pid_)
        PID       = $pid_
        PortOpen  = $portOpen
        Ready     = ($null -ne $pid_) -and $portOpen
        Host      = $Host_
        Port      = $Port
    }
}

# --- Start ---
function Start-Gateway {
    if (-not (Test-Path $StartScript)) {
        Write-Log "ERROR" "Start script not found: $StartScript"
        return $false
    }
    Write-Log "INFO" "Launching IB Gateway via start_gateway.bat"
    try {
        Start-Process -FilePath "cmd.exe" -ArgumentList "/c", "`"$StartScript`"" `
            -WorkingDirectory $ProjectRoot -WindowStyle Minimized
        return $true
    } catch {
        Write-Log "ERROR" "Failed to launch Gateway: $_"
        return $false
    }
}

# --- Stop ---
function Stop-Gateway {
    $pid_ = Find-GatewayProcess
    if ($null -eq $pid_) {
        Write-Log "INFO" "No Gateway process found"
        return $true
    }
    Write-Log "INFO" "Stopping Gateway process (PID $pid_)"
    try {
        Stop-Process -Id $pid_ -Force -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 3
        $check = Find-GatewayProcess
        if ($null -eq $check) {
            Write-Log "INFO" "Gateway stopped successfully"
            return $true
        }
        Write-Log "WARN" "Gateway process still running after stop attempt"
        return $false
    } catch {
        Write-Log "ERROR" "Error stopping Gateway: $_"
        return $false
    }
}

# --- Wait for Ready ---
function Wait-GatewayReady {
    param([int]$TimeoutSec, [int]$Grace = 0)
    Write-Log "INFO" ("Waiting for Gateway on " + $Host_ + ":" + $Port + " (timeout " + $TimeoutSec + "s)...")

    if ($Grace -gt 0) {
        Write-Log "INFO" ("Startup grace period: " + $Grace + "s")
        Start-Sleep -Seconds $Grace
    }

    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        if (Test-PortOpen -H $Host_ -P $Port) {
            Write-Log "INFO" ("Gateway is ready - accepting connections on " + $Host_ + ":" + $Port)
            return $true
        }
        $remaining = [int]($deadline - (Get-Date)).TotalSeconds
        if ($remaining -gt 0) {
            Write-Log "DEBUG" ("Port not open, retrying in " + $PollInterval + "s... (" + $remaining + "s remaining)")
            Start-Sleep -Seconds $PollInterval
        }
    }
    Write-Log "ERROR" ("Gateway did not become ready within " + $TimeoutSec + "s")
    return $false
}

# --- Ensure Running ---
function Ensure-GatewayRunning {
    $status = Get-GatewayStatus
    if ($status.Ready) {
        Write-Log "INFO" "Gateway already running (PID $($status.PID)) and accepting connections"
        return $true
    }
    if ($status.Running -and -not $status.PortOpen) {
        Write-Log "INFO" "Gateway process found (PID $($status.PID)) but port not open, waiting..."
        return Wait-GatewayReady -TimeoutSec $Timeout -Grace 0
    }
    # Need to start
    Write-Log "INFO" "Gateway is not running, starting..."
    if (-not (Start-Gateway)) { return $false }
    return Wait-GatewayReady -TimeoutSec $Timeout -Grace $GracePeriod
}

# --- Daily Fetch ---
function Invoke-DailyFetch {
    param([int]$Days = 3)
    $fetchScript = Join-Path $GatewayDir "daily_fetch.py"
    Write-Log "INFO" ("Running daily fetch (days=" + $Days + ", skip-gateway-check)...")
    Push-Location $ProjectRoot
    try {
        & uv run python $fetchScript --days $Days --skip-gateway-check
        return $LASTEXITCODE
    } finally {
        Pop-Location
    }
}

# --- Main ---
switch ($Command) {
    "status" {
        $s = Get-GatewayStatus
        if ($s.Ready) {
            Write-Host "READY - Gateway running (PID $($s.PID)), port $($s.Port) open" -ForegroundColor Green
            exit 0
        } elseif ($s.Running) {
            Write-Host "STARTING - Gateway running (PID $($s.PID)), port $($s.Port) not yet open" -ForegroundColor Yellow
            exit 1
        } else {
            Write-Host "STOPPED - No Gateway process, port $($s.Port) closed" -ForegroundColor Red
            exit 1
        }
    }
    "start" {
        $ok = Ensure-GatewayRunning
        if (-not $ok) {
            Write-Log "ERROR" "Failed to start Gateway"
            exit 1
        }
        if ($Fetch) {
            $rc = Invoke-DailyFetch -Days $FetchDays
            exit $rc
        }
        exit 0
    }
    "stop" {
        $ok = Stop-Gateway
        exit ([int](-not $ok))
    }
    "restart" {
        Stop-Gateway | Out-Null
        Start-Sleep -Seconds 5
        $ok = Ensure-GatewayRunning
        exit ([int](-not $ok))
    }
}
