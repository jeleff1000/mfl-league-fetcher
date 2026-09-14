param(
    [int]$YearMin = 1869,
    [int]$YearMax = 2025,
    [string]$BaseOutDir = (Join-Path (Split-Path -Parent $PSScriptRoot) "tmp\cfb_all_years"),
    [int]$CdpPort = 9223,
    [int]$SleepSeconds = 4,
    [int]$StepTimeoutMinutes = 90,
    [int]$RetrySleepSeconds = 120,
    [int]$MaxPageErrors = 2,
    [int]$ZeroRowStopAfter = 100,
    [ValidateSet("auto", "requests", "cdp")]
    [string]$FetchMode = "auto",
    [switch]$IncludePlayers,
    [string[]]$PlayerPageKinds = @("main", "gamelog", "splits")
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$Scraper = Join-Path $RepoRoot "scripts\scrape_cfb_tables.py"
$PlayerSupervisor = Join-Path $RepoRoot "scripts\finish_cfb_players.ps1"
$LogDir = Join-Path $RepoRoot "tmp\cfb_year_range_logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$RunLog = Join-Path $LogDir ("cfb_year_range_{0}_{1}_{2}.log" -f $YearMin, $YearMax, (Get-Date -Format "yyyyMMdd_HHmmss"))

function Write-RunLog {
    param([string]$Message)
    $line = "{0} {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
    Add-Content -LiteralPath $RunLog -Value $line
}

function Ensure-CdpBrowser {
    param([int]$Port, [int]$Year)

    $versionUrl = "http://127.0.0.1:$Port/json/version"
    while ($true) {
        try {
            $null = Invoke-RestMethod -Uri $versionUrl -TimeoutSec 5
            return
        } catch {
            Write-RunLog "cdp unavailable port=$Port year=$Year; launching/checking Edge then sleeping ${RetrySleepSeconds}s"
            $edge = "C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"
            if (-not (Test-Path -LiteralPath $edge)) {
                $edge = "C:\Program Files\Microsoft\Edge\Application\msedge.exe"
            }
            if (Test-Path -LiteralPath $edge) {
                $profileDir = Join-Path $RepoRoot ("tmp\edge-cdp-{0}" -f $Port)
                New-Item -ItemType Directory -Force -Path $profileDir | Out-Null
                Start-Process -FilePath $edge -ArgumentList @(
                    "--remote-debugging-port=$Port",
                    "--remote-debugging-address=127.0.0.1",
                    "--user-data-dir=$profileDir",
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--disable-extensions",
                    "https://www.sports-reference.com/cfb/years/$Year.html"
                ) -ErrorAction SilentlyContinue
            }
            Start-Sleep -Seconds $RetrySleepSeconds
        }
    }
}

function Invoke-Step {
    param([string]$YearDir, [string]$Label, [string[]]$Arguments, [switch]$Soft)
    $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $log = Join-Path $YearDir "logs\$Label`_$stamp.log"
    $err = Join-Path $YearDir "logs\$Label`_$stamp.err.log"
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $log) | Out-Null
    Write-RunLog "starting $Label out=$YearDir log=$log"
    $proc = Start-Process -FilePath "python" -ArgumentList (@($Scraper) + $Arguments) -WorkingDirectory $RepoRoot -RedirectStandardOutput $log -RedirectStandardError $err -WindowStyle Hidden -PassThru
    $completed = $proc.WaitForExit($StepTimeoutMinutes * 60 * 1000)
    if (-not $completed) {
        Write-RunLog "$Label timed out after ${StepTimeoutMinutes}m; killing pid=$($proc.Id)"
        Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
        $code = 124
    } else {
        $code = $proc.ExitCode
    }
    if ($null -eq $code) {
        $code = if ((Test-Path -LiteralPath $err) -and ((Get-Item -LiteralPath $err).Length -gt 0)) { 1 } else { 0 }
    }
    Write-RunLog "$Label exited code=$code"
    if ((Test-Path -LiteralPath $err) -and ((Get-Item -LiteralPath $err).Length -gt 0)) {
        Write-RunLog "$Label stderr=$err"
    }
    if ($code -ne 0 -and -not $Soft) {
        throw "$Label failed with exit code $code; see $log"
    }
    return $code
}

function Invoke-FetchStep {
    param([string]$YearDir, [string]$Label, [string[]]$Arguments, [int]$Year)
    while ($true) {
        Ensure-CdpBrowser -Port $CdpPort -Year $Year
        $code = Invoke-Step -YearDir $YearDir -Label $Label -Arguments $Arguments -Soft
        if ($code -eq 0) {
            return
        }
        Write-RunLog "$Label failed code=$code; sleeping ${RetrySleepSeconds}s before retry"
        Start-Sleep -Seconds $RetrySleepSeconds
    }
}

function Invoke-CfbStatus {
    param([string]$YearDir)
    $json = & python $Scraper status --out-dir $YearDir
    if ($LASTEXITCODE -ne 0) {
        throw "status failed for $YearDir with exit code $LASTEXITCODE"
    }
    return ($json | ConvertFrom-Json)
}

function Get-StatusInt {
    param($Status, [string]$Name)
    if ($Status.PSObject.Properties.Name -contains $Name) {
        return [int]($Status.$Name)
    }
    return 0
}

function Get-DatasetDone {
    param($Status, [string]$Dataset)
    if ($Status.PSObject.Properties.Name -contains "datasets" -and
        $Status.datasets.PSObject.Properties.Name -contains $Dataset) {
        return [int]($Status.datasets.$Dataset.completed_pages)
    }
    return 0
}

function Test-NeedsIndex {
    param([string]$YearDir, $Status)
    foreach ($name in @("year_page_index.parquet", "team_index.parquet", "boxscore_index.parquet")) {
        if (-not (Test-Path -LiteralPath (Join-Path $YearDir $name))) {
            return $true
        }
    }
    $teams = Get-StatusInt -Status $Status -Name "teams"
    $boxscores = Get-StatusInt -Status $Status -Name "boxscores"
    return ($teams -le 0 -and $boxscores -le 0)
}

function Invoke-UntilComplete {
    param(
        [string]$YearDir,
        [string]$Label,
        [string[]]$Arguments,
        [int]$Year,
        [string]$Dataset,
        [string]$TargetProperty
    )

    while ($true) {
        $status = Invoke-CfbStatus -YearDir $YearDir
        $target = Get-StatusInt -Status $status -Name $TargetProperty
        $done = Get-DatasetDone -Status $status -Dataset $Dataset
        Write-RunLog "$Label status completed=$done/$target"
        if ($target -le 0 -or $done -ge $target) {
            return
        }

        $before = $done
        Ensure-CdpBrowser -Port $CdpPort -Year $Year
        $code = Invoke-Step -YearDir $YearDir -Label $Label -Arguments $Arguments -Soft
        $afterStatus = Invoke-CfbStatus -YearDir $YearDir
        $after = Get-DatasetDone -Status $afterStatus -Dataset $Dataset
        Write-RunLog "$Label after code=$code completed=$after/$target"
        if ($after -le $before) {
            Write-RunLog "$Label made no progress; before=$before after=$after code=$code; sleeping ${RetrySleepSeconds}s before retry"
            Start-Sleep -Seconds $RetrySleepSeconds
        } elseif ($after -lt $target) {
            Start-Sleep -Seconds 60
        }
    }
}

Write-RunLog "range started year_min=$YearMin year_max=$YearMax base=$BaseOutDir fetch_mode=$FetchMode include_players=$IncludePlayers"

foreach ($year in $YearMin..$YearMax) {
    $yearDir = Join-Path $BaseOutDir ("year={0}" -f $year)
    New-Item -ItemType Directory -Force -Path $yearDir | Out-Null
    Write-RunLog "year $year started"

    $fetch = @(
        "--out-dir", $yearDir,
        "--cdp-port", "$CdpPort",
        "--fetch-mode", "$FetchMode",
        "--sleep", "$SleepSeconds",
        "--challenge-retries", "2",
        "--challenge-sleep", "20",
        "--max-page-errors", "$MaxPageErrors",
        "--zero-row-stop-after", "$ZeroRowStopAfter",
        "--stop-on-error",
        "--skip-existing"
    )

    $status = Invoke-CfbStatus -YearDir $yearDir
    if (Test-NeedsIndex -YearDir $yearDir -Status $status) {
        Invoke-FetchStep -YearDir $yearDir -Label "build_index_$year" -Arguments (@("build-index", "--year", "$year") + $fetch) -Year $year
    } else {
        Write-RunLog "year $year index exists; skipping build-index"
    }

    Invoke-UntilComplete -YearDir $yearDir -Label "scrape_context_$year" -Arguments (@("scrape-pages", "--group", "context") + $fetch) -Year $year -Dataset "context" -TargetProperty "year_pages"
    Invoke-UntilComplete -YearDir $yearDir -Label "scrape_team_pages_$year" -Arguments (@("scrape-pages", "--group", "team") + $fetch) -Year $year -Dataset "team_pages" -TargetProperty "teams"
    Invoke-UntilComplete -YearDir $yearDir -Label "scrape_boxscores_$year" -Arguments (@("scrape-boxscores") + $fetch) -Year $year -Dataset "boxscores" -TargetProperty "boxscores"

    foreach ($dataset in @("context", "team_pages", "boxscores")) {
        Invoke-Step -YearDir $yearDir -Label "compact_$dataset`_$year" -Arguments @(
            "compact",
            "--out-dir", $yearDir,
            "--dataset", $dataset,
            "--delete-parts"
        ) -Soft | Out-Null
    }

    if ($IncludePlayers) {
        Invoke-Step -YearDir $yearDir -Label "build_player_index_$year" -Arguments @(
            "build-player-index",
            "--out-dir", $yearDir
        ) -Soft | Out-Null

        $playerArgs = @(
            "-OutDir", $yearDir,
            "-Year", "$year",
            "-CdpPort", "$CdpPort",
            "-SleepSeconds", "$SleepSeconds",
            "-PageKinds"
        ) + $PlayerPageKinds
        Write-RunLog "starting player supervisor for $year"
        & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $PlayerSupervisor @playerArgs
        if ($LASTEXITCODE -ne 0) {
            throw "player supervisor failed for $year with exit code $LASTEXITCODE"
        }
    }

    Invoke-Step -YearDir $yearDir -Label "status_$year" -Arguments @("status", "--out-dir", $yearDir) | Out-Null
    Write-RunLog "year $year finished"
}

Write-RunLog "range finished"
