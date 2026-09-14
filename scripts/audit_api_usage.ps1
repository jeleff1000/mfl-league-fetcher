$ErrorActionPreference = "Stop"

function RgLines {
  param(
    [string]$Pattern,
    [string]$Path,
    [string[]]$Globs = @("*.ts","*.tsx")
  )
  $globArgs = @()
  foreach ($g in $Globs) { $globArgs += @("-g", $g) }
  & rg -n $Pattern $Path @globArgs 2>$null
}

function RgFiles {
  param([string]$Path)
  & rg --files $Path 2>$null
}

$root = (Resolve-Path ".").Path
$apiFiles = @(RgFiles "frontend/src/app/api")
$apiLeagueFiles = @(RgFiles "frontend/src/app/api/league")

$callsiteLines = @(
  & rg -n -F -e "fetchJson<" -e "fetch(" -e "/api/league/" "frontend/src" -g "*.ts" -g "*.tsx" 2>$null
)
$endpointMatches = @()
foreach ($line in $callsiteLines) {
  if ($line -match '(/api/[^"\s\)]+)') {
    $endpointMatches += $Matches[1]
  }
}
$uniqueEndpoints = $endpointMatches | Sort-Object -Unique

$rawLeagueDb = @(RgLines "\$\{db\}\.public\.|%DB%\.public\.|leagueTable\(" "frontend/src/app/api/league" @("*.ts"))
$selectStar = @(RgLines "SELECT \\*" "frontend/src/app/api/league" @("*.ts"))
$highLimit = @(RgLines "LIMIT\\s+(1000|2000|5000|10000)" "frontend/src/app/api/league" @("*.ts"))

$md = @()
$tick = [char]96
$md += "# API Usage Audit"
$md += ""
$md += ("Generated: {0}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"))
$md += ""
$md += "## High-Level Counts"
$md += "- Total API route files: $($apiFiles.Count)"
$md += "- League API route files: $($apiLeagueFiles.Count)"
$md += "- Frontend fetch callsites (raw count): $($callsiteLines.Count)"
$md += "- Unique literal `/api/...` endpoints found in frontend: $($uniqueEndpoints.Count)"
$md += ""
$md += "## Unique /api Endpoints (literal strings)"
if ($uniqueEndpoints.Count -eq 0) {
  $md += "- None detected (strings are likely assembled dynamically)"
} else {
  foreach ($ep in $uniqueEndpoints) { $md += "- $tick$ep$tick" }
}
$md += ""
$md += "## Potential Raw League-DB References (review)"
if ($rawLeagueDb.Count -eq 0) {
  $md += "- None detected"
} else {
  foreach ($line in $rawLeagueDb) { $md += "- $tick$line$tick" }
}
$md += ""
$md += "## SELECT * Usage in League API (review)"
if ($selectStar.Count -eq 0) {
  $md += "- None detected"
} else {
  foreach ($line in $selectStar) { $md += "- $tick$line$tick" }
}
$md += ""
$md += "## High LIMIT Usage in League API (review)"
if ($highLimit.Count -eq 0) {
  $md += "- None detected"
} else {
  foreach ($line in $highLimit) { $md += "- $tick$line$tick" }
}
$md += ""
$md += "## Notes"
$md += "- This report is heuristic. Some SQL in `config.ts` is later rewritten by the query engine and may be scoped to centralized tables."
$md += "- Lines above are starting points for manual review of column/row usage and scoping."

$outPath = Join-Path $root "scripts\\audit_api_usage.md"
$md | Set-Content -Path $outPath -Encoding UTF8
$outPath
