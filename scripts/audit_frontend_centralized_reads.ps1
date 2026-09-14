$repoRoot = Split-Path -Parent $PSScriptRoot
$routeRoot = Join-Path $repoRoot "frontend/src/app/api/league/[db]"

$checks = @(
    @{ Label = "late dbScope()"; Pattern = '\bdbScope\(' },
    @{ Label = 'per-league ${db}.public reference'; Pattern = '\$\{db\}\.public\.' },
    @{
        Label = "unscoped leagueTable read"
        Pattern = '\b(?:FROM|JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|INNER\s+JOIN|FULL\s+JOIN|CROSS\s+JOIN)\s+\$\{leagueTable\('
    }
)

$findings = @()
$files = Get-ChildItem -LiteralPath $routeRoot -Recurse -Filter *.ts | Where-Object { $_.Name -ne "config.ts" }

foreach ($file in $files) {
    $lines = Get-Content -LiteralPath $file.FullName
    for ($i = 0; $i -lt $lines.Length; $i++) {
        $line = $lines[$i]
        $trimmed = $line.Trim()
        if ($trimmed.StartsWith("//") -or $trimmed.StartsWith("*")) {
            continue
        }
        foreach ($check in $checks) {
            if ($line -match $check.Pattern) {
                $relativePath = $file.FullName.Substring($repoRoot.Length + 1).Replace('\', '/')
                $findings += [pscustomobject]@{
                    Path  = $relativePath
                    Line  = $i + 1
                    Label = $check.Label
                    Text  = $trimmed
                }
            }
        }
    }
}

if ($findings.Count -gt 0) {
    Write-Host "Found possible non-centralized frontend read paths:"
    foreach ($finding in $findings) {
        Write-Host ("{0}:{1}: {2}: {3}" -f $finding.Path, $finding.Line, $finding.Label, $finding.Text)
    }
    exit 1
}

Write-Host "Frontend centralized-read audit passed: no raw route/view read regressions found."
