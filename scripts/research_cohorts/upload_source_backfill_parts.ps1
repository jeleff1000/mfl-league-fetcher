param(
    [string]$ReleaseTag = 'research-source-backfill-v1',
    [string]$ReleaseRepo = 'jeleff1000/mfl-league-fetcher',
    [string]$PartsDir = 'D:\league-history-data\fantasy_leagues\sampling_corpus\source_backfill_release_v5',
    [int]$PartCount = 15
)

$uploaded = Join-Path $PartsDir '.uploaded'
New-Item -ItemType Directory -Force -Path $uploaded | Out-Null
while ($true) {
    $didWork = $false
    for ($i = 0; $i -lt $PartCount; $i++) {
        $name = 'research-source-backfill-v1.tar.part-{0:D2}' -f $i
        $asset = Join-Path $PartsDir $name
        $marker = Join-Path $uploaded ($name + '.ok')
        if ((Test-Path $asset) -and -not (Test-Path $marker)) {
            $a = Get-Item -LiteralPath $asset
            Start-Sleep -Seconds 5
            $b = Get-Item -LiteralPath $asset
            if ($a.Length -eq $b.Length -and $b.Length -gt 0) {
                gh release upload $ReleaseTag $asset --repo $ReleaseRepo --clobber
                if ($LASTEXITCODE -eq 0) {
                    Set-Content -LiteralPath $marker -Value 'uploaded'
                    $didWork = $true
                }
            }
        }
    }
    $complete = (Get-ChildItem -LiteralPath $uploaded -Filter '*.ok' -File -ErrorAction SilentlyContinue).Count -ge $PartCount
    if ($complete) { break }
    if (-not $didWork) { Start-Sleep -Seconds 30 }
}
