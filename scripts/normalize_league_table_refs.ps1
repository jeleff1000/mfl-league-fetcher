$ErrorActionPreference = "Stop"

function EnsureScopedLeagueImport {
  param([string]$content)

  if ($content -notmatch "scopedLeagueTable") { return $content }

  if ($content -match 'from "@/lib/table-ref"') {
    return [regex]::Replace(
      $content,
      'import\s*\{\s*([^\}]+)\s*\}\s*from\s*"@/lib/table-ref";',
      {
        $imports = $args[0].Groups[1].Value.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ }
        if ($imports -contains "scopedLeagueTable") {
          return $args[0].Value
        }
        $newImports = ($imports + "scopedLeagueTable") -join ", "
        return 'import { ' + $newImports + ' } from "@/lib/table-ref";'
      },
      1
    )
  }

  $lines = $content -split "`r?`n"
  for ($i = 0; $i -lt $lines.Count; $i++) {
    if ($lines[$i] -match "^import\s") {
      $lines = $lines[0..$i] + 'import { scopedLeagueTable } from "@/lib/table-ref";' + $lines[($i+1)..($lines.Count-1)]
      break
    }
  }
  return ($lines -join "`n")
}

$files = & rg --files "frontend/src/app/api/league" -g "*.ts"
foreach ($file in $files) {
  $content = (Get-Content -LiteralPath $file | Out-String)
  $updated = $content

  $updated = $updated -replace '\$\{db\}\.public\.([A-Za-z0-9_]+)', '${scopedLeagueTable("$1", db)}'

  if ($updated -ne $content) {
    $updated = EnsureScopedLeagueImport $updated
    if ($updated -ne $content) {
      Set-Content -LiteralPath $file -Value $updated
    }
  }
}

"OK"
