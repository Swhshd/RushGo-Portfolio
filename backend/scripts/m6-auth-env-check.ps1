param([string]$EnvFile = (Join-Path $PSScriptRoot '../.env'), [switch]$LocalOnly)
$ErrorActionPreference = 'Stop'
$keys = @('SMTP_HOST','SMTP_PORT','SMTP_TLS_MODE','SMTP_USERNAME','SMTP_PASSWORD','SMTP_FROM','AUTH_FRONTEND_ORIGIN')
$present = @{}
if (Test-Path -LiteralPath $EnvFile -PathType Leaf) {
    foreach ($line in [IO.File]::ReadAllLines((Resolve-Path -LiteralPath $EnvFile).Path)) {
        if ($line -match '^\s*(?:export\s+)?([A-Z_]+)\s*=\s*(.*)$' -and $keys -contains $Matches[1]) {
            $value = $Matches[2].Trim().Trim('"').Trim("'")
            $present[$Matches[1]] = -not [string]::IsNullOrWhiteSpace($value)
        }
    }
}
# No values escape this process. Effective container check executes the fixed allowlist only.
if (-not $LocalOnly) {
    $container = docker compose -f docker-compose.yml -f docker-compose.local.yml -f docker-compose.observability.yml ps -q user-service 2>$null
    if ($LASTEXITCODE -eq 0 -and $container) {
        foreach ($key in $keys) {
            $probe = 'if [ -n "$' + $key + '" ]; then printf SET; else printf NOT_SET; fi'
            $state = docker exec $container sh -c $probe 2>$null
            $present[$key] = $present[$key] -and ($LASTEXITCODE -eq 0 -and $state -eq 'SET')
        }
    }
}
foreach ($key in $keys) { $state = if ($present[$key]) { 'SET' } else { 'NOT_SET' }; "$key = $state" }
