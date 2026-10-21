param(
    [Parameter(Mandatory)] [string] $ResourceGroup,
    [Parameter(Mandatory)] [string] $AcrName,
    [Parameter(Mandatory)] [string] $DnsLabel,
    [ValidateRange(1, 65535)] [int] $UdpPort = 5027,
    [ValidatePattern('^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$')]
    [string] $ImageTag = [DateTime]::UtcNow.ToString('yyyyMMddHHmmssfff'),
    [string] $KafkaParametersFile = 'local.parameters.json',
    [string] $Location = 'westeurope'
)
$ErrorActionPreference = 'Stop'

function Invoke-AzureCli {
    param([string[]] $Arguments)

    & az @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Azure CLI command '$($Arguments[0])' failed with exit code $LASTEXITCODE."
    }
}

$root = Split-Path $PSScriptRoot -Parent
$parametersFile = if ([System.IO.Path]::IsPathRooted($KafkaParametersFile)) {
    $KafkaParametersFile
} else {
    Join-Path $PSScriptRoot $KafkaParametersFile
}
$deploymentFile = Join-Path $PSScriptRoot 'main.bicep'
$buildContext = Join-Path $root 'src\Atc700Receiver'

if (-not (Test-Path $parametersFile)) {
    throw "Missing $parametersFile. Copy local.parameters.example.json to local.parameters.json and set the rotated Kafka connection string."
}

$parametersDocument = Get-Content $parametersFile -Raw | ConvertFrom-Json
$connectionString = $parametersDocument.parameters.kafkaConnectionString.value
if ([string]::IsNullOrWhiteSpace($connectionString) -or $connectionString.Contains('<')) {
    throw 'Set a rotated Kafka connection string in infra/local.parameters.json before deploying.'
}

$connectionProperties = @{}
foreach ($segment in $connectionString -split ';') {
    $separator = $segment.IndexOf('=')
    if ($separator -gt 0) {
        $connectionProperties[$segment.Substring(0, $separator)] = $segment.Substring($separator + 1)
    }
}

$endpoint = $connectionProperties['Endpoint']
if ([string]::IsNullOrWhiteSpace($endpoint)) {
    throw 'The Kafka connection string must contain an Endpoint.'
}
$endpointMatch = [regex]::Match($endpoint, '^sb://(?<host>[^/]+)/?$', 'IgnoreCase')
$kafkaTopic = $connectionProperties['EntityPath']
if (-not $endpointMatch.Success -or [string]::IsNullOrWhiteSpace($kafkaTopic)) {
    throw 'The Kafka connection string must contain an sb:// Endpoint and an EntityPath.'
}
$kafkaBootstrapHost = $endpointMatch.Groups['host'].Value
$baseParameters = @(
    "@$parametersFile"
    "acrName=$AcrName"
    "dnsLabel=$DnsLabel"
    "location=$Location"
    "kafkaBootstrapHost=$kafkaBootstrapHost"
    "kafkaTopic=$kafkaTopic"
)
$registryParameters = $baseParameters + @('deployContainer=false')
$containerParameters = $baseParameters + @('deployContainer=true', "imageTag=$ImageTag", "udpPort=$UdpPort")

Invoke-AzureCli -Arguments @('group', 'create', '-n', $ResourceGroup, '-l', $Location) | Out-Null
$registryWhatIfArgs = @('deployment', 'group', 'what-if', '--resource-group', $ResourceGroup,
    '--template-file', $deploymentFile, '--parameters') + $registryParameters
Invoke-AzureCli -Arguments $registryWhatIfArgs
$registryDeployArgs = @('deployment', 'group', 'create', '--resource-group', $ResourceGroup,
    '--template-file', $deploymentFile, '--parameters') + $registryParameters
Invoke-AzureCli -Arguments $registryDeployArgs | Out-Null

Invoke-AzureCli -Arguments @('acr', 'build', '--registry', $AcrName, '--image', "atc700receiver:$ImageTag", $buildContext)

$containerWhatIfArgs = @('deployment', 'group', 'what-if', '--resource-group', $ResourceGroup,
    '--template-file', $deploymentFile, '--parameters') + $containerParameters
Invoke-AzureCli -Arguments $containerWhatIfArgs
$containerDeployArgs = @('deployment', 'group', 'create', '--resource-group', $ResourceGroup,
    '--template-file', $deploymentFile, '--parameters') + $containerParameters + @(
    '--query', 'properties.outputs.hostname.value', '--output', 'tsv')
Invoke-AzureCli -Arguments $containerDeployArgs
