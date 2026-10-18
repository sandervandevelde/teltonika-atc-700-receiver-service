param(
    [Parameter(Mandatory)] [string] $ResourceGroup,
    [Parameter(Mandatory)] [string] $AcrName,
    [Parameter(Mandatory)] [string] $DnsLabel,
    [string] $Location = 'westeurope'
)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
$parametersFile = Join-Path $PSScriptRoot 'local.parameters.json'
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
$kafkaBootstrapServers = '{0}:9093' -f $endpointMatch.Groups['host'].Value
$baseParameters = @(
    "@$parametersFile"
    "acrName=$AcrName"
    "dnsLabel=$DnsLabel"
    "location=$Location"
    "kafkaBootstrapServers=$kafkaBootstrapServers"
    "kafkaTopic=$kafkaTopic"
)
$registryParameters = $baseParameters + @('deployContainer=false')
$containerParameters = $baseParameters + @('deployContainer=true', 'imageTag=latest', "udpPort=5027")

az group create -n $ResourceGroup -l $Location | Out-Null
$registryWhatIfArgs = @('deployment', 'group', 'what-if', '--resource-group', $ResourceGroup,
    '--template-file', $deploymentFile, '--parameters') + $registryParameters
az @registryWhatIfArgs | Out-Null
$registryDeployArgs = @('deployment', 'group', 'create', '--resource-group', $ResourceGroup,
    '--template-file', $deploymentFile, '--parameters') + $registryParameters
az @registryDeployArgs | Out-Null

az acr build --registry $AcrName --image atc700receiver:latest $buildContext

$containerWhatIfArgs = @('deployment', 'group', 'what-if', '--resource-group', $ResourceGroup,
    '--template-file', $deploymentFile, '--parameters') + $containerParameters
az @containerWhatIfArgs | Out-Null
$containerDeployArgs = @('deployment', 'group', 'create', '--resource-group', $ResourceGroup,
    '--template-file', $deploymentFile, '--parameters') + $containerParameters + @(
    '--query', 'properties.outputs.hostname.value', '--output', 'tsv')
az @containerDeployArgs
