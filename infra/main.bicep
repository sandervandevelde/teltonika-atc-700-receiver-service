// Azure Container Instances exposes the Teltonika UDP listener directly.
param location string = resourceGroup().location
@description('Globally unique ACR name')
param acrName string
@description('DNS label -> <label>.<region>.azurecontainer.io (no path)')
param dnsLabel string
param imageTag string = 'latest'
@description('Set false on first run to create only the registry')
param deployContainer bool = true
param udpPort int = 5027
param kafkaBootstrapServers string
param kafkaTopic string
@secure()
param kafkaConnectionString string

resource acr 'Microsoft.ContainerRegistry/registries@2023-07-01' = {
  name: acrName
  location: location
  sku: { name: 'Basic' }
  properties: { adminUserEnabled: true }
}

resource aci 'Microsoft.ContainerInstance/containerGroups@2023-05-01' = if (deployContainer) {
  name: 'atc700receiver'
  location: location
  properties: {
    osType: 'Linux'
    restartPolicy: 'Always'
    imageRegistryCredentials: [
      {
        server: acr.properties.loginServer
        username: acr.listCredentials().username
        password: acr.listCredentials().passwords[0].value
      }
    ]
    ipAddress: {
      type: 'Public'
      dnsNameLabel: dnsLabel
      ports: [ { protocol: 'UDP', port: udpPort } ]
    }
    containers: [
      {
        name: 'receiver'
        properties: {
          image: '${acr.properties.loginServer}/atc700receiver:${imageTag}'
          ports: [ { protocol: 'UDP', port: udpPort } ]
          environmentVariables: [
            { name: 'UDP_PORT', value: string(udpPort) }
            { name: 'KAFKA_BOOTSTRAP_SERVERS', value: kafkaBootstrapServers }
            { name: 'KAFKA_TOPIC', value: kafkaTopic }
            { name: 'KAFKA_SECURITY_PROTOCOL', value: 'SASL_SSL' }
            { name: 'KAFKA_SASL_MECHANISM', value: 'PLAIN' }
            { name: 'KAFKA_SASL_USERNAME', value: '$ConnectionString' }
            { name: 'KAFKA_SASL_PASSWORD', secureValue: kafkaConnectionString }
          ]
          resources: { requests: { cpu: 1, memoryInGB: 1 } }
        }
      }
    ]
  }
}

output hostname string = deployContainer ? aci!.properties.ipAddress.fqdn : ''
output acrLoginServer string = acr.properties.loginServer
