# UDP Message Processing

The receiver listens for Teltonika UDP datagrams on `UDP_PORT` (default `5027`). Each received datagram increments the process-local `messageNumber` counter. The counter restarts from 1 when the receiver process restarts.

## Datagram and Codec Format

The Teltonika UDP channel header contains:

| Field | Size | Meaning |
| --- | ---: | --- |
| Length | 2 bytes | Number of bytes after this field |
| Packet ID | 2 bytes | Datagram ID copied into the acknowledgement |
| Not-usable byte | 1 byte | Must be `0x01` |
| AVL packet ID | 1 byte | Packet ID copied into the acknowledgement |
| IMEI length | 2 bytes | ATC700 IMEI length, expected to be 15 |
| IMEI | 15 bytes | ASCII device identifier |

The remaining payload is Codec 8 Extended (`0x8E`): record count, one or more AVL records, and a repeated record count. A single UDP datagram can therefore contain multiple AVL records. Each becomes an element of the JSON `records` array. Codec 8 Extended I/O counts and IDs are 2 bytes; variable-length I/O values carry their own length. The TCP AVL CRC trailer is not part of this UDP channel payload.

## Successful Message JSON

One successful datagram is logged as one compact JSON object. `ingestionTimestampUtc` is when the receiver accepted the datagram; each record's `timestampUtc` is the tracker timestamp. The `records` array groups all AVL records from the same datagram.

```json
{"messageNumber":48,"ingestionTimestampUtc":"2026-10-18T10:04:16.0622373Z","sender":"10.92.0.9:30661","byteCount":105,"decodeStatus":"success","decodeError":null,"rawPayloadHex":null,"imei":"359710000000001","recordCount":2,"alarmButton":"pressed","records":[{"recordNumber":1,"timestampUtc":"2026-10-18T10:04:15.871+00:00","priority":0,"latitude":48.8566,"longitude":2.3522,"altitude":35,"angle":90,"satellites":8,"speed":0,"eventIoId":236,"io":[{"id":236,"name":"alarm","valueHex":"01","value":true,"unit":null}]},{"recordNumber":2,"timestampUtc":"2026-10-18T10:04:16.871+00:00","priority":0,"latitude":48.85661,"longitude":2.35221,"altitude":35,"angle":90,"satellites":8,"speed":0,"eventIoId":0,"io":[]}]}
```

Fields:

| Field | Description |
| --- | --- |
| `messageNumber` | Monotonically increasing per-datagram counter for this process |
| `ingestionTimestampUtc` | UTC timestamp when the datagram was received |
| `sender` | UDP sender address and port |
| `byteCount` | Received datagram size |
| `decodeStatus` | `success` or `error` |
| `decodeError` | Error reason, or `null` after successful decoding |
| `rawPayloadHex` | Full datagram as hex on errors; otherwise `null` |
| `imei` | Device IMEI when parsed |
| `recordCount` | Number of decoded AVL records |
| `alarmButton` | `pressed`, `not pressed`, or `not reported` based on Alarm I/O 236 |
| `records` | AVL records carried by this datagram |

Each record contains its tracker timestamp, GPS fields, event I/O ID, and `io` array. Every I/O item includes its numeric `id`, descriptive `name` where known, original `valueHex`, decoded `value` where known, and `unit` where applicable.

## Known ATC700 I/O IDs

The receiver decodes these IDs using the ATC700 parameter table and Codec examples:

| AVL ID | JSON name | Decoded value |
| ---: | --- | --- |
| 16 | `totalOdometer` | Unsigned 4-byte value in meters |
| 21 | `gsmSignal` | Signal level, 0 to 5 |
| 67 | `batteryVoltage` | Millivolts |
| 68 | `batteryCurrent` | Milliamps |
| 69 | `gnssStatus` | `off`, `onWithFix`, `onWithoutFix`, or `sleeping` |
| 70 | `pcbTemperature` | Signed tenths of a degree Celsius |
| 113 | `batteryLevel` | Percent |
| 181 | `gnssPdop` | Raw value scaled by 0.1, in meters |
| 182 | `gnssHdop` | Raw value scaled by 0.1, in meters |
| 236 | `alarm` | Boolean; `true` means an alarm event occurred |
| 240 | `movement` | `onStop` or `moving` |

IDs without a confirmed mapping use `name: "unknown"`, `value: null`, and retain `valueHex` for later interpretation. The current mappings are implemented in `src/Atc700Receiver/UdpReceiverService.cs`.

## Errors and Acknowledgement

Malformed datagrams, unsupported codecs, and invalid records are logged as one warning-level JSON object with `decodeStatus: "error"`, a `decodeError`, and the original datagram in `rawPayloadHex`. Error messages are not acknowledged.

For a successfully decoded UDP message, the receiver sends the Teltonika 7-byte acknowledgement containing the original packet ID, AVL packet ID, and accepted record count.

The JSON log contains device IMEIs and location data. Limit access and retention accordingly.

## Deploy to Azure

The deployment uses Azure Container Instances (ACI) because the receiver needs an externally reachable UDP port. The [Bicep template](../infra/main.bicep) exposes UDP port `5027` and assigns a direct hostname. The [deployment script](../infra/deploy.ps1) creates or reuses the resource group, provisions an Azure Container Registry (ACR), builds the image, deploys the container, and prints the hostname.

The receiver forwards each successfully decoded JSON message to the Kafka endpoint before acknowledging the tracker. Kafka uses the IMEI as the record key so messages from one device stay on the same partition. The producer uses SASL over TLS (`SASL_SSL` with `PLAIN`) and requires broker persistence (`acks=all`). The receiver sends the Teltonika UDP acknowledgement only after Kafka confirms persistence. If Kafka delivery fails, it logs a forwarding error and withholds the UDP acknowledgement so the tracker can retry. This is at-least-once delivery: a tracker may resend if the Kafka write succeeded but its UDP acknowledgement was lost.

### Configure Kafka Credentials

The deploy script reads the connection string from `infra/local.parameters.json`. Create it from the placeholder template:

```powershell
Copy-Item .\infra\local.parameters.example.json .\infra\local.parameters.json
```

Edit `infra/local.parameters.json` locally and replace the placeholders with the **rotated** Kafka/Event Hubs connection string. A SAS key was previously exposed in chat; revoke/rotate that key and use only its replacement. Do not paste the replacement into chat.

The local file is excluded by `.gitignore`, but it contains the SAS secret in plaintext on disk. Restrict file access and never commit or upload it. The deployment script reads it as a secure Bicep parameter; Bicep passes it to ACI with `secureValue`. The ACR build uses only `src/Atc700Receiver` as its build context, so the local parameter file is not sent to the image builder.

The deployment script derives the Kafka host from the connection string's `Endpoint` (`<namespace>.servicebus.windows.net:9093`) and the topic from `EntityPath`. The container receives these variables:

| Variable | Value |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `<namespace>.servicebus.windows.net:9093` |
| `KAFKA_TOPIC` | Connection string `EntityPath` |
| `KAFKA_SECURITY_PROTOCOL` | `SASL_SSL` |
| `KAFKA_SASL_MECHANISM` | `PLAIN` |
| `KAFKA_SASL_USERNAME` | `$ConnectionString` |
| `KAFKA_SASL_PASSWORD` | Full rotated connection string; stored as an ACI secure environment variable |

Sign in and confirm the intended subscription before deploying:

```powershell
az login
az account show
```

From the repository root, deploy to the existing West Europe resource group:

```powershell
.\infra\deploy.ps1 `
	-ResourceGroup teltonika-atc700-weu-rg `
	-AcrName atc700rxweu20261017 `
	-DnsLabel teltonika-atc700-20261017 `
	-Location westeurope
```

The ACR name must be globally unique. The DNS label must be available in the selected region. The script prints the resulting hostname; for the current deployment it is `teltonika-atc700-20261017.westeurope.azurecontainer.io`.

Configure the ATC700 to send **UDP / Codec 8 Extended** to that hostname on port `5027`. To test the deployed endpoint, run the separate test client from the repository root:

```powershell
dotnet run --project tools/Atc700Receiver.TestClient -- teltonika-atc700-20261017.westeurope.azurecontainer.io 5027
```

The client should report that the receiver acknowledged two records. Inspect the receiver's runtime output with:

```powershell
az container logs --resource-group teltonika-atc700-weu-rg --name atc700receiver
```

The test client sends synthetic records, including an alarm event; it does not press the physical ATC700 alarm button. ACI and ACR are billable Azure resources.

## Sources

- [Teltonika Codec specification](https://wiki.teltonika-gps.com/view/Codec) — UDP channel framing, Codec 8 Extended record/I/O layout, and acknowledgements.
- [ATC700 Teltonika Data Sending Parameters ID](https://wiki.teltonika-gps.com/view/ATC700_Teltonika_Data_Sending_Parameters_ID) — ATC700 AVL I/O IDs, names, types, units, and alarm I/O 236.
- [ATC700 Input/output (I/O) configuration](https://wiki.teltonika-gps.com/view/ATC700_Input/output_(I/O)) — I/O priorities, event generation, and when I/O values are included in records.