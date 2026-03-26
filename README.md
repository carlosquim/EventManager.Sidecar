# EventManager.Sidecar

EventManager.Sidecar is a lightweight, independent .NET Core background service designed to run as a sidecar container in Kubernetes. It abstracts away event consumption and infrastructure bindings from your main application, routing events to it via HTTP.

This project was built to replace Azure Functions core dependencies, enabling true portability and compliance with enterprise security and scaling requirements.

## 🚀 Key Features

### 1. Universal Event Consumption
The sidecar connects to various Azure event sources and consumes events autonomously. The following processors are supported and can be toggled via configuration:
- **Cosmos DB Change Feed** (`EnableCosmosDb`)
- **Blob Storage Events** (`EnableBlobStorage`)
- **Blob Change Feed** (`EnableBlobChangeFeed`)
- **Event Grid** (`EnableEventGrid`)
- **SQL Triggers** (`EnableSql` - optional)
- **Azure WebJobs Storage** blobs processing

### 2. Dual-Entry & Event Forwarding
Instead of integrating SDKs or Function triggers directly into the main application, the Sidecar handles the events and forwards them using configurable dispatchers:
- **HTTP Forwarder:** Sends incoming events to the main container's HTTP endpoints (`MainAppTargetUrl`). Includes built-in transient fault handling and exponential backoff retries using **Polly**.
- **Service Bus Forwarder:** Events can also be routed out to Azure Service Bus if the sidecar is configured to act as an intermediary bus publisher.

### 3. KEDA Autoscaling Integration
Designed specifically for Kubernetes Event-driven Autoscaling (KEDA):
- Exposes a custom metrics API endpoint (`/metrics/cosmos-lag`) to report Cosmos DB Change Feed lag.
- Allows KEDA's `metrics-api` scaler to dynamically scale out the sidecar (and the main app pod) based on real-time processing demands, bypassing the limitations of unsupported external scalers.

### 4. Robust Distributed Lease Management
Incorporates a custom `IBlobLeaseManager` for distributed coordination across multiple sidecar replicas.
- **Lease Heartbeat Mechanism:** Ensures leases on blobs/events are kept alive during long-running processing tasks.
- Prevents split-brain scenarios and ensures at-least-once delivery semantics for safe parallel processing.

### 5. Enterprise Telemetry & Observability
Deep integration with **Azure Application Insights** and **OpenTelemetry**.
- Automatically exports Logs, Metrics, and Distributed Traces to Azure Monitor.
- Correlates sidecar operations with the main application requests for end-to-end visibility.

### 6. Agnostic & Portable
- Built as a standard ASP.NET Core application, removing the Azure Functions Host dependency.
- Easy to run locally, on Docker, or inside a Kubernetes Pod alongside any language/framework of the main application.

## ⚙️ Configuration

Configuration is driven through `appsettings.json`, `local.settings.json`, and environment variables. To enable a specific event processor, simply set its feature flag to `true` and provide the necessary connection strings.

```json
{
  "EnableCosmosDb": true,
  "EnableBlobStorage": false,
  "EnableEventGrid": false,
  "EnableBlobChangeFeed": true,
  "MainAppTargetUrl": "http://localhost:5000/",
  "APPLICATIONINSIGHTS_CONNECTION_STRING": "InstrumentationKey=...",
  "CosmosTriggers": [
    {
      "ConnectionEndpoint": "...",
      "DatabaseName": "SampleDB",
      "ContainerName": "SampleContainer",
      "LeaseContainerName": "leases",
      "TargetFunctionName": "CosmosTriggerFunction"
    }
  ]
}
```

## 🐳 Running Locally

```bash
# Build the application
dotnet build

# Run the Sidecar
dotnet run
```
You can also package it into a Docker image using the provided `Dockerfile`.

## 🏗️ Architecture Pattern

```
[ Azure Cosmos DB / Blob / Event Grid ]
                 │
                 ▼
 ┌───────────────────────────────┐
 │       KUBERNETES POD          │
 │                               │
 │  ┌─────────────────────────┐  │
 │  │   EventManager.Sidecar  │  │
 │  │   (Reads Events)        │  │
 │  └───────────┬─────────────┘  │
 │              │ HTTP POST      │
 │              ▼                │
 │  ┌─────────────────────────┐  │
 │  │        Main App         │  │
 │  │   (Processes Domain)    │  │
 │  └─────────────────────────┘  │
 └───────────────────────────────┘
```
