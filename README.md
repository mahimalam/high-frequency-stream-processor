# High-Frequency Stream Processor

An enterprise-grade, low-latency real-time data stream processing engine built in TypeScript. Designed for sub-second WebSocket ingestion, statistical anomaly detection, and authenticated stream consumption.

## Architecture

- **Runtime:** Node.js v20 LTS with a non-blocking asynchronous event loop and NTP-synchronized timestamping
- **Transport:** Persistent WebSocket connection with exponential-backoff auto-reconnection
- **Auth:** HMAC-SHA256 signature generation for authenticated private stream endpoints
- **Analytics:** Rolling 60-sample window for real-time mean/standard-deviation rate tracking and 3σ anomaly detection

## Features

- `establishLowLatencyStream` — connects to any WebSocket endpoint (public or HMAC-authenticated)
- `parseStreamEvent` — typed frame parsing with null-safety for malformed frames
- `executeRiskCalculation` — inline statistical delta analysis per event (< 500µs budget)
- `rollingStats` — live event-rate mean and standard deviation
- Exponential-backoff reconnection (1s → 30s cap)

## Quick Start

```bash
npm install
npm start          # connects to Wikimedia public real-time stream (~1000 events/min)
```

For authenticated private streams, configure `.env`:

```env
STREAM_CLIENT_ID=your_client_id
STREAM_CLIENT_SECRET=your_client_secret
NODE_PORT=8080
METRICS_LEVEL=info   # or "debug" for verbose output
```

## Testing

```bash
npm test
```

Covers: HMAC signature generation, frame parsing, rolling statistics, connection lifecycle.

## CI

GitHub Actions runs on Node.js 20 and 22 with full TypeScript type-checking on every push.
