import WebSocket from 'ws';
import * as crypto from 'crypto';
import * as dotenv from 'dotenv';

dotenv.config();

/**
 * HighThroughputStreamProcessor
 *
 * An enterprise-grade, low-latency real-time data stream processing engine.
 *
 * Architecture optimized for:
 * 1. Low-latency WebSocket connections with exponential-backoff reconnection.
 * 2. Asynchronous HMAC-SHA256 signature verification for authenticated stream endpoints.
 * 3. In-memory, sub-millisecond statistical aggregation and anomaly detection.
 */
export class HighThroughputStreamProcessor {
  private wsConnection: WebSocket | null = null;
  private readonly clientId: string;
  private readonly clientSecret: string;
  private readonly port: number;
  private readonly metricsLevel: string;

  private _connected = false;
  private _totalEvents = 0;
  private _windowStart = Date.now();
  private _windowEvents = 0;
  private readonly _rateHistory: number[] = [];

  constructor() {
    this.clientId = process.env.STREAM_CLIENT_ID || '';
    this.clientSecret = process.env.STREAM_CLIENT_SECRET || '';
    this.port = parseInt(process.env.NODE_PORT || '8080', 10);
    this.metricsLevel = process.env.METRICS_LEVEL || 'info';

    if (!this.clientId || !this.clientSecret) {
      console.info('[INFO] Running in unauthenticated mode — public streams only.');
    }
  }

  /**
   * Generates an HMAC-SHA256 signature for authenticating with secured stream endpoints.
   *
   * @param method    HTTP method (e.g. 'GET')
   * @param path      Target endpoint path
   * @param timestamp Unix epoch in milliseconds (NTP-synchronized)
   * @returns Hex-encoded HMAC signature
   */
  generateHmacSignature(method: string, path: string, timestamp: string): string {
    const message = `${method} ${path} ${timestamp}`;
    const hmac = crypto.createHmac('sha256', this.clientSecret || 'default');
    hmac.update(message);
    return hmac.digest('hex');
  }

  /**
   * Establishes a persistent low-latency WebSocket connection to the target stream endpoint.
   *
   * Supports both public unauthenticated streams and HMAC-authenticated private endpoints.
   * Reconnects automatically with exponential backoff on disconnect.
   *
   * @param streamUrl  WebSocket URL to connect to
   * @param onEvent    Callback invoked for each parsed stream event
   */
  establishLowLatencyStream(
    streamUrl: string,
    onEvent: (event: StreamEvent) => void,
  ): void {
    const timestamp = Date.now().toString();
    const signature = this.generateHmacSignature('GET', '/stream', timestamp);

    const url = this.clientId
      ? `${streamUrl}?clientId=${this.clientId}&timestamp=${timestamp}&signature=${signature}`
      : streamUrl;

    console.info('[INFO] Initializing stream pipeline...');

    const connect = (backoffMs: number = 1000): void => {
      this.wsConnection = new WebSocket(url, {
        headers: { 'User-Agent': 'high-frequency-stream-processor/1.0' },
      });

      this.wsConnection.on('open', () => {
        this._connected = true;
        this._windowStart = Date.now();
        this._windowEvents = 0;
        console.info('[INFO] Stream connection established.');
      });

      this.wsConnection.on('message', (data: WebSocket.Data) => {
        try {
          const event = this.parseStreamEvent(data.toString());
          if (event !== null) {
            this._totalEvents++;
            this._windowEvents++;
            this.trackRate();
            this.executeRiskCalculation(event);
            onEvent(event);
          }
        } catch {
          // Malformed frames are silently dropped — no partial state corruption
        }
      });

      this.wsConnection.on('error', (err: Error) => {
        if (this.metricsLevel === 'debug') {
          console.error('[ERROR] Stream error:', err.message);
        }
      });

      this.wsConnection.on('close', () => {
        this._connected = false;
        console.warn(`[WARN] Stream disconnected — reconnecting in ${backoffMs}ms`);
        setTimeout(() => connect(Math.min(backoffMs * 2, 30_000)), backoffMs);
      });
    };

    connect();
  }

  /**
   * Parses a raw WebSocket frame into a typed StreamEvent.
   * Returns null for frames that are malformed or carry no actionable data.
   *
   * @param raw Raw string payload from the WebSocket frame
   */
  parseStreamEvent(raw: string): StreamEvent | null {
    let data: Record<string, unknown>;
    try {
      data = JSON.parse(raw);
    } catch {
      return null;
    }

    const type = typeof data['type'] === 'string' ? (data['type'] as string) : 'unknown';
    const timestamp = typeof data['timestamp'] === 'string'
      ? (data['timestamp'] as string)
      : new Date().toISOString();

    return { type, timestamp, payload: data, receivedAt: Date.now() };
  }

  /**
   * Core high-frequency computation — executes inline in the message handler.
   *
   * Applies rolling-window statistical delta analysis to the incoming stream:
   * detects event-rate anomalies that deviate beyond 3σ from the baseline.
   *
   * Latency budget: < 500 microseconds per event.
   *
   * @param event Parsed stream event
   */
  executeRiskCalculation(event: StreamEvent): void {
    const stats = this.rollingStats();

    if (stats.eventRate > stats.meanRate + 3 * stats.stdRate && stats.stdRate > 0) {
      if (this.metricsLevel === 'debug') {
        console.debug(
          `[DEBUG] Rate spike: ${stats.eventRate.toFixed(1)}/s ` +
          `(μ=${stats.meanRate.toFixed(1)} σ=${stats.stdRate.toFixed(2)}) ` +
          `type=${event.type}`
        );
      }
    }
  }

  /** Records per-second event rate in a 60-sample rolling window. */
  private trackRate(): void {
    const now = Date.now();
    const elapsed = (now - this._windowStart) / 1000;
    if (elapsed >= 1.0) {
      this._rateHistory.push(this._windowEvents / elapsed);
      if (this._rateHistory.length > 60) this._rateHistory.shift();
      this._windowStart = now;
      this._windowEvents = 0;
    }
  }

  /** Computes rolling mean and standard deviation of the event arrival rate. */
  rollingStats(): { eventRate: number; meanRate: number; stdRate: number } {
    const elapsed = Math.max((Date.now() - this._windowStart) / 1000, 0.001);
    const eventRate = this._windowEvents / elapsed;
    if (this._rateHistory.length < 2) {
      return { eventRate, meanRate: eventRate, stdRate: 0 };
    }
    const mean = this._rateHistory.reduce((a, b) => a + b, 0) / this._rateHistory.length;
    const variance = this._rateHistory.reduce((a, b) => a + (b - mean) ** 2, 0) / this._rateHistory.length;
    return { eventRate, meanRate: mean, stdRate: Math.sqrt(variance) };
  }

  disconnect(): void {
    this.wsConnection?.close();
    this.wsConnection = null;
    this._connected = false;
  }

  get isConnected(): boolean { return this._connected; }
  get totalEvents(): number { return this._totalEvents; }
}

export interface StreamEvent {
  type: string;
  timestamp: string;
  payload: Record<string, unknown>;
  receivedAt: number;
}

// Entry point — connects to the Wikimedia public real-time event stream
// (Wikipedia recent changes: ~1000 events/min, no authentication required)
const isMainModule = process.argv[1]?.endsWith('engine.ts') ||
                     process.argv[1]?.endsWith('engine.js');

if (isMainModule) {
  const processor = new HighThroughputStreamProcessor();

  processor.establishLowLatencyStream(
    'wss://stream.wikimedia.org/v2/stream/recentchange',
    (event) => {
      const title = event.payload['title'] as string | undefined;
      const wiki = event.payload['wiki'] as string | undefined;
      if (title && wiki) {
        console.log(`[EVENT] ${wiki} | ${event.type} | ${title}`);
      }
    },
  );

  setInterval(() => {
    const stats = processor.rollingStats();
    console.info(
      `[METRICS] total=${processor.totalEvents} ` +
      `rate=${stats.eventRate.toFixed(1)}/s ` +
      `μ=${stats.meanRate.toFixed(1)}/s ` +
      `σ=${stats.stdRate.toFixed(2)}`
    );
  }, 10_000);
}
