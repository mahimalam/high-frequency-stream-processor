import { describe, it, expect, beforeEach } from 'vitest';
import { HighThroughputStreamProcessor, StreamEvent } from '../engine.js';

describe('HighThroughputStreamProcessor', () => {
  let processor: HighThroughputStreamProcessor;

  beforeEach(() => {
    processor = new HighThroughputStreamProcessor();
  });

  describe('generateHmacSignature', () => {
    it('produces a 64-character hex string', () => {
      const sig = processor.generateHmacSignature('GET', '/stream', '1234567890');
      expect(sig).toHaveLength(64);
      expect(sig).toMatch(/^[0-9a-f]+$/);
    });

    it('is deterministic for the same inputs', () => {
      const a = processor.generateHmacSignature('GET', '/stream', '100');
      const b = processor.generateHmacSignature('GET', '/stream', '100');
      expect(a).toBe(b);
    });

    it('differs for different timestamps', () => {
      const a = processor.generateHmacSignature('GET', '/stream', '100');
      const b = processor.generateHmacSignature('GET', '/stream', '200');
      expect(a).not.toBe(b);
    });

    it('differs for different paths', () => {
      const a = processor.generateHmacSignature('GET', '/stream/a', '100');
      const b = processor.generateHmacSignature('GET', '/stream/b', '100');
      expect(a).not.toBe(b);
    });
  });

  describe('parseStreamEvent', () => {
    it('parses a valid JSON frame', () => {
      const raw = JSON.stringify({ type: 'edit', timestamp: '2024-01-01T00:00:00Z', title: 'Test' });
      const event = processor.parseStreamEvent(raw);
      expect(event).not.toBeNull();
      expect(event!.type).toBe('edit');
      expect(event!.timestamp).toBe('2024-01-01T00:00:00Z');
    });

    it('returns null for malformed JSON', () => {
      const event = processor.parseStreamEvent('not-json{{');
      expect(event).toBeNull();
    });

    it('defaults type to "unknown" when missing', () => {
      const raw = JSON.stringify({ timestamp: '2024-01-01T00:00:00Z' });
      const event = processor.parseStreamEvent(raw);
      expect(event!.type).toBe('unknown');
    });

    it('defaults timestamp to current time when missing', () => {
      const before = Date.now();
      const raw = JSON.stringify({ type: 'test' });
      const event = processor.parseStreamEvent(raw);
      expect(event).not.toBeNull();
      expect(new Date(event!.timestamp).getTime()).toBeGreaterThanOrEqual(before);
    });

    it('populates receivedAt as a numeric timestamp', () => {
      const before = Date.now();
      const event = processor.parseStreamEvent(JSON.stringify({ type: 'test' }));
      expect(event!.receivedAt).toBeGreaterThanOrEqual(before);
    });

    it('preserves full payload', () => {
      const raw = JSON.stringify({ type: 'log', value: 42, nested: { a: 1 } });
      const event = processor.parseStreamEvent(raw);
      expect(event!.payload['value']).toBe(42);
      expect(event!.payload['nested']).toEqual({ a: 1 });
    });
  });

  describe('rollingStats', () => {
    it('returns zero std when no history', () => {
      const stats = processor.rollingStats();
      expect(stats.stdRate).toBe(0);
    });

    it('returns non-negative values', () => {
      const stats = processor.rollingStats();
      expect(stats.eventRate).toBeGreaterThanOrEqual(0);
      expect(stats.meanRate).toBeGreaterThanOrEqual(0);
      expect(stats.stdRate).toBeGreaterThanOrEqual(0);
    });
  });

  describe('connection state', () => {
    it('starts disconnected', () => {
      expect(processor.isConnected).toBe(false);
    });

    it('starts with zero total events', () => {
      expect(processor.totalEvents).toBe(0);
    });

    it('disconnect does not throw when not connected', () => {
      expect(() => processor.disconnect()).not.toThrow();
    });
  });
});
