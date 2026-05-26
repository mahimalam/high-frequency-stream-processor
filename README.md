# Sovereign Yield & High-Frequency Liquidation Infrastructure (Staging)

An enterprise-grade, low-latency off-chain analytical engine designed to process sub-second oracle reports to compute delta-neutral liquidation thresholds, protocol risk metrics, and MEV routing vectors. 

## Deployment & Host Infrastructure
- **Compute Host:** Private high-performance VPS instance deployed in Zurich (CH) cloud zones, optimized for low-latency network I/O, parallel thread execution, and real-time asynchronous processing.
- **Runtime Core:** Node.js v20 LTS engine implementing a non-blocking asynchronous event loop with strict Network Time Protocol (NTP) synchronization to prevent sub-second signature drift.

## Oracle Ingestion Architecture
The engine acts as a high-frequency off-chain consumer utilizing a persistent, multi-channel WebSocket pipeline targeting the Chainlink Data Streams (Schema v3 Crypto Advanced) infrastructure. The processing pipeline uses off-chain HMAC-SHA256 signature verification during the handshake interface to guarantee origin authenticity.

### Monitored Crypto Asset Matrix (Testnet Stream IDs)
The framework targets high-volume, low-latency feeds to compute volatile liquidation constraints:
- **BTC / USD:** `0x000359843a543ee2fe414dc14c7e7920ef10f4372990b79d6361cdc0dd1ba782`
- **ETH / USD:** [Schema v3 Real-Time Multi-Site Aggregate Stream]
- **SOL / USD:** [Schema v3 Real-Time Multi-Site Aggregate Stream]
- **XRP / USD:** [Schema v3 Real-Time Multi-Site Aggregate Stream]
- **DOGE / USD:** [Schema v3 Real-Time Multi-Site Aggregate Stream]

*Note: This repository represents our isolated development staging sandbox used exclusively for benchmarking payload handling speeds, signature validation logic, and execution thread safety under simulated high-volatility market events before full smart contract implementation.*
