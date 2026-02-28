# Directive: Data Infrastructure

## Goal

Establish all API connections and scraping fallbacks needed to feed the trading system with real-time and historical data from Polymarket, Kalshi, on-chain sources, and alternative data providers.

## Data Sources

### Tier 1: Full Public APIs (Direct Integration)

#### Polymarket CLOB API
- **Base URL**: `https://clob.polymarket.com`
- **Auth**: Ed25519 private key + HMAC signature per request
- **Key endpoints**:
  - `GET /markets` — list active markets with prices
  - `GET /book` — full orderbook for a market
  - `POST /order` — place an order (signed)
  - `GET /orders` — query open orders
  - `DELETE /order/{id}` — cancel order
- **Rate limits**: TBD — discover during integration, update this directive
- **Tool**: `execution/connectors/polymarket_clob.py`
- **Output**: Raw price ticks → `stream:prices:polymarket`

#### Polymarket Gamma API
- **Base URL**: `https://gamma-api.polymarket.com`
- **Auth**: None (public)
- **Key endpoints**:
  - `GET /markets` — market metadata, categories, resolution criteria
  - `GET /markets/{id}` — single market detail incl. `close_time`, `resolution_source`
- **Rate limits**: TBD
- **Tool**: `execution/connectors/polymarket_gamma.py`
- **Output**: Market metadata → PostgreSQL `market_prices` (title, close_time enrichment)

#### Polymarket Data API
- **Base URL**: `https://data-api.polymarket.com`
- **Auth**: None (public)
- **Key endpoints**:
  - `GET /activity` — wallet trade activity
  - `GET /positions` — wallet current positions
  - `GET /leaderboard` — top traders by PnL
- **Rate limits**: TBD
- **Tool**: `execution/connectors/polymarket_data_api.py`
- **Output**: Wallet activity data → used by wallet_profiler

#### Kalshi REST API
- **Base URL**: `https://api.elections.kalshi.com/trade-api/v2`
- **Auth**: RSA-PSS token, 30-minute TTL
- **Critical auth pattern**:
  ```
  POST /trade-api/v2/login → { token, expiry }
  Store token + expiry timestamp
  Refresh at 25-minute mark (5 min before expiry)
  All requests: Authorization: Bearer <token>
  ```
- **Key endpoints**:
  - `GET /markets` — list markets with prices
  - `GET /markets/{ticker}` — single market detail
  - `POST /orders` — place order
  - `GET /portfolio/positions` — current positions
- **Rate limits**: TBD
- **Tool**: `execution/connectors/kalshi_rest.py`
- **Output**: Market data → PostgreSQL; orders → execution engine

#### Kalshi WebSocket
- **Base URL**: `wss://api.elections.kalshi.com/trade-api/v2/ws`
- **Auth**: Same token as REST
- **Subscription**: Send market tickers to subscribe to live price updates
- **Tool**: `execution/connectors/kalshi_ws.py`
- **Output**: Raw price ticks → `stream:prices:kalshi`

#### Polygon RPC (via Alchemy)
- **Base URL**: Alchemy Polygon endpoint (env var `ALCHEMY_API_KEY`)
- **Auth**: API key in URL
- **Key methods**:
  - `eth_getLogs` — filter for wallet transfer events
  - `eth_getTransactionReceipt` — individual tx details
- **Tool**: `execution/connectors/polygon_rpc.py`
- **Output**: On-chain tx history → used by wallet_profiler for IPS scoring

#### Goldsky GraphQL
- **Base URL**: `https://subgraph.goldsky.com`
- **Auth**: None
- **Key queries**:
  - Order-filled events for Polymarket contracts
  - Filter by wallet address, time range, market
- **Tool**: `execution/connectors/goldsky_graphql.py`
- **Output**: Trade history → used by wallet_profiler + s2_insider_detection

### Tier 2: Scraping/Parsing Required

#### Glint (via Telegram)
- **Method**: Telegram bot listens to Glint alert channel
- **Data format**: Structured alert messages with signal type, matched contracts, impact level, relevance score
- **Auth**: `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHANNEL_ID`
- **Tool**: `execution/connectors/glint_telegram.py`
- **Output**: Parsed signals → `stream:news:signals`

#### Hashdive
- **Method**: Playwright headless browser scraping trader profile pages
- **Data**: Smart Scores, insider flags, wallet performance metrics
- **Auth**: `HASHDIVE_SESSION_COOKIE` (manually extracted after browser login)
- **XHR interception**: Capture API responses during page load for structured data
- **Tool**: `execution/connectors/hashdive_scraper.py`
- **Output**: Wallet scores → PostgreSQL `wallet_scores.hashdive_score`

## Error Handling

- **Auth failures**: Log clearly, retry once with fresh credentials, then alert and halt the connector
- **Rate limiting**: Back off exponentially (1s, 2s, 4s, 8s, 16s, max 60s). Log the rate limit response headers.
- **Network timeouts**: 30s timeout for REST, 60s for WebSocket reconnection. Log all reconnections.
- **Data validation**: Every connector must validate response schema before publishing. Drop malformed records with a warning log, never publish garbage to Redis.
- **Kalshi token expiry**: If a request returns 401, immediately refresh token and retry once. If refresh fails, halt connector and alert.

## Testing

Each connector must support `--test` mode that:
1. Returns fixture data from a local JSON file in `.tmp/fixtures/`
2. Does NOT make any network calls
3. Publishes fixture data to Redis Streams (or stdout in dry-run mode)
4. Can be run in CI without credentials

## Edge Cases

- **Polymarket CLOB signing**: Ed25519 signatures must be computed correctly. Use the `py_clob_client` library if available, otherwise implement from spec. Test against known signature vectors.
- **Kalshi US-only**: All Kalshi API calls must originate from a US IP. If deploying outside US, route Kalshi traffic through a US proxy/VPN.
- **Glint channel format changes**: Glint may change their alert message format. The parser must be resilient — log unparseable messages and continue.
- **Hashdive anti-bot**: Playwright must use stealth mode. Rotate user agents. Add random delays between page loads (2-5s).
- **Goldsky pagination**: Large queries return paginated results. Implement cursor-based pagination with a maximum of 1000 records per request.
