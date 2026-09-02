# PredictFun adapter

This document is the implementation contract for the native PredictFun adapter. It follows the
NautilusTrader adapter, data-testing, and execution-testing guides at the pinned source revision.
The adapter is intentionally implemented in Rust with a thin PyO3 facade; an out-of-tree Python
adapter would not satisfy the current Nautilus live-client contract.

## Protocol sources

- PredictFun REST/OpenAPI: `https://api.predict.fun/docs` and `https://dev.predict.fun/`
- PredictFun WebSocket: `wss://ws.predict.fun/ws`
- Official TypeScript SDK `@predictdotfun/sdk` 1.3.8, commit
  `5ff2b4ac568964d4895aa27d8a04e95bdc05da69`
- Official Python SDK 0.0.22 and its cross-SDK EIP-712 hash vectors
- Nautilus native Polymarket adapter for binary-option mapping and snapshot semantics
- Nautilus Derive and Lighter adapters for execution lifecycle and reconciliation patterns

PredictFun is not present in the pinned Nautilus source and the public Python API does not support
an out-of-tree live adapter, so a custom native adapter is required.

## Capability matrix

| Capability | PredictFun transport | Nautilus behavior |
| --- | --- | --- |
| Instruments | `GET /v1/markets`, `GET /v1/markets/{id}` | One `BinaryOption` per outcome token; raw symbol is `onChainId` |
| L2 order book | REST and `predictOrderbook/{marketId}` | Full snapshot translated to atomic `CLEAR + ADD`; YES book also produces the exact complementary NO book |
| Quotes | Derived from the authoritative full book | Best bid/ask emitted after a valid snapshot |
| Public trades | No authoritative public trade stream documented | Unsupported; `lastOrderSettled` is not converted into synthetic `TradeTick` data |
| Trading status | `predictTradingStatus/{marketId}` | Instrument status updates for OPEN, MATCHING_NOT_ENABLED, CANCEL_ONLY, CLOSED |
| Limit orders | `POST /v1/orders`, strategy `LIMIT` | GTC and GTD; post-only supported |
| Market orders | `POST /v1/orders`, strategy `MARKET` | Venue book-sweeping order with explicit slippage; FOK when requested |
| Modify | No atomic modify endpoint | Unsupported; callers must cancel and submit a replacement |
| Cancel | REST off-chain removal plus exchange `cancelOrders` on BNB Chain | Grouped by exchange variant; direct EOA call or Predict Account `Kernel.execute`; a canceled event requires a successful receipt, terminal contract status, and REST race reconciliation |
| Order reports | `GET /v1/orders`, `GET /v1/orders/{hash}` | Targeted and bulk reports |
| Fill reports | `GET /v1/orders/matches` | Stable settlement identity; exact reported native fee asset |
| Positions | `GET /v1/positions` | Authoritative account-level NETTING reports |
| Private events | `predictWalletEvents/{jwt}` | Order/fill routing with REST recovery and settlement-ID deduplication |
| Accounts | EOA and Predict smart account | Both account types support signing and cancellation; total collateral comes from on-chain USDT and locked collateral from open BUY orders |
| Environments | BNB 56 and BNB testnet 97 | Explicit `mainnet`/`testnet`; credentials and URLs cannot cross environments |

## Non-negotiable invariants

- All domain prices, quantities, amounts, and fees use `Decimal` or integer wei. JSON numbers are
  converted from their lexical representation only at the wire boundary; no `f64` reaches the
  domain layer.
- PredictFun contracts use 18-decimal wei while the pinned Nautilus fixed-point model supports at
  most 16 decimal places. The official order builder restricts executable quantity to at least
  `0.01` shares and five significant digits, which is exactly representable. A venue value with
  non-zero digits beyond Nautilus precision fails closed and is recovered through reconciliation;
  it is never silently rounded.
- Every full book begins with `F_SNAPSHOT` clear. Each add carries `F_SNAPSHOT`; only the final
  add carries `F_LAST`. An empty book is represented by the clear alone with `F_LAST`.
- A single underlying market subscription is reference-counted across YES and NO instruments.
  Reconnect reauthenticates, restores intent, and resets the snapshot epoch before readiness.
- Execution becomes connected only after private WebSocket authentication, initial account state,
  and REST reconciliation are available.
- The execution configuration requires an explicit BNB JSON-RPC URL. Its chain ID must match the
  selected environment (56 or 97), and the URL is redacted from debug output.
- A request failure is never converted into `Ok(None)`. State-changing timeouts, 5xx responses,
  or response parse failures are ambiguous and trigger reconciliation rather than rejection.
- Wallet events are deduplicated by stable venue order/hash and settlement IDs. A deduplication key
  is consumed only after every derived Nautilus event has been constructed and routed.
- API keys, JWTs, wallet keys, signatures, and raw authentication frames are redacted from logs.
- Ordinary tests are network-free. Mainnet execution is never enabled by tests or examples.

## Price and outcome mapping

The wire order book is an aggregated YES book. For every YES ask `(p, q)`, the NO bid is
`(1 - p, q)`; for every YES bid `(p, q)`, the NO ask is `(1 - p, q)`. Levels are sorted after the
transform and validated against the market tick. Outcome instruments retain distinct on-chain token
IDs; the venue market ID alone is never used as the execution symbol.

## Acceptance order

1. Protocol fixture, exact arithmetic, EIP-712, and reconnect state-machine tests.
2. DataTester cases for instrument loading, books, snapshots, quotes, status, unsubscribe/reconnect.
3. Testnet execution tests for limit/market, cancellation, failures, private fills, and restart
   reconciliation.
4. Recorder soak test with no execution credentials loaded.
5. Terminal Poly migration to the recorder's shared live stream.

The existing Terminal Poly recorder remains active until step 4 demonstrates continuous, lossless
native recording. Production trading requires a separate operator decision after testnet evidence.

## Testnet harnesses

The crate includes Nautilus `DataTester` and `ExecTester` examples. Both are pinned to testnet and
require explicit API, WebSocket, market, and instrument values; the execution harness additionally
requires the private key, BNB RPC URL, and optional Predict Account address. It is dry-run unless
`PREDICTFUN_EXEC_TESTER_LIVE=1` is deliberately set.

```bash
cargo run -p nautilus-predictfun --example predictfun-data-tester --features examples
cargo run -p nautilus-predictfun --example predictfun-exec-tester --features examples
```

Required variables are documented in the example source. Test evidence must retain the environment,
account type, market, timestamps, reconciliation result, and transaction hashes without retaining
API keys, JWTs, private keys, or RPC credentials.
