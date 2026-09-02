# PredictFun adapter development tracker

Last updated: 2026-09-01

This file is the durable source of truth for implementation progress. A box is checked only when
the implementation and its required deterministic tests exist. Live acceptance boxes additionally
require retained test evidence. Production trading is outside this tracker and requires an explicit
operator decision.

## Definition of done

- [x] Native Rust data and execution clients implement the pinned NautilusTrader 2.0 adapter traits.
- [x] Strict Rust config, factories, PyO3 module, Python package, and type stubs are wired.
- [ ] All supported protocol capabilities have deterministic fixture tests.
- [ ] DataTester applicable cases pass on testnet or a controlled live data session.
- [ ] ExecTester applicable cases pass on PredictFun testnet with reconciliation enabled.
- [ ] Recorder soak test proves restart/reconnect continuity and Parquet catalog readability.
- [ ] Terminal Poly consumes the shared recorder stream; its duplicate recorder is removed only
      after a rollback-safe cutover.

## 0. Research and contract

- [x] Read the pinned Nautilus adapter guide completely.
- [x] Read the pinned data and execution testing specifications.
- [x] Read networking/reconnect/backpressure and reconciliation contracts.
- [x] Compare with native Polymarket, Derive, and Lighter adapter patterns.
- [x] Inspect PredictFun REST/OpenAPI, WebSocket documentation, TS SDK 1.3.8, and Python SDK 0.0.22.
- [x] Record capability matrix, protocol boundaries, unsupported public trades, and invariants in
      `docs/integrations/predictfun.md`.

## 1. Protocol core

- [x] Add crate skeleton, venue/environment constants, and strict wire enums.
- [x] Add exact decimal JSON boundary parser; no domain `f64`.
- [x] Add market, outcome, book, order, fill, position, and wallet-event wire models.
- [x] Add full YES snapshot and complementary NO snapshot conversion.
- [x] Handle empty book as `CLEAR | F_SNAPSHOT | F_LAST`.
- [x] Validate book market ID, version monotonicity, price range, tick alignment, duplicate levels,
      crossed books, and outcome pairing.
- [ ] Add official/captured REST and WebSocket fixtures (valid, malformed, and schema-drift cases).
- [x] Add EIP-712 EOA signing and hash vectors for all four exchange-contract variants on both chains.
- [x] Add Predict smart-account Kernel signature wrapping and official cross-SDK vectors.
- [x] Add exact limit/market order amount builder, significant-digit rules, slippage, and minimums.
- [x] Add exact collateral-fee parsing tests and fail closed for share-denominated fees that cannot
      be represented as Nautilus USDT commission; never substitute a generic/zero fee.

## 2. HTTP and authentication

- [x] Typed shared `nautilus_network::HttpClient` wrapper with central mainnet/testnet URLs.
- [x] API-key authentication and JWT challenge/sign flow with secret redaction/zeroization.
- [x] Market discovery and specific-token lookup without per-report hidden network fetches.
- [x] Fresh order-book snapshot request and response validation.
- [x] Submit, off-chain remove, targeted order, bulk orders, matches, positions, and account
      activity calls. Account total comes from BNB-chain USDT; locked collateral is derived exactly
      from authoritative open BUY orders.
- [x] Cursor pagination with loop detection, 240 requests/minute policy, and typed venue errors.
- [x] Submit failures conservatively classified as venue-rejected or ambiguous; ambiguous POSTs are
      never retried or converted into terminal rejection.

## 3. WebSocket lifecycle

- [x] Typed request/envelope/order-book/wallet message boundary.
- [x] Shared handler-mode client using Nautilus reconnect support and unbounded live event paths.
- [x] Exact application heartbeat echo; shared transport owns control frames.
- [x] Request correlation and acknowledgement failure visibility; private startup waits for ACK.
- [x] Reference-counted market topics across YES/NO instruments.
- [x] Reauthenticate with a fresh JWT after reconnect, subscribe with acknowledgement, and complete
      private REST/account reconciliation before restoring execution readiness.
- [x] Snapshot epoch reset and stale/out-of-order version recovery.
- [x] Private wallet topic authenticated by JWT; raw auth frames never logged.
- [x] Repeated bounded shutdown joins/aborts every owned task safely.

## 4. Instruments and data client

- [x] Map each outcome on-chain token to a distinct Nautilus `BinaryOption` raw symbol.
- [ ] Complete instrument metadata, activation/expiration, tick/size precision, fee metadata, and
      trading state mapping from captured payloads.
- [x] Instrument provider supports load-all, market IDs, token IDs, and typed filters.
- [x] Publish instruments before data readiness.
- [x] Implement subscribe/unsubscribe/request for books, quotes, status, and instruments.
- [x] Implement Rust `DataClient` and Python data-client factory.
- [x] Explicitly reject unsupported trade/bar subscriptions without fabricated data.

## 5. Execution client

- [x] Strict local validation for limit GTC/GTD, post-only, market FOK, and unsupported commands.
- [x] Stable `OrderIdentity`/`OrderContext` mapping across client ID, venue ID, order hash, token,
      market, and strategy ownership.
- [x] Submit lifecycle and grouped cancellation are implemented. Cancellation performs advisory
      REST removal, signs/sends authoritative exchange invalidation for EOA or Predict Account,
      verifies the receipt and contract terminal state, then resolves cancel/fill races through REST.
- [x] Private order/fill routing for tracked and external orders.
- [x] Wallet fills are emitted only after settlement success and deduplicated by settlement ID.
- [x] Targeted and bulk order reports plus position reports are implemented.
- [x] Bulk order, fill, position, and mass-status reports with declared lookback/completeness.
- [x] Initial on-chain account state and calculated locked collateral before connected readiness.
- [x] REST recovery for missed/ambiguous private events and restart/reconnect reconciliation.
- [x] Implement Rust `ExecutionClient` and Python execution-client factory (unsupported unsafe
      operations fail closed).

## 6. Repository and Python wiring

- [x] Root Cargo workspace member and dependency.
- [x] Makefile adapter crate list.
- [x] PyO3 Cargo dependency and feature propagation.
- [x] `_libnautilus.predictfun` module registration.
- [x] Python `nautilus_trader.adapters.predictfun` facade and deterministic `__all__`.
- [x] Config/factory extractors, generated stubs, docs navigation, and recorder example are wired.
- [x] Reproducible patch artifact and SHA-256 pin in the deployment repository.

## 7. Verification and rollout

- [x] `cargo fmt --check` and focused Clippy clean.
- [ ] Protocol unit/fixture/property tests clean and network-free.
- [x] Python facade/config/factory tests clean.
- [ ] Applicable DataTester cases are implemented/documented; retained testnet evidence is pending.
- [ ] Applicable ExecTester cases are implemented/documented; retained BNB testnet evidence for EOA
      and Predict Account is pending.
- [ ] Reconnect, malformed payload, rate-limit, timeout, ambiguous submit/cancel, restart, and
      incomplete reconciliation acceptance cases passing.
- [ ] Rebuild/install the updated pinned Nautilus wheel through the macOS ARM64 bootstrap path.
- [ ] Run data-only recorder soak without loading wallet/JWT execution credentials.
- [ ] Verify compressed Parquet partitions, schema, catalog queries, retention, and restart gaps.
- [ ] Cut Terminal Poly to the shared realtime stream with rollback path.
- [ ] Stop/remove the duplicate Terminal recorder only after parity and soak evidence.

## Current focus

Testnet acceptance, data-recorder soak, and rollback-safe Terminal cutover. The protocol, HTTP,
handler-mode WebSocket, instrument provider, data client, on-chain cancellation paths, account
state, and reconciliation compile and pass focused deterministic tests. Production execution
remains disabled until the explicit testnet and soak gates pass.

## Open risks / decisions

- Current server configuration exposes only a PredictFun API-key name; no wallet key/JWT signer is
  configured. This is sufficient for data development but not execution acceptance.
- The documented WebSocket endpoint is shared; the testnet WebSocket URL must be confirmed from
  an authoritative response before being enabled rather than inferred.
- Public `lastOrderSettled` lacks documented unique trade semantics, so public `TradeTick` remains
  unsupported. Owned fills come from wallet events and REST matches.
- REST removal is advisory. The official SDK confirms that authoritative cancellation calls the
  selected exchange contract directly for EOAs or through `Kernel.execute` for Predict Accounts;
  both paths still require retained BNB testnet evidence.
- Predict fees may be denominated in shares or collateral. Reconciliation will use the reported fee
  asset and fail closed if it cannot be represented exactly.
- PredictFun contract amounts are 18-decimal wei; pinned Nautilus fixed-point values support 16
  decimals. Official executable quantities fit because of the 0.01 minimum/five-significant-digit
  rule. Non-representable wire residues must fail closed, never round silently.

## Evidence log

| Date | Evidence | Result |
| --- | --- | --- |
| 2026-08-31 | Pinned Nautilus source `648970ce64a304d93da0a29320cb6e19b905fa39` | Adapter API and test contracts reviewed |
| 2026-08-31 | Official PredictFun TS SDK 1.3.8 / Python SDK 0.0.22 | EIP-712 fields, chains, contracts, amount normalization recorded |
| 2026-08-31 | `cargo check -p nautilus-predictfun --all-targets` | Passed |
| 2026-08-31 | `cargo test -p nautilus-predictfun --lib` | 24 protocol/data/transport tests passed |
| 2026-08-31 | `cargo clippy -p nautilus-predictfun --all-targets -- -D warnings` | Passed after boxing wallet events and using key sorts |
| 2026-08-31 | `cargo fmt -p nautilus-predictfun` | Applied; stable rustfmt only warned that two nightly-only grouping options were ignored |
| 2026-08-31 | Official Python SDK 0.0.22 oracle | Kernel digest vector `0x5907…9b24` matched |
| 2026-08-31 | `cargo test -p nautilus-predictfun --lib` | 25 protocol/data/transport tests passed |
| 2026-08-31 | `cargo clippy -p nautilus-predictfun --all-targets --features python -- -D warnings` | Passed |
| 2026-09-01 | `cargo check -p nautilus-predictfun --all-targets --features python` | Passed |
| 2026-09-01 | `cargo test -p nautilus-predictfun --lib` | 33 protocol/data/execution tests passed |
| 2026-09-01 | `cargo clippy -p nautilus-predictfun --all-targets --features python -- -D warnings` | Passed |
| 2026-09-01 | `cargo check -p nautilus-predictfun --examples --features examples` | DataTester and testnet ExecTester harnesses passed |
| 2026-09-01 | Official mainnet DataTester, market 1852158 | Exact instrument load, live 3-level book, quote subscription, reconnect/resubscription, and snapshot reset passed without production mutation |
| 2026-09-01 | `cargo clippy -p nautilus-predictfun --all-targets --all-features -- -D warnings` | Passed |
