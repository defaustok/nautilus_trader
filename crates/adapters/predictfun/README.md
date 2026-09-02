# Nautilus PredictFun adapter

Native PredictFun protocol support for NautilusTrader. The implementation contract and capability
matrix live in `docs/integrations/predictfun.md`.

The crate is under active integration. Protocol parsing and signing must remain network-free and
deterministic; live execution is validated against BNB testnet before production credentials are
accepted.

The `predictfun-data-tester` and `predictfun-exec-tester` examples provide the standard Nautilus
live acceptance harnesses. The execution example is testnet-only and defaults to dry-run.

`predictfun-testnet-probe` is a read-only live qualification tool. It verifies public REST reads,
signs the dynamic authentication challenge locally, and reads authenticated account endpoints. Set
`PREDICTFUN_TESTNET_PRIVATE_KEY_FILE` to a protected EOA key file. Testnet REST does not require an
API key. An optional WebSocket qualification runs only when
`PREDICTFUN_TESTNET_WEBSOCKET_URL` is set; private topic errors redact the wallet JWT.
