# Offline inputs

`snapshot.json` is a hand-authored synthetic application message shaped after
the [Pax snapshot documentation](https://docs.pax.markets/api-reference/websocket-messages/),
reviewed on 2026-10-07. It is not a production capture or evidence of liquidity.

The timing integration test and benchmark include the same bytes. No decoder
or reconstruction engine consumes them yet; the initial benchmark measures
harness overhead only. Add validated snapshot/event sequences and an independent
book oracle when reconstruction is implemented.
