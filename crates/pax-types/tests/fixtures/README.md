# Public contract fixtures

`snapshot.json` and `xa.json` reproduce the numeric book-building example from
[the official documentation](https://docs.pax.markets/api-reference/websocket-market-data-book-building/),
reviewed 2026-10-07. They are documentation examples, not a live recording.

`metadata.json` was read from the unauthenticated production
[exchange-info endpoint](https://api.pax.markets/v1/metadata/exchange-info)
on 2026-10-07 UTC. It identifies one active market at that observation time;
production operation always fetches fresh metadata.

All tests are offline. No fixture contains account data or credentials.

`xa-final.json` is the manually specified expected state of the documented XA
example: asks at 100010 and 100012 are consumed, and 1000 units rest as a bid at
100012. It is a benchmark reference, not a second received snapshot.
