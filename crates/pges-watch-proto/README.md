# pges-watch-proto

The gRPC watch service of the pges sequencer, `pges.watch.v1`: the protobuf messages, and the tonic client and
server generated from [`proto/pges/watch/v1/watch.proto`](proto/pges/watch/v1/watch.proto).

The leading sequencer streams each store's high-water mark to subscribers: the store's newest position, each
category's newest position, the WAL position replica readers wait for, and the fence of the term that
announced it. A sequencer that stands by answers `UNAVAILABLE`.

Most applications do not use this crate directly: `pges-sequencer` serves the service (feature `watch-grpc`)
and `pges-subscribe`'s `GrpcWaker` consumes it (feature `wake-grpc`). Clients in other languages generate
their own code from the proto file.

The generated code is checked in, so a build needs neither `protoc` nor a build script.

## Features

- `client`: `v1::watch_client::WatchClient`.
- `server`: `v1::watch_server::{Watch, WatchServer}`.

## Example

```rust
use pges_core::{high_water, position};
use pges_watch_proto::v1;

let high = high_water::HighWater::new(position!(42), Vec::new(), None);
let message = v1::WatchResponse::new(3, &high);

assert_eq!(message.fence, 3);
assert_eq!(message.high_water(), Some(high));
```
