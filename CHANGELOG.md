# Changelog

All notable changes to iroh-docs will be documented in this file.

## [0.28.0] - 2024-11-04

### ⛰️  Features

- *(iroh-gossip)* Configure the max message size ([#2340](https://github.com/n0-computer/iroh-docs/issues/2340)) - ([8815bed](https://github.com/n0-computer/iroh-docs/commit/8815bedd3009a7013ea6eae3447622696e0bee0f))
- *(iroh-net)* [**breaking**] Improve initial connection latency ([#2234](https://github.com/n0-computer/iroh-docs/issues/2234)) - ([62afe07](https://github.com/n0-computer/iroh-docs/commit/62afe077adc67bd13db38cf52d875ea5c96e8f07))
- *(iroh-net)* Own the public QUIC API ([#2279](https://github.com/n0-computer/iroh-docs/issues/2279)) - ([6f4c35e](https://github.com/n0-computer/iroh-docs/commit/6f4c35ef9c51c671c4594ea828a3dd29bd952ce5))
- *(iroh-net)* [**breaking**] Upgrade to Quinn 0.11 and Rustls 0.23 ([#2595](https://github.com/n0-computer/iroh-docs/issues/2595)) - ([add0bc3](https://github.com/n0-computer/iroh-docs/commit/add0bc3a9a002d685e755f8fce03241dc9fa500a))
- [**breaking**] New quic-rpc, simlified generics, bump MSRV to 1.76 ([#2268](https://github.com/n0-computer/iroh-docs/issues/2268)) - ([9c49883](https://github.com/n0-computer/iroh-docs/commit/9c498836f8de4c9c42f15d422f282fa3efdbae10))
- Set derive_more to 1.0.0 (no beta!) ([#2736](https://github.com/n0-computer/iroh-docs/issues/2736)) - ([9e517d0](https://github.com/n0-computer/iroh-docs/commit/9e517d09e361978f3a95983f6c906068d1cf6512))
- Ci ([#1](https://github.com/n0-computer/iroh-docs/issues/1)) - ([6b2c973](https://github.com/n0-computer/iroh-docs/commit/6b2c973f8ba9acefce1b36df93a8e6a5f8d7392a))

### 🐛 Bug Fixes

- *(docs)* Prevent deadlocks with streams returned from docs actor ([#2346](https://github.com/n0-computer/iroh-docs/issues/2346)) - ([6251369](https://github.com/n0-computer/iroh-docs/commit/625136937a2fa63479c760ce9a13d24f8c97771c))
- *(iroh-docs)* Ensure docs db write txn gets closed regularly under all circumstances ([#2474](https://github.com/n0-computer/iroh-docs/issues/2474)) - ([ea226dd](https://github.com/n0-computer/iroh-docs/commit/ea226dd48c0dcc0397f319aeb767563cbec298c6))
- *(iroh-docs)* [**breaking**] Add `flush_store` and use it to make sure the default author is persisted ([#2471](https://github.com/n0-computer/iroh-docs/issues/2471)) - ([f67a8b1](https://github.com/n0-computer/iroh-docs/commit/f67a8b143a36a18903c1338e8412cdd7f1729582))
- *(iroh-docs)* Do not dial invalid peers ([#2470](https://github.com/n0-computer/iroh-docs/issues/2470)) - ([dc7645e](https://github.com/n0-computer/iroh-docs/commit/dc7645e45ce875b9c19d32d65544abefd0417e80))
- *(iroh-net)* Prevent adding addressing info that points back to us ([#2333](https://github.com/n0-computer/iroh-docs/issues/2333)) - ([54d5991](https://github.com/n0-computer/iroh-docs/commit/54d5991f5499ed52749e420eb647d726a3674eb4))
- *(iroh-net)* Fix a compiler error with newer `derive_more` versions ([#2578](https://github.com/n0-computer/iroh-docs/issues/2578)) - ([3f93765](https://github.com/n0-computer/iroh-docs/commit/3f93765ea36f47554123bfb48c3ac06068b970b6))
- *(iroh-net)* [**breaking**] DiscoveredDirectAddrs need to update the timestamp ([#2808](https://github.com/n0-computer/iroh-docs/issues/2808)) - ([916aedd](https://github.com/n0-computer/iroh-docs/commit/916aedd4a7a0f458be3d19d1d3f3d46d0bb074ad))
- Properly wait for docs engine shutdown ([#2389](https://github.com/n0-computer/iroh-docs/issues/2389)) - ([5b48493](https://github.com/n0-computer/iroh-docs/commit/5b48493917a5a87b21955fefc2f341a2f9fa0a1e))
- Pin derive_more to avoid sudden breakages ([#2584](https://github.com/n0-computer/iroh-docs/issues/2584)) - ([cfda981](https://github.com/n0-computer/iroh-docs/commit/cfda981a0ad075111e0c64e3e9a145d4490d1cc5))

### 🚜 Refactor

- *(iroh)* [**breaking**] Remove tags from downloader ([#2348](https://github.com/n0-computer/iroh-docs/issues/2348)) - ([38f75de](https://github.com/n0-computer/iroh-docs/commit/38f75de7cbde0a39582afcac12c73a571705ea58))
- *(iroh-docs)* Replace flume with async_channel in docs ([#2540](https://github.com/n0-computer/iroh-docs/issues/2540)) - ([335b6b1](https://github.com/n0-computer/iroh-docs/commit/335b6b15bf07471c672320127c205a34788462bb))
- *(iroh-net)* [**breaking**] Rename MagicEndpoint -> Endpoint ([#2287](https://github.com/n0-computer/iroh-docs/issues/2287)) - ([86e7c07](https://github.com/n0-computer/iroh-docs/commit/86e7c07a41953f07994dd5d31b9ed3d529a519c3))
- Renames iroh-sync & iroh-bytes ([#2271](https://github.com/n0-computer/iroh-docs/issues/2271)) - ([f1c3cf9](https://github.com/n0-computer/iroh-docs/commit/f1c3cf9b96fab2340df3dc0f5355137234772d95))
- Move docs engine into iroh-docs ([#2343](https://github.com/n0-computer/iroh-docs/issues/2343)) - ([28cf40b](https://github.com/n0-computer/iroh-docs/commit/28cf40b295c3adca75f9328a23be3ff0e6e8a57f))
- [**breaking**] Metrics ([#2464](https://github.com/n0-computer/iroh-docs/issues/2464)) - ([4938055](https://github.com/n0-computer/iroh-docs/commit/4938055577fb6a975e547983ebdfb1c3b6b03df9))
- [**breaking**] Migrate to tokio's AbortOnDropHandle ([#2701](https://github.com/n0-computer/iroh-docs/issues/2701)) - ([cf05227](https://github.com/n0-computer/iroh-docs/commit/cf05227141cacace21fe914c0479786000989869))
- Extract `ProtocolHandler` impl into here - ([16bc7fe](https://github.com/n0-computer/iroh-docs/commit/16bc7fe4c7dee1b1b88f54390856c3fafa3d7656))

### 📚 Documentation

- *(*)* Document cargo features in docs ([#2761](https://github.com/n0-computer/iroh-docs/issues/2761)) - ([8346d50](https://github.com/n0-computer/iroh-docs/commit/8346d506fd2553bc813548a7d41f04adc984f7d2))
- Fix typos discovered by codespell ([#2534](https://github.com/n0-computer/iroh-docs/issues/2534)) - ([b5b7072](https://github.com/n0-computer/iroh-docs/commit/b5b70726f10cd33d354e07b8985e171cdd3cfc0f))

### ⚙️ Miscellaneous Tasks

- Release - ([b8dd924](https://github.com/n0-computer/iroh-docs/commit/b8dd9243361b708a41b15cad4809ad5b34d73c87))
- Release - ([045e3ce](https://github.com/n0-computer/iroh-docs/commit/045e3ce6dd56bf57b2127960fd1ebad4b9858026))
- Release - ([a9a8700](https://github.com/n0-computer/iroh-docs/commit/a9a87007b597f4f41c68dcbebe8b30bdb2cd3119))
- Release - ([08fe6f3](https://github.com/n0-computer/iroh-docs/commit/08fe6f3ad6fc4ded76991b059a8ffefaf06c1129))
- Introduce crate-ci/typos ([#2430](https://github.com/n0-computer/iroh-docs/issues/2430)) - ([b97e407](https://github.com/n0-computer/iroh-docs/commit/b97e4071508191894581e48882a53cf6b095f86f))
- Release - ([73c22f7](https://github.com/n0-computer/iroh-docs/commit/73c22f7f13ce0276677fc599e80c11f470127966))
- Release - ([9b53b1b](https://github.com/n0-computer/iroh-docs/commit/9b53b1bee105952d883a81bb2238b1e569f78332))
- Fix clippy warnings ([#2550](https://github.com/n0-computer/iroh-docs/issues/2550)) - ([956c51a](https://github.com/n0-computer/iroh-docs/commit/956c51a21b49e15e136434205dbc21040aa8b62b))
- Release - ([5fd07fa](https://github.com/n0-computer/iroh-docs/commit/5fd07fabf313828d5484b7ea315199f9bd7178b9))
- Release - ([92d8493](https://github.com/n0-computer/iroh-docs/commit/92d8493071ac9c5642358daea93b47b1c620e312))
- Release - ([f929d15](https://github.com/n0-computer/iroh-docs/commit/f929d15e76bfabb6d713402daff3b399f122e016))
- Release - ([7e3f028](https://github.com/n0-computer/iroh-docs/commit/7e3f028c65028c1b16c41a01fcc25ff6e2ad2e63))
- Release - ([5f98043](https://github.com/n0-computer/iroh-docs/commit/5f980439a5305bf781ea8c04f3d20976d26f88a8))
- Format imports using rustfmt ([#2812](https://github.com/n0-computer/iroh-docs/issues/2812)) - ([e6aadbd](https://github.com/n0-computer/iroh-docs/commit/e6aadbd337dda7697a34526a5a17fb38d15978c6))
- Increase version numbers and update ([#2821](https://github.com/n0-computer/iroh-docs/issues/2821)) - ([5ac936f](https://github.com/n0-computer/iroh-docs/commit/5ac936f0d5a7a8a6ffe9931638dbb009dc339494))
- Release - ([d52df5f](https://github.com/n0-computer/iroh-docs/commit/d52df5f46d1c9fea2b2012a32f8fe9a3b86a14e3))
- Copy missing files - ([61d8e6a](https://github.com/n0-computer/iroh-docs/commit/61d8e6ada870d736182333d3dc4f2c2387e1dbb4))
- Update Cargo.toml - ([6be9cb0](https://github.com/n0-computer/iroh-docs/commit/6be9cb01a3dc8ba0d9cbd74572231b1a18df36ae))
- Upgrade 0.28 iroh-net - ([7e48c6a](https://github.com/n0-computer/iroh-docs/commit/7e48c6a20e411c8e5928b5f09fe0ba46d5880ce5))
- Upgrade 0.28 iroh-router - ([933af7a](https://github.com/n0-computer/iroh-docs/commit/933af7af0e38281853042764aaa99ad95327d320))
- Release iroh-docs version 0.28.0 - ([c3017de](https://github.com/n0-computer/iroh-docs/commit/c3017de4573930fd03a05ec4aa6c7ab7a84f1890))


