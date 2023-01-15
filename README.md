## Indexstar ⭐️

Indexstar is a load splitter for [storetheindex](https://github.com/ipni/storetheindex/).
Inbound requests to an indexstar can query multiple storetheindex nodes and return the union of discovered results.

### Usage

```bash
go run . --listen :8080 --backends http://localhost:8080,http://localhost:8081
```

## Lead Maintainer

[Willscott](https://github.com/willscott)

## Contributing

Contributions are welcome! This repository is part of the IPFS project and therefore governed by our [contributing guidelines](https://github.com/ipfs/community/blob/master/CONTRIBUTING.md).

## License

[SPDX-License-Identifier: Apache-2.0 OR MIT](LICENSE.md)

