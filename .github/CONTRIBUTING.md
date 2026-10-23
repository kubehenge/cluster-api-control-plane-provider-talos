# Contributing

Issues and pull requests are welcome. For anything bigger than a small fix, open an issue first so the approach can be agreed before you write it.

- Run `make generate manifests lint unit` before you push; CI also runs the integration tests and builds the image.
- Commit subjects follow [Conventional Commits](https://www.conventionalcommits.org/) (`fix: ...`, `feat: ...`, `chore: ...`) and say what the change does.
- Sign off every commit (`git commit -s`), which certifies the [Developer Certificate of Origin](https://developercertificate.org/).
- New source files carry the MPL-2.0 header from `hack/boilerplate.go.txt`; code copied from Cluster API keeps its Apache-2.0 header.

Security problems are reported privately, as [SECURITY.md](SECURITY.md) describes.
