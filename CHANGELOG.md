# Changelog

## [1.1.0](https://github.com/googleapis/dotnet-spanner-nhibernate/compare/Google.Cloud.Spanner.NHibernate-v1.0.1...Google.Cloud.Spanner.NHibernate-1.1.0) (2022-10-26)


### Features

* add initial version of NHibernate ([a225cd1](https://github.com/googleapis/dotnet-spanner-nhibernate/commit/a225cd16f19990fef13e1ee8fb3f360ac2f85189))
* add schema exporter and updater ([#33](https://github.com/googleapis/dotnet-spanner-nhibernate/issues/33)) ([2648313](https://github.com/googleapis/dotnet-spanner-nhibernate/commit/264831320efaca58ac1abb11ecec1632db88a26c))
* add support for JOIN hints ([#53](https://github.com/googleapis/dotnet-spanner-nhibernate/issues/53)) ([457d249](https://github.com/googleapis/dotnet-spanner-nhibernate/commit/457d2490593a19d0450184de695ade47093c90ad))
* add support for query hints ([#6](https://github.com/googleapis/dotnet-spanner-nhibernate/issues/6)) ([9dc9f75](https://github.com/googleapis/dotnet-spanner-nhibernate/commit/9dc9f75185c8fdc3518ceb53de0b5e52363cdeff))
* enable SchemaValidator usage ([#65](https://github.com/googleapis/dotnet-spanner-nhibernate/issues/65)) ([bb5a7fc](https://github.com/googleapis/dotnet-spanner-nhibernate/commit/bb5a7fc80c9a6e8332c83186d82adad68fffec13))
* support versioning check when using mutations ([#60](https://github.com/googleapis/dotnet-spanner-nhibernate/issues/60)) ([47a31db](https://github.com/googleapis/dotnet-spanner-nhibernate/commit/47a31db8e5023168ca6657ffcab76daf277c36b6))
* use mutations for batches when possible ([#9](https://github.com/googleapis/dotnet-spanner-nhibernate/issues/9)) ([1dea9df](https://github.com/googleapis/dotnet-spanner-nhibernate/commit/1dea9dfbecdede6e07eb0f4c0d74bd246b19334c))
* use netstandard2.0 for libraries ([#69](https://github.com/googleapis/dotnet-spanner-nhibernate/issues/69)) ([42b9a9f](https://github.com/googleapis/dotnet-spanner-nhibernate/commit/42b9a9f6a4de4479381185251eadc612d01f4898))


### Bug Fixes

* add missing override + make method private ([#8](https://github.com/googleapis/dotnet-spanner-nhibernate/issues/8)) ([dbcf45a](https://github.com/googleapis/dotnet-spanner-nhibernate/commit/dbcf45a793a5f89c141766679a20dcd49e227c4e))
* batch count was increased twice ([#63](https://github.com/googleapis/dotnet-spanner-nhibernate/issues/63)) ([522d8e0](https://github.com/googleapis/dotnet-spanner-nhibernate/commit/522d8e05da4b5a3ce36ac31491d649842d7f9058))
* enable batching of versioned data by default ([#68](https://github.com/googleapis/dotnet-spanner-nhibernate/issues/68)) ([97c7ccc](https://github.com/googleapis/dotnet-spanner-nhibernate/commit/97c7ccc224e31e6a9310dca0a251bd9f8bb30edf)), closes [#61](https://github.com/googleapis/dotnet-spanner-nhibernate/issues/61)
* Ephemeral BatchDML commands were not retried if aborted ([#43](https://github.com/googleapis/dotnet-spanner-nhibernate/issues/43)) ([45029da](https://github.com/googleapis/dotnet-spanner-nhibernate/commit/45029dafb28f41bfed4f7bfa41983b0e2fe2e19e)), closes [#28](https://github.com/googleapis/dotnet-spanner-nhibernate/issues/28)
* implement GetHashCode for entities that implement Equals ([#58](https://github.com/googleapis/dotnet-spanner-nhibernate/issues/58)) ([8563066](https://github.com/googleapis/dotnet-spanner-nhibernate/commit/8563066d6ed1955917af89a875a62f4b4233d640))
* mock server tests would always try to connect to emulator ([#30](https://github.com/googleapis/dotnet-spanner-nhibernate/issues/30)) ([584fb84](https://github.com/googleapis/dotnet-spanner-nhibernate/commit/584fb84c0e51efb6e141aa5b68b2f924477bd5b1))
* Removing the script to remove the extra tag ([#124](https://github.com/googleapis/dotnet-spanner-nhibernate/issues/124)) ([0192e54](https://github.com/googleapis/dotnet-spanner-nhibernate/commit/0192e5411b4b828b35dfabb059c3c9c590cdac36))
* Updating master branch to main ([#120](https://github.com/googleapis/dotnet-spanner-nhibernate/issues/120)) ([4a8f098](https://github.com/googleapis/dotnet-spanner-nhibernate/commit/4a8f098c6da8b19a6f51e1c660df1416f985f1be))
* Updating the release from beta to public release ([#122](https://github.com/googleapis/dotnet-spanner-nhibernate/issues/122)) ([1be9380](https://github.com/googleapis/dotnet-spanner-nhibernate/commit/1be9380535388818bef8b4c52aa7aacc896f6f29))

## [1.0.1-beta01](https://github.com/googleapis/dotnet-spanner-nhibernate/compare/Google.Cloud.Spanner.NHibernate-1.0.0-beta01...Google.Cloud.Spanner.NHibernate-1.0.1-beta01) (2022-10-26)


### Bug Fixes

* Updating master branch to main ([#120](https://github.com/googleapis/dotnet-spanner-nhibernate/issues/120)) ([4a8f098](https://github.com/googleapis/dotnet-spanner-nhibernate/commit/4a8f098c6da8b19a6f51e1c660df1416f985f1be))
