# dynamodb_rename

Rename fields (including nested fields) in a DynamoDB database

## Installation

```console
cargo install --git https://github.com/finnbear/dynamodb_bulk_edit
```

## Examples

```console
# Renames all key1's (at the root level) to key2.
dynamo_bulk_edit --table test_table --profile aws_profile --replace key1>key2

# Renames all key1's (at any level) to key2.
dynamo_bulk_edit --table test_table --profile aws_profile --replace *key1>*key2
```

## License

Licensed under either of

* Apache License, Version 2.0
  ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license
  ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.