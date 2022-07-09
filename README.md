# dynamodb_rename

Rename fields (including nested fields) in a DynamoDB database

**Please don't use this if you can't tolerate losing or corrupting all your data!**

## Installation

```console
cargo install --git https://github.com/finnbear/dynamodb_bulk_edit
```

## Examples

```console
# Renames all key1's (at the root level) to key2.
dynamo_bulk_edit --table test_table --rename "key1>key2"

# Renames all key1's (under obj1) to key2.
dynamo_bulk_edit --table test_table --rename "obj1.key1>obj1.key2"

# Renames all key1's (at any level) to key2.
dynamo_bulk_edit --table test_table --rename "*key1>*key2"
```

You can use the `--profile [name]` argument for credentials.

## Features

- Prints a summary of modifications
- Asks for confirmation before making modifications
- Performs a conditional check to guard against concurrent modification or deletion of attributes.

## Limitations

- Scans the entire table into memory
- If new root-level attributes are added concurrently, they will be lost.

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