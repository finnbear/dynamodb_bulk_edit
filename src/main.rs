use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::timeout;
use aws_config::timeout::Api;
use aws_sdk_dynamodb::error::{PutItemError, ScanError};
use aws_sdk_dynamodb::model::AttributeValue;
use aws_sdk_dynamodb::types::SdkError;
use aws_sdk_dynamodb::{Client, Region};
use aws_smithy_types::tristate::TriState;
use regex::{Match, Regex};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::io::BufRead;
use std::str::FromStr;
use std::time::Duration;
use std::{io, process};
use structopt::lazy_static::lazy_static;
use structopt::StructOpt;

#[derive(StructOpt)]
struct Options {
    #[structopt(long)]
    region: Option<String>,
    #[structopt(long)]
    profile: Option<String>,
    #[structopt(long)]
    timeout: Option<u64>,
    #[structopt(long)]
    table: String,
    #[structopt(long)]
    replace: Vec<Replace>,
}

struct Replace {
    root: bool,
    prefix: String,
    from: String,
    to: String,
}

#[derive(Debug)]
enum ReplaceParseError {
    MissingArrow,
    InvalidAttribute(String),
    Unsupported,
}

impl Display for ReplaceParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplaceParseError::MissingArrow => f.write_str("replacement missing arrow ('>')"),
            ReplaceParseError::InvalidAttribute(a) => {
                f.write_fmt(format_args!("attribute '{}' is invalid", a))
            }
            ReplaceParseError::Unsupported => {
                f.write_str("replacements that that move values are not yet supported")
            }
        }
    }
}

impl FromStr for Replace {
    type Err = ReplaceParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((mut before, mut after)) = s.split_once('>') {
            let root = if before.starts_with("*") {
                before = &before[1..];
                if after.starts_with("*") {
                    after = &after[1..];
                } else {
                    return Err(ReplaceParseError::Unsupported);
                }
                false
            } else {
                true
            };

            fn validate_attribute_name(name: &str) -> Result<(), ReplaceParseError> {
                lazy_static! {
                    static ref NAME_REGEX: Regex = Regex::new("[a-zA-Z0-9_\\-.]+").unwrap();
                }

                if NAME_REGEX
                    .find(name)
                    .map(|m: Match| m.start() == 0 && m.end() == name.len())
                    .unwrap_or(false)
                {
                    Ok(())
                } else {
                    Err(ReplaceParseError::InvalidAttribute(name.to_string()))
                }
            }

            validate_attribute_name(before)?;
            validate_attribute_name(after)?;

            let (prefix, from, to) = if before.contains('.') {
                let from = before.split('.').last().unwrap().to_string();
                let prefix = before
                    .strip_suffix(&format!(".{}", from))
                    .unwrap()
                    .to_string();
                let to = after
                    .strip_prefix(&format!("{}.", prefix))
                    .ok_or(ReplaceParseError::Unsupported)?
                    .to_string();
                (prefix, from, to)
            } else {
                if after.contains('.') {
                    return Err(ReplaceParseError::Unsupported);
                }
                (String::new(), before.to_string(), after.to_string())
            };

            Ok(Self {
                root,
                prefix,
                from,
                to,
            })
        } else {
            Err(ReplaceParseError::MissingArrow)
        }
    }
}

#[tokio::main]
async fn main() {
    let options: Options = Options::from_args();
    let mut credentials_builder = DefaultCredentialsChain::builder();

    if let Some(region) = options.region {
        credentials_builder = credentials_builder.region(Region::new(Cow::Owned(region)));
    }
    if let Some(profile) = options.profile {
        credentials_builder = credentials_builder.profile_name(&profile);
    }

    let credentials_provider = credentials_builder.build().await;

    let mut shared_config_loader =
        aws_config::from_env().credentials_provider(credentials_provider);

    if let Some(timeout) = options.timeout {
        let timeout = Duration::from_secs(timeout);
        shared_config_loader =
            shared_config_loader.timeout_config(timeout::Config::new().with_api_timeouts(
                Api::new()
                    .with_call_timeout(TriState::Set(timeout))
                    .with_call_attempt_timeout(TriState::Set(timeout)),
            ))
    }

    let shared_config = shared_config_loader.load().await;

    let client = Client::new(&shared_config);

    let rows = match scan(&client, &options.table).await {
        Ok(rows) => rows,
        Err(e) => {
            eprintln!("error scanning: {}", e.to_string());
            process::exit(1);
        }
    };

    eprintln!("scanned {} row(s) in table...", rows.len());

    let mut result = ReplaceResult::default();
    let mut dirty = Vec::new();
    for mut row in rows {
        let old = row.clone();
        replace(String::new(), &mut row, &options.replace, &mut result);
        if old != row {
            dirty.push((old, row));
        }
    }

    if result.replacements == 0 {
        eprintln!("no replacements found.");
        return;
    }

    eprintln!(
        "prepared to make {} replacement(s) across {} item(s) with {} overwritten key(s)...",
        result.replacements,
        dirty.len(),
        result.overwrites
    );

    eprint!("confirm (type 'Y' and press 'Enter'): ");

    let mut line = String::new();
    let stdin = io::stdin();
    stdin
        .lock()
        .read_line(&mut line)
        .expect("could not read line from stdin");

    if line.trim() != "Y" {
        println!("canceled.");
        process::exit(1);
    }

    let mut count = 0;
    for (old, new) in dirty {
        if let Err(e) = put(&client, old, new, &options.table).await {
            let e_string = e.to_string();
            let compat = e.into();
            if matches!(
                compat,
                aws_sdk_dynamodb::Error::ConditionalCheckFailedException(_)
            ) {
                eprintln!("after {} successfully updated items(s), concurrent modification detected. retry if desired.", count);
            } else {
                eprintln!(
                    "after {} successfully updated item(s), error putting item: {}",
                    count, e_string
                );
            }
            process::exit(1);
        } else {
            count += 1;
        }
    }

    eprintln!("successfully updated {} items.", count);
}

#[derive(Debug, Default)]
struct ReplaceResult {
    replacements: usize,
    overwrites: usize,
}

fn replace(
    path: String,
    attribute: &mut HashMap<String, AttributeValue>,
    replacements: &[Replace],
    result: &mut ReplaceResult,
) {
    for replacement in replacements {
        if path == replacement.prefix || (!replacement.root && path.ends_with(&replacement.prefix))
        {
            if let Some(value) = attribute.remove(&replacement.from) {
                result.replacements += 1;
                result.overwrites +=
                    attribute.insert(replacement.to.clone(), value).is_some() as usize;
            }
        }
    }

    for (key, value) in attribute {
        if let AttributeValue::M(map) = value {
            let new_path = if path.is_empty() {
                key.clone()
            } else {
                path.clone() + "." + key
            };
            replace(new_path, map, replacements, result);
        }
    }
}

async fn scan_inner(
    client: &Client,
    table: &str,
    last_evaluated_key: Option<HashMap<String, AttributeValue>>,
) -> Result<
    (
        Vec<HashMap<String, AttributeValue>>,
        Option<HashMap<String, AttributeValue>>,
    ),
    SdkError<ScanError>,
> {
    let scan_output = match client
        .scan()
        .table_name(table)
        .set_exclusive_start_key(last_evaluated_key)
        .send()
        .await
    {
        Ok(output) => output,
        Err(e) => return Err(e),
    };

    Ok((
        scan_output.items.unwrap_or_default(),
        scan_output.last_evaluated_key,
    ))
}

async fn scan(
    client: &Client,
    table: &str,
) -> Result<Vec<HashMap<String, AttributeValue>>, SdkError<ScanError>> {
    let mut ret = Vec::new();
    let mut last_evaluated_key = None;
    loop {
        match scan_inner(client, table, last_evaluated_key).await {
            Err(e) => return Err(e),
            Ok((mut items, lek)) => {
                ret.append(&mut items);
                last_evaluated_key = lek;

                if last_evaluated_key.is_none() {
                    break;
                }
            }
        }
    }

    Ok(ret)
}

async fn put(
    client: &Client,
    old: HashMap<String, AttributeValue>,
    item: HashMap<String, AttributeValue>,
    table: &str,
) -> Result<(), SdkError<PutItemError>> {
    let mut req = client.put_item().table_name(table).set_item(Some(item));
    let mut expr = Vec::new();
    let mut i = 0;
    for (key, value) in old {
        expr.push(format!("#a{} = :a{}", i, i));
        req = req
            .expression_attribute_names(format!("#a{}", i), key)
            .expression_attribute_values(format!(":a{}", i), value);
        i += 1;
    }
    if !expr.is_empty() {
        req = req.condition_expression(expr.join(" AND "));
    }
    req.send().await.map(|_| ())
}
