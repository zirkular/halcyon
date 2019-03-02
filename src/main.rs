extern crate csv;
#[macro_use]
extern crate serde_derive;
extern crate clap; 

use clap::{Arg, App};

use std::error::Error;
use std::process;
use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use std::env;

use std::time::Instant;


#[derive(Debug, Deserialize, Eq, PartialEq)]
struct Week {
    tweetid: String,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct Tweet {
    tweetid: String,
    like_count: String,
    quoted_tweet_tweetid: String,
    in_reply_to_tweetid: String,
    is_retweet: String,
}

fn export_raw(filename: String, limit: u64) -> Result<(), Box<Error>> {
    let input = File::open(filename.clone())?;
    let mut output = File::create(String::from(filename + "." + &limit.to_string()))?;
    let buffered = BufReader::new(input);
    let mut count = 0;

    for line in buffered.lines() {
        if count > limit {
            break;
        }
        let text = line?;
        output.write_all(text.as_bytes())?;
        output.write_all("\n".as_bytes())?;
        count = count + 1;
    }
    Ok(())
}

fn read_dataset(filename: String) -> Result<(), Box<Error>> {
    let mut count = 0;
    let mut tweets = Vec::new();

    println!("Processing {} now. This may take a while...", filename);
    let mut rdr = csv::Reader::from_path(filename)?;
    let mut iter = rdr.deserialize();

    while let Some(result) = iter.next() {
        let tweet: Tweet = result?;
        count = count + 1;
        if tweet.is_retweet == "True" {
            tweets.push(tweet);
        }
    }
    println!("Parsed tweets with filter (quoted_tweet_tweetid, in_reply_to_tweetid): {:?}", tweets.len());
    println!("Parsed tweets total: {:?}", count);
    Ok(())
}

fn main() {
    let about = r"
      /\ \
     / /\ \
    / /__\ \
    \/____\/

    https://zirkular.io
    http://000.graphics
    ";
    let arguments = App::new("halcyon")
       .version("0.1")
       .about(about)
       .author("erak & tekcor")
       .arg(
           Arg::with_name("filename")
                .takes_value(true)
                .required(true)
                .help("Path to the input CSV file.")
        )
       .arg(
           Arg::with_name("export-raw")
                .long("export-raw")
                .takes_value(true)
                .help("Re-exports N rows as 'filename.csv.N' from the input file.")
        )
       .get_matches(); 

    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        println!("{}", "Usage: halcyon [filename]");
        process::exit(1);
    }

    enum Mode {
        Default,
        ExportRaw,
    }

    let filename = args[1].clone();
    let now = Instant::now();
    let mut mode = Mode::Default;
    let mut limit = 0;

    if arguments.is_present("export-raw") {
        mode = Mode::ExportRaw;
        if let Some(arg) = arguments.value_of("export-raw") {
            limit = arg.parse().unwrap();
        }
    }

    match mode {
        Mode::Default => {
            if let Err(err) = read_dataset(filename) {
                println!("Error processing dataset: {}", err);
                process::exit(1);
            }
        },
        Mode::ExportRaw => {
            if let Err(err) = export_raw(filename, limit) {
                println!("Error exporting raw dataset: {}", err);
                process::exit(1);
            }
        },
    }
    println!("Processing took {} seconds.", now.elapsed().as_secs());
}