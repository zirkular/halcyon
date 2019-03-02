extern crate csv;
#[macro_use]
extern crate serde_derive;
extern crate clap;
extern crate chrono;

use chrono::{NaiveDateTime};

use clap::{Arg, App};

use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use std::process;

use std::time::Instant;


#[derive(Debug, Deserialize, Eq, PartialEq)]
struct Week {
    tweetid: String,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct Tweet {
    tweetid: String,
    tweet_time: String,
    like_count: String,
    quoted_tweet_tweetid: String,
    in_reply_to_tweetid: String,
    is_retweet: String,
    retweet_tweetid: String,
}

#[derive(Debug, Serialize, Eq, PartialEq)]
struct GPUTweet {
    tweet_id: i64,
    tweet_time: i64,
    ref_tweet_id: i64,
    ref_tweet_time: i64,
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

fn process_dataset(filename: String) -> Result<(), Box<Error>> {
    let output_filename = String::from(filename.clone() + ".out");
    let mut retweet_misses = 0;
    let mut tweets: HashMap<i64, Tweet> = HashMap::new();
    let mut gpu_tweets = Vec::new();

    println!("Processing {} now. This may take a while...", filename);
    let mut wtr = csv::Writer::from_path(output_filename.clone())?;
    let mut rdr = csv::Reader::from_path(filename)?;
    let mut iter = rdr.deserialize();

    while let Some(result) = iter.next() {
        let tweet: Tweet = result?;
        let tweet_id = tweet.tweetid.parse().unwrap();
        tweets.insert(tweet_id, tweet);
    }

    for (key, tweet) in &tweets {
        let mut retweet_id: i64 = 0;
        let mut retweet_time = NaiveDateTime::from_timestamp(0, 0);

        let is_retweet = match tweet.is_retweet.as_ref() {
            "True" => true,
            _ => false,
        };
        let reply_to_tweetid: i64 = match tweet.in_reply_to_tweetid.len() {
            0 => 0,
            _ => match tweet.in_reply_to_tweetid.parse() {
                Ok(number) => number,
                _ => 0
            },
        };

        if is_retweet {
            retweet_id = tweet.retweet_tweetid.parse().unwrap();     
        }
        if reply_to_tweetid > 0 {
            retweet_id = reply_to_tweetid;
        }

        if is_retweet || reply_to_tweetid > 0 {
            match tweets.get(&retweet_id) {
                Some(ref retweet) => {
                    retweet_time = NaiveDateTime::parse_from_str(retweet.tweet_time.as_ref(), "%Y-%m-%d %H:%M")?;
                },
                _ => retweet_misses = retweet_misses + 1,
            }
            let ref_tweet_time = NaiveDateTime::parse_from_str(tweet.tweet_time.as_ref(), "%Y-%m-%d %H:%M")?;

            gpu_tweets.push(GPUTweet{
                tweet_id: retweet_id,
                tweet_time: retweet_time.timestamp(),
                ref_tweet_id: key.clone(),
                ref_tweet_time: ref_tweet_time.timestamp(),
            });
        }
    }
    
    println!("Parsed tweets total: {:?}", &tweets.len());
    println!("Added tweets for export (retweets, replies): {:?}", &gpu_tweets.len());
    println!("Detected missing tweets: {:?}", retweet_misses);
    // 191152
    println!("Writing output to {} now...", output_filename);
    
    for gpu_tweet in gpu_tweets {
        wtr.serialize(gpu_tweet)?;
    }
    
    wtr.flush()?;
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
            if let Err(err) = process_dataset(filename) {
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