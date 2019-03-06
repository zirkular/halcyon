extern crate csv;
#[macro_use]
extern crate serde;
extern crate clap;
extern crate chrono;

use chrono::{NaiveDateTime};

use clap::{Arg, App};

use serde::Serialize;

use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use std::process;

use std::time::Instant;


#[derive(Serialize, Deserialize, Debug)]
struct Week {
    tweetid: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Tweet {
    tweetid: String,
    tweet_time: String,
    quote_count: String,
    reply_count: String,
    like_count: String,
    retweet_count: String,
    in_reply_to_tweetid: String,
    quoted_tweet_tweetid: String,
    is_retweet: String,
    retweet_tweetid: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct GPUTweetTime {
    tweet_time: i64,
}

#[derive(Serialize, Deserialize, Debug)]
struct GPUTweetScore {
    tweet_score: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct GPUTweetConnection {
    tweet_time: i64,
    ref_tweet_time: i64,
}

fn unwrap_decimal(number_string: &String) -> f64 {
    let number: f64 = match number_string.len() {
        0 => 0.0,
        _ => match number_string.parse() {
            Ok(number) => number,
            _ => 0.0,
        },
    };
    return number;
}

fn unwrap_integer(number_string: &String) -> i64 {
    let number: i64 = match number_string.len() {
        0 => 0,
        _ => match number_string.parse() {
                Ok(number) => number,
                _ => 0,
            },
    };
    return number;
}

fn export_raw(filename: &String, limit: u64) -> Result<(), Box<Error>> {
    let input = File::open(filename)?;
    let mut output = File::create(String::from(filename.clone() + "." + &limit.to_string()))?;
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

fn write_csv<T: Serialize>(filename: &String, entries: &Vec<T>) -> Result<(), Box<Error>>
{
    let mut wtr = csv::Writer::from_path(filename)?;

    println!("Writing output to {}...", filename);
    for entry in entries {
        wtr.serialize(entry)?;
    }
    wtr.flush()?;
    Ok(())
}

fn process_dataset(filename: &String) -> Result<(), Box<Error>> {
    let output_filename_tweets = String::from(filename.clone() + ".tweets");
    let output_filename_scores = String::from(filename.clone() + ".scores");
    let output_filename_connections = String::from(filename.clone() + ".connections");

    let mut tweets: HashMap<i64, Tweet> = HashMap::new();
    let mut gpu_tweets_time = Vec::new();
    let mut gpu_tweets_score = Vec::new();
    let mut gpu_tweets_connection = Vec::new();

    println!("Processing {} now. This may take a while...", filename);
    let mut rdr = csv::Reader::from_path(filename)?;
    let mut iter = rdr.deserialize();

    while let Some(result) = iter.next() {
        let tweet: Tweet = result?;
        let tweet_id = tweet.tweetid.parse().unwrap();
        tweets.insert(tweet_id, tweet);
    }

    for (_key, tweet) in &tweets {
        let mut id: i64 = 0;
        let mut retweet_time = NaiveDateTime::from_timestamp(0, 0);
        
        let quote_count: f64 = unwrap_decimal(&tweet.quote_count);
        let reply_count: f64 = unwrap_decimal(&tweet.reply_count);
        let like_count: f64 = unwrap_decimal(&tweet.like_count);
        let retweet_count: f64 = unwrap_decimal(&tweet.retweet_count);
        let score = quote_count + reply_count + like_count + retweet_count;
        
        let is_retweet = match tweet.is_retweet.as_ref() {
            "True" => true,
            _ => false,
        };

        let mut is_reply = false;
        let reply_to_tweetid: i64 = unwrap_integer(&tweet.in_reply_to_tweetid);
        
        let mut is_quote = false;
        let quoted_tweet_tweetid: i64 = unwrap_integer(&tweet.quoted_tweet_tweetid);
        
        if is_retweet {
            id = tweet.retweet_tweetid.parse().unwrap();     
        }
        if reply_to_tweetid > 0 {
            id = reply_to_tweetid;
            is_reply = true;
        }
        if quoted_tweet_tweetid > 0 {
            id = quoted_tweet_tweetid;
            is_quote = true;
        }

        if score > 0.0 || is_retweet || is_reply || is_quote { 
            let mut is_connected = true;
            match tweets.get(&id) {
                Some(ref retweet) => {
                    retweet_time = NaiveDateTime::parse_from_str(retweet.tweet_time.as_ref(), "%Y-%m-%d %H:%M")?;
                },
                _ => is_connected = false,
            }
            let ref_tweet_time = NaiveDateTime::parse_from_str(tweet.tweet_time.as_ref(), "%Y-%m-%d %H:%M")?;

            gpu_tweets_time.push(GPUTweetTime {
                tweet_time: ref_tweet_time.timestamp(),
            });

            gpu_tweets_score.push(GPUTweetScore { 
                tweet_score: score as u64,
            });

            if is_connected {
                gpu_tweets_connection.push(GPUTweetConnection {
                    tweet_time: retweet_time.timestamp(),
                    ref_tweet_time: ref_tweet_time.timestamp(),
                });
            }
        }
    }
    println!("Parsed tweets total: {:?}", &tweets.len());
    println!("Added tweets for export: {:?}", &gpu_tweets_time.len());
    println!("Added scores for export: {:?}", &gpu_tweets_score.len());
    println!("Added connections for export: {:?}", &gpu_tweets_connection.len());

    write_csv(&output_filename_tweets, &gpu_tweets_time)?;
    write_csv(&output_filename_scores, &gpu_tweets_score)?;
    write_csv(&output_filename_connections, &gpu_tweets_connection)?;
    
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
            if let Err(err) = process_dataset(&filename) {
                println!("Error processing dataset: {}", err);
                process::exit(1);
            }
        },
        Mode::ExportRaw => {
            if let Err(err) = export_raw(&filename, limit) {
                println!("Error exporting raw dataset: {}", err);
                process::exit(1);
            }
        },
    }
    println!("Processing took {} seconds.", now.elapsed().as_secs());
}