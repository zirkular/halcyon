extern crate csv;
#[macro_use]
extern crate serde_derive;

use std::error::Error;
use std::io;
use std::process;

use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use std::env;

use std::time::{Duration, Instant};
use std::thread::sleep;

use csv::Reader;

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
}

fn readDataset(filename: String) -> Result<(), Box<Error>> {
    let mut count = 0;
    let mut tweets = Vec::new();

    println!("Processing {} now. This may take a while...", filename);
    let mut rdr = csv::Reader::from_path(filename)?;
    let mut iter = rdr.deserialize();

    while let Some(result) = iter.next() {
        let tweet: Tweet = result?;
        count = count + 1;
        if tweet.quoted_tweet_tweetid.len() != 0 &&
            tweet.in_reply_to_tweetid.len() != 0 {
            tweets.push(tweet);
        }
    }
    println!("Parsed tweets with filter (quoted_tweet_tweetid, in_reply_to_tweetid): {:?}", tweets.len());
    println!("Parsed tweets total: {:?}", count);
    Ok(())
}

fn main() {
    let args: Vec<String> = env::args().collect();

    println!("{}", "");
    println!("{}", "## halcyon ##");
    println!("{}", "");
    println!("{}", r"  /\ \  ");
    println!("{}", r" / /\ \ ");
    println!("{}", r"/ /__\ \");
    println!("{}", r"\/____\/");
    println!("{}", "");
    println!("{}", "https://zirkular.io");
    println!("{}", "http://000.graphics");
    println!("{}", "");

    assert!(args.len() >= 2, "No filename provided.");

    let filename = args[1].clone();
    let now = Instant::now();

    if let Err(err) = readDataset(filename) {
        println!("Error reading dataset: {}", err);
        process::exit(1);
    }
    println!("Processing took {} seconds.", now.elapsed().as_secs());
}