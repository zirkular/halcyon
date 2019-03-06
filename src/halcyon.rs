extern crate csv;
extern crate serde;
extern crate chrono;

use self::chrono::{NaiveDateTime};
use std::collections::HashMap;
use std::error::Error;

pub mod import {
    pub fn unwrap_decimal(number_string: &String) -> f64 {
        let number: f64 = match number_string.len() {
            0 => 0.0,
            _ => match number_string.parse() {
                Ok(number) => number,
                _ => 0.0,
            },
        };
        return number;
    }

    pub fn unwrap_integer(number_string: &String) -> i64 {
        let number: i64 = match number_string.len() {
            0 => 0,
            _ => match number_string.parse() {
                    Ok(number) => number,
                    _ => 0,
                },
        };
        return number;
    }
}

pub mod export {
    extern crate csv;
    extern crate serde;
    
    use serde::Serialize;
    use std::error::Error;

    use std::fs::File;
    use std::io::BufReader;
    use std::io::prelude::*;

    #[derive(Serialize, Deserialize, Debug)]
    pub struct Tweet {
        pub tweetid: String,
        pub tweet_time: String,
        pub quote_count: String,
        pub reply_count: String,
        pub like_count: String,
        pub retweet_count: String,
        pub in_reply_to_tweetid: String,
        pub quoted_tweet_tweetid: String,
        pub is_retweet: String,
        pub retweet_tweetid: String,
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct GPUTweetTime {
        pub tweet_time: i64,
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct GPUTweetScore {
        pub tweet_score: u64,
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct GPUTweetConnection {
        pub tweet_time: i64,
        pub ref_tweet_time: i64,
    }

    pub fn write_csv<T: Serialize>(filename: &String, entries: &Vec<T>) -> Result<(), Box<Error>> {
        let mut wtr = csv::Writer::from_path(filename)?;

        println!("Writing output to {}...", filename);
        for entry in entries {
            wtr.serialize(entry)?;
        }
        wtr.flush()?;
        Ok(())
    }

    pub fn write_raw(filename: &String, limit: u64) -> Result<(), Box<Error>> {
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
}

pub fn process_and_export(filename: &String) -> Result<(), Box<Error>> {
    let output_filename_tweets = String::from(filename.clone() + ".tweets");
    let output_filename_scores = String::from(filename.clone() + ".scores");
    let output_filename_connections = String::from(filename.clone() + ".connections");

    let mut tweets: HashMap<i64, export::Tweet> = HashMap::new();
    let mut gpu_tweets_time = Vec::new();
    let mut gpu_tweets_score = Vec::new();
    let mut gpu_tweets_connection = Vec::new();

    println!("Processing {} now. This may take a while...", filename);
    let mut rdr = csv::Reader::from_path(filename)?;
    let mut iter = rdr.deserialize();

    while let Some(result) = iter.next() {
        let tweet: export::Tweet = result?;
        let tweet_id = tweet.tweetid.parse().unwrap();
        tweets.insert(tweet_id, tweet);
    }

    for (_key, tweet) in &tweets {
        let mut id: i64 = 0;
        let mut retweet_time = NaiveDateTime::from_timestamp(0, 0);
        
        let quote_count: f64 = import::unwrap_decimal(&tweet.quote_count);
        let reply_count: f64 = import::unwrap_decimal(&tweet.reply_count);
        let like_count: f64 = import::unwrap_decimal(&tweet.like_count);
        let retweet_count: f64 = import::unwrap_decimal(&tweet.retweet_count);
        let score = quote_count + reply_count + like_count + retweet_count;
        
        let is_retweet = match tweet.is_retweet.as_ref() {
            "True" => true,
            _ => false,
        };

        let mut is_reply = false;
        let reply_to_tweetid: i64 = import::unwrap_integer(&tweet.in_reply_to_tweetid);
        
        let mut is_quote = false;
        let quoted_tweet_tweetid: i64 = import::unwrap_integer(&tweet.quoted_tweet_tweetid);
        
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

            gpu_tweets_time.push(export::GPUTweetTime {
                tweet_time: ref_tweet_time.timestamp(),
            });

            gpu_tweets_score.push(export::GPUTweetScore { 
                tweet_score: score as u64,
            });

            if is_connected {
                gpu_tweets_connection.push(export::GPUTweetConnection {
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

    export::write_csv(&output_filename_tweets, &gpu_tweets_time)?;
    export::write_csv(&output_filename_scores, &gpu_tweets_score)?;
    export::write_csv(&output_filename_connections, &gpu_tweets_connection)?;
    
    Ok(())
}