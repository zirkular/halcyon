extern crate csv;
#[macro_use]
extern crate serde;
extern crate clap;

use clap::{Arg, App};

use std::env;
use std::process;
use std::time::Instant;

mod halcyon;
use halcyon::*;

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
            if let Err(err) = process_and_export(&filename) {
                println!("Error processing dataset: {}", err);
                process::exit(1);
            }
        },
        Mode::ExportRaw => {
            if let Err(err) = export::write_raw(&filename, limit) {
                println!("Error exporting raw dataset: {}", err);
                process::exit(1);
            }
        },
    }
    println!("Processing took {} seconds.", now.elapsed().as_secs());
}