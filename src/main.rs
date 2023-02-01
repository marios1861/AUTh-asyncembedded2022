use std::collections::HashMap;
use std::path::Path;

use tokio;
use tokio::fs;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time::Instant;

use clap::Parser;

use asyncembedded2022::candlestick::Candlestick;
use asyncembedded2022::mav::{MovingAverage, MovingAverageData};
use asyncembedded2022::{
    data_from_channel, get_socket, minute_interval, subscribe_to_symbols, task1, write_delay,
};

#[derive(Parser)]
#[command(author = "Mariano N. <marios1861@gmail.com>")]
#[command(name = "stock processing app")]
#[command(version)]
#[command(about = "A real time stocks processing tool using data from Finnhub Stock API")]
struct Cli {
    #[arg(short, long)]
    token: String,
    symbols: Vec<String>,
}

// Single threaded runtime (our embedded device is single core!)
#[tokio::main]
async fn main() {
    // Parse commandline args
    let cli = Cli::parse();

    // Get socket
    let mut ws_stream = get_socket(&cli.token).await.unwrap();

    // Subscribe to each given symbol
    subscribe_to_symbols(&cli.symbols, &mut ws_stream)
        .await
        .unwrap();

    // Create data folder (if it doesn't already exist)
    let data_folder = Path::new("./data");
    fs::create_dir_all(data_folder).await.unwrap();

    // Create interval for timed tasks
    let mut interval1 = minute_interval();
    let mut interval2 = minute_interval();

    // file handle options (reate if doesn't exist, truncate to 0 and append)
    let mut data_opts = fs::OpenOptions::new();
    data_opts.create(true).append(true);

    // create delay file handle for task2 and task3
    let mut delays2 = data_opts
        .open(data_folder.join("delays2.json"))
        .await
        .unwrap();

    let mut delays3 = data_opts
        .open(data_folder.join("delays3.json"))
        .await
        .unwrap();

    // Create filehandles for each symbol
    let mut raw_data = HashMap::new();
    let mut candles = HashMap::new();
    let mut mav = HashMap::new();

    for symbol in &cli.symbols {
        raw_data.insert(
            symbol.clone(),
            data_opts
                .open(data_folder.join(format!("{symbol}.json")))
                .await
                .unwrap(),
        );
        candles.insert(
            symbol.clone(),
            data_opts
                .open(data_folder.join(format!("{symbol}_candles.json")))
                .await
                .unwrap(),
        );
        mav.insert(
            symbol.clone(),
            data_opts
                .open(data_folder.join(format!("{symbol}_mav.json")))
                .await
                .unwrap(),
        );
    }

    // We have put a limiter in task1 which doesn't allow for more than 600 batches of data to received
    // per minute. Each batch can contain many data points
    let (tx1, mut rx1): (
        Sender<(String, Candlestick)>,
        Receiver<(String, Candlestick)>,
    ) = mpsc::channel(600);
    let (tx2, mut rx2): (
        Sender<(String, MovingAverageData)>,
        Receiver<(String, MovingAverageData)>,
    ) = mpsc::channel(600);

    // Create tokio tasks for each task of the application
    // two tasks are automatically scheduled to execute at
    // the soonest possible moment from the interval tick

    // Candlestick generator task (task2)
    let task2_symbols = cli.symbols.clone();
    tokio::spawn(async move {
        loop {
            interval1.tick().await;
            // create a Vec that can hold the amount of rx1 we haven't received yet

            let mut tot_data: Vec<(String, Candlestick)> = Vec::new();
            // collect all the values we have received
            while let Ok(value) = rx1.try_recv().map_err(|err| {
                if err != TryRecvError::Empty {
                    println!("Task 2: channel: {err}"); // print actual important errors (it always ends with the "Empty" error)
                }
            }) {
                tot_data.push(value);
            }

            if tot_data.len() == 0 {
                println!("No data received!");
                continue;
            }

            let data_all = data_from_channel(&tot_data, &task2_symbols);

            for symbol in &task2_symbols {
                let file = candles.get_mut(symbol).unwrap();
                let data = data_all.get(symbol).unwrap();

                // Time task2
                let start = Instant::now();

                if let Some(new_candlestick) = Candlestick::combine(data.to_owned()) {
                    new_candlestick
                        .save(file)
                        .await
                        .expect("Could not write candle data!");
                    write_delay(start, &mut delays2)
                        .await
                        .expect("Could not write delay2 data!");
                } else {
                    // no data for this symbol has been received
                    continue;
                }
            }
        }
    });

    // create a moving average queue with a 15 values memory
    let task3_symbols = cli.symbols.clone();
    let mut mav_avgs: HashMap<String, MovingAverage> = HashMap::new();
    for symbol in &task3_symbols {
        mav_avgs.insert(symbol.to_string(), MovingAverage::new(15));
    }

    // Moving average + 15 min vol calculator task (task 3)
    tokio::spawn(async move {
        loop {
            interval2.tick().await;
            // create a Vec that can hold the amount of rx1 we haven't received yet

            let mut tot_data: Vec<(String, MovingAverageData)> = Vec::new();
            // collect all the values we have received
            while let Ok(value) = rx2.try_recv().map_err(|err| {
                if err != TryRecvError::Empty {
                    println!("Task 3: channel: {err}");
                }
            }) {
                tot_data.push(value);
            }

            if tot_data.len() == 0 {
                continue;
            }

            let data_all = data_from_channel(&tot_data, &task3_symbols);

            // process the data with task2
            // we are cold starting the moving average by first collected data of the last 15 minutes
            // and then writing it out
            // if we haven't yet collected 15 minutes of data just append to the queue
            for symbol in &task3_symbols {
                let file = mav.get_mut(symbol).unwrap();
                let mav_val = mav_avgs.get_mut(symbol).unwrap();
                let data = data_all.get(symbol).unwrap();

                // Time task3
                let start = Instant::now();
                if !mav_val.is_full() {
                    if let None = mav_val.init_update(data) {
                        continue; // no data for this symbol has been received
                    }
                } else {
                    if let None = mav_val.update(data) {
                        continue; // no data for this symbol has been received
                    }
                }

                mav_val.save(file).await.expect("Could not write mav data!");

                write_delay(start, &mut delays3)
                    .await
                    .expect("Could not write delay3 data!");
            }
        }
    });

    // await task1 (which is infinite)
    // We spawn tasks 2 and 3 and run the first on the main thread
    task1(&mut ws_stream, &tx1, &tx2, &mut raw_data, &cli.symbols).await;
}
