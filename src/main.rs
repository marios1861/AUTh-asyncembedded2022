use std::collections::HashMap;
use std::path::Path;

use tokio;
use tokio::fs;
use tokio::sync::broadcast::{self, Receiver, Sender};

use clap::Parser;
use serde_json as json;

use asyncembedded2022::candlestick::Candlestick;
use asyncembedded2022::mav::MovingAverage;
use asyncembedded2022::{get_socket, minute_interval, subscribe_to_symbols, symbol_data, task1, write_delay};
use tokio::time::Instant;

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
    data_opts.create(true).write(true).truncate(true);

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

    // Create a channel to send symbol data to the processing tasks
    // The channel has a hard cap on capacity
    // The trades per minute for each symbol are not known
    // For apple, the trades in 2022 were about 240 per minute, on average
    // We approximate this with a cap of 300 trades, per symbol
    // This is the most important memory consumption of the application
    let (tx, mut rx1): (Sender<json::Value>, Receiver<json::Value>) =
        broadcast::channel(300 * cli.symbols.len());
    let mut rx2 = tx.subscribe();

    // Create tokio tasks for each task of the application
    // loop over the task1 function
    // which attempts to read the stream until it is empty every time it is called
    // in the meanwhile, the other two tasks are automatically scheduled to execute at
    // the soonest possible moment
    let task = tokio::spawn(async move {
        loop {
            task1(&mut ws_stream, &tx, &mut raw_data).await.unwrap();
        }
    });

    // Candlestick generator task (task2)
    let task2_symbols = cli.symbols.clone();
    tokio::spawn(async move {
        loop {
            interval1.tick().await;
            // create a Vec that can hold the amount of rx1 we haven't received yet

            
            let mut tot_data: Vec<json::Value> = Vec::with_capacity(rx1.len());
            // collect all the values we have received
            while let Ok(value) = rx1.try_recv() {
                tot_data.push(value);
            }
            
            if tot_data.len() == 0 {
                continue;
            }

            
            let data_all = symbol_data(&tot_data, &task2_symbols);
            
            for symbol in &task2_symbols {
                let file = candles.get_mut(symbol).unwrap();
                let data = data_all.get(symbol).unwrap();

                // Time task2
                let start = Instant::now();

                let new_candlestick =
                    Candlestick::new(data).expect("Could not generate candlestick!");
                new_candlestick.save(file).await.unwrap();

                write_delay(start, &mut delays2).await.unwrap();
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
            
            
            let mut tot_data: Vec<json::Value> = Vec::with_capacity(rx2.len());
            // collect all the values we have received
            while let Ok(value) = rx2.try_recv() {
                tot_data.push(value);
            }
            
            if tot_data.len() == 0 {
                continue;
            }

            
            let data_all = symbol_data(&tot_data, &task3_symbols);
            
            // process the data with task2
            // we are cold starting the moving average by first collected data of the last 15 minutes
            // and then writing it out
            // if we haven't yet collected 15 minutes of data just append to the queue
            for symbol in &task3_symbols {
                let file = mav.get_mut(symbol).unwrap();
                let mav_val = mav_avgs.get_mut(symbol).unwrap();
                let data = data_all.get(symbol).unwrap();
                
                if !mav_val.is_full() {
                    mav_val.init_update(data);
                } else {
                    // Time task3
                    let start = Instant::now();

                    mav_val.update(data);
                    mav_val.save(file).await.unwrap();

                    write_delay(start, &mut delays3).await.unwrap();
                }
            }
        }
    });

    // await task1 (which is infinite)
    // This allows the tasks to run, else the main thread would terminate since it has 
    // no more work to do after spawing the 3 tasks
    // We could instead spawn 2 tasks and run the first on the main thread
    // But this should not incur any performance penalty since tokio threads are "green"
    // so they are not necessarily linked to os threads, and their overhead is very tiny
    task.await.unwrap();
}
