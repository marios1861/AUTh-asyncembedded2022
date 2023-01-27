use asyncembedded2022::mav::MovingAverage;
use asyncembedded2022::{display_socket_error, symbol_data};
use clap::Parser;
use futures::sink::SinkExt;
use futures::stream::{iter, StreamExt};
use serde_json as json;
use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio;
use tokio::io::AsyncWriteExt;
use tokio::sync::broadcast::{self, Receiver, Sender};
use tokio::time::{interval_at, Duration, Instant};
use tokio::{fs, net::TcpStream};
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};
use url::Url;

use asyncembedded2022::candlestick::Candlestick;

#[derive(Parser)]
#[command(author = "Mariano N. <marios1861@gmail.com>")]
#[command(name = "stock processing app")]
#[command(version = "0.1")]
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

    // Get Url
    let url_string = format!("wss://ws.finnhub.io?token={0}", cli.token);
    let url = Url::parse(&url_string).unwrap();

    // Get socket
    let (mut ws_stream, _) = connect_async(url)
        .await
        .map_err(display_socket_error)
        .unwrap();

    // Create subscription message for each symbol
    let requests: Vec<Message> = cli
        .symbols
        .iter()
        .map(|sym| {
            Message::Text(
                json::json!({
                "type": "subscribe",
                "symbol": sym
                })
                .to_string(),
            )
        })
        .collect();

    // convert requests list to stream of messages
    let mut messages_iter = iter(requests.into_iter().map(Ok));
    // Subscribe to each symbol (send stream)
    ws_stream.send_all(&mut messages_iter).await.unwrap();

    // Create data folder (if it doesn't already exist)
    let data_folder = Path::new("./data");
    fs::create_dir_all(data_folder).await.unwrap();

    // Create interval for timed tasks
    // Waits until the next clock minute then ticks every minute
    let init_delay = Duration::from_secs(
        60 - SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            % 60,
    );
    let start = Instant::now() + init_delay;
    let mut interval1 = interval_at(start, Duration::from_secs(60));
    let mut interval2 = interval_at(start, Duration::from_secs(60));
    // Create filehandles for each symbol
    let mut data_opts = fs::OpenOptions::new();
    data_opts.create(true).append(true);
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
        let task = tokio::spawn(async move {
            task1(&mut ws_stream, &tx, &mut raw_data).await.unwrap();
        });
        
        // Candlestick generator task
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
                let new_candlestick =
                    Candlestick::new(data).expect("Could not generate candlestick!");
                new_candlestick.save(file).await.unwrap();
            }
        }
    });

    // create a moving average queue with a 15 values memory
    let task3_symbols = cli.symbols.clone();
    let mut mav_avgs: HashMap<String, MovingAverage> = HashMap::new();
    for symbol in &task3_symbols {
        mav_avgs.insert(symbol.to_string(), MovingAverage::new(15));
    }
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
                    mav_val.update(data);
                    mav_val.save(file).await.unwrap();
                }
            }
        }
    });

    task.await.unwrap();
}

/// Asyncronously syncronously reads from the stream and appends each symbol to a file
///
/// # Arguments
///
/// * `ws_stream` - The finnhub API stream
/// * `data_folder` - The folder path in which to save the individual symbol data files
async fn task1(
    ws_stream: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    channel: &Sender<json::Value>,
    files: &mut HashMap<String, fs::File>,
) -> Result<(), Box<dyn std::error::Error>> {
    while let Some(Ok(msg)) = ws_stream.next().await {
        if let json::Value::Object(parsed) = json::from_str(&msg.into_text()?)? {
            if let Some(json::Value::Array(data)) = parsed.get("data") {
                for datum_json in data {
                    if let json::Value::Object(datum) = datum_json {
                        if let Some(json::Value::String(symbol)) = datum.get("s") {
                            if let Some(f) = files.get_mut(symbol) {
                                f.write_all(datum_json.to_owned().to_string().as_bytes())
                                    .await?;
                                channel.send(datum_json.to_owned())?;
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }
        // println!("Received message: {:?}", parsed);
    }
    return Err(Box::new(io::Error::new(
        io::ErrorKind::Other,
        "Something went wrong!",
    )));
}
