pub mod candlestick;
pub mod mav;

use std::collections::HashMap;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use candlestick::Candlestick;
use futures::stream::{iter, StreamExt};
use futures::SinkExt;
use mav::MovingAverageData;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::time::{interval, interval_at, Instant, Interval};
use tokio::{fs, net::TcpStream, sync::mpsc::Sender};

use serde_json::{from_str, json, Value};
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tung::Message;
use tungstenite as tung;
use url::Url;

pub fn symbol_data(total_data: &Vec<Value>, symbols: &Vec<String>) -> HashMap<String, Vec<Value>> {
    let mut data_all: HashMap<String, Vec<Value>> = HashMap::new();
    for symbol in symbols {
        data_all.insert(
            symbol.to_string(),
            total_data
                .iter()
                .filter(|val| {
                    val.as_object().unwrap().get("s").unwrap().as_str().unwrap() == symbol
                })
                .map(|val| val.to_owned())
                .collect::<Vec<Value>>(),
        );
    }
    return data_all;
}

pub fn data_from_channel<T>(
    total_data: &Vec<(String, T)>,
    symbols: &Vec<String>,
) -> HashMap<String, Vec<T>>
where
    T: Clone,
{
    let mut data_all: HashMap<String, Vec<T>> = HashMap::new();
    for symbol in symbols {
        data_all.insert(
            symbol.to_string(),
            total_data
                .iter()
                .filter(|&val| &val.0 == symbol)
                .map(|val| val.1.clone())
                .collect(),
        );
    }
    return data_all;
}

pub fn print_socket_error(err: tung::Error) {
    if let tung::Error::Http(res) = err {
        let (_, body) = res.into_parts();
        if let Some(body) = body {
            match std::str::from_utf8(&body) {
                Err(err) => panic!("error: {}", err),
                Ok(val) => panic!("value: {}", val),
            }
        }
    }
}

/**
Asyncronously syncronously reads from the stream and appends each symbol to a file
Reads every 100ms

# Arguments

* `ws_stream` - The finnhub API stream
* `data_folder` - The folder path in which to save the individual symbol data files
*/
pub async fn task1(
    ws_stream: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    channel1: &Sender<(String, Candlestick)>,
    channel2: &Sender<(String, MovingAverageData)>,
    files: &mut HashMap<String, fs::File>,
    symbols: &Vec<String>,
) {
    let mut limiter = interval(Duration::from_millis(100));
    while let Some(Ok(msg)) = ws_stream.next().await {
        limiter.tick().await;
        let msg_copy = msg.clone();
        if let Message::Text(msg) = msg {
            if let Ok(Value::Object(parsed)) = from_str(&msg) {
                if let Some(Value::Array(data)) = parsed.get("data") {
                    // Categorize data by symbol
                    let data_all = symbol_data(data, symbols);

                    for symbol in symbols {
                        if data_all[symbol].len() == 0 {
                            continue;
                        }
                        for datum in &data_all[symbol] {
                            if let Some(f) = files.get_mut(symbol) {
                                f.write_all((datum.to_owned().to_string() + "\n").as_bytes())
                                    .await
                                    .expect("Could not write data to file!");
                            }
                        }
                        let candlestick =
                            Candlestick::new(data_all[symbol].to_owned()).expect("Bad data");
                        channel1
                            .send((symbol.to_owned(), candlestick))
                            .await
                            .expect("Could not send candlestick!");

                        let mavdata = MovingAverageData::new(data_all[symbol].to_owned());
                        channel2
                            .send((symbol.to_owned(), mavdata))
                            .await
                            .expect("Could not send moving average!");
                    }
                } else if let Some(Value::String(type_msg)) = parsed.get("type") {
                    if type_msg == "ping" {
                        continue;
                    } else {
                        println!("Can't find data in message: {msg_copy}");
                    }
                }
            } else {
                println!("msg: {:?}", msg_copy);
                println!("Could not parse msg into a JSON object!");
            }
        } else {
            println!("msg: {:?}", msg_copy);
            println!("Could not convert msg into a string!");
        }
    }
}

pub async fn get_socket(token: &str) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, ()> {
    let url_string = format!("wss://ws.finnhub.io?token={0}", token);
    let url = Url::parse(&url_string).unwrap();

    // Get socket
    let (ws_stream, _) = connect_async(url).await.map_err(print_socket_error)?;
    return Ok(ws_stream);
}

pub async fn subscribe_to_symbols(
    symbols: &Vec<String>,
    ws_stream: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
) -> Result<(), tung::Error> {
    // Create subscription message for each symbol
    let requests: Vec<Message> = symbols
        .iter()
        .map(|symbol| {
            Message::Text(
                json!({
                "type": "subscribe",
                "symbol": symbol
                })
                .to_string(),
            )
        })
        .collect();

    // convert requests list to stream of messages
    let mut messages_iter = iter(requests.into_iter().map(Ok));
    // Subscribe to each symbol (send stream)
    ws_stream.send_all(&mut messages_iter).await?;

    Ok(())
}

/// Create interval for timed tasks
/// Waits until the next clock minute then ticks every minute
pub fn minute_interval() -> Interval {
    let init_delay = Duration::from_secs(
        60 - SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            % 60,
    );
    let start = Instant::now() + init_delay;
    interval_at(start, Duration::from_secs(60))
}

/// Return time elapsed since start
pub async fn write_delay(start: Instant, file: &mut File) -> tokio::io::Result<()> {
    let elapsed = start.elapsed();
    file.write_all((elapsed.as_nanos().to_string() + "\n").as_bytes())
        .await?;
    Ok(())
}
