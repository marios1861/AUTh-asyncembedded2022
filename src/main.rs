use clap::Parser;
use futures::stream::{StreamExt, iter};
use futures::sink::SinkExt;
use serde_json as json;
use tokio;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;

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

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Get Url
    let url_string = format!("wss://ws.finnhub.io?token={0}", cli.token);
    let url = Url::parse(&url_string).unwrap();

    let (mut ws_stream, _) = connect_async(url).await.unwrap();

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

    // Subscribe to each symbol
    let mut messages_iter = iter(requests.into_iter().map(Ok));
    ws_stream.send_all(&mut messages_iter).await.unwrap();

    while let Some(Ok(msg)) = ws_stream.next().await {
        let parsed: json::Value = json::from_str(&msg.into_text().unwrap()).expect("Can't parse JSON!");
        println!("Received message: {:?}", parsed);
    }
}
