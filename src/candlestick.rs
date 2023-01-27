use tokio::fs::File;
use tokio::io;
use tokio::io::AsyncWriteExt;

use serde::Serialize;
use serde_json as json;

#[derive(Serialize)]
pub struct Candlestick {
    init_price: f64,
    last_price: f64,
    max_price: f64,
    min_price: f64,
    tot_vol: i64,
}

impl Candlestick {
    pub fn new(data: &Vec<&json::Value>) -> Option<Candlestick> {
        let tot_vol = data
            .iter()
            .filter_map(|val| val.as_object()?.get("v")?.as_i64())
            .sum();
        let prices: Vec<f64> = data
            .iter()
            .filter_map(|val| val.as_object()?.get("p")?.as_f64())
            .collect();
        let init_price = prices.first()?.clone();
        let last_price = prices.last()?.clone();
        let max_price = prices.iter().cloned().fold(0.0 / 0.0, f64::max);
        let min_price = prices.iter().cloned().fold(0.0 / 0.0, f64::min);

        Some(Candlestick {
            init_price,
            last_price,
            max_price,
            min_price,
            tot_vol,
        })
    }

    pub async fn save(&self, file: &mut File) -> io::Result<()> {
        file.write_all((json::to_string(self)? + "\n").as_bytes())
            .await
    }
}
