pub mod candlestick;
pub mod mav;

use serde_json::Value;
use std::collections::HashMap;
use tungstenite as tung;

pub fn symbol_data<'a>(
    total_data: &'a Vec<Value>,
    symbols: &Vec<String>,
) -> HashMap<String, Vec<&'a Value>> {
    let mut data_all: HashMap<String, Vec<&Value>> = HashMap::new();
    for symbol in symbols {
        data_all.insert(
            symbol.to_string(),
            total_data
                .iter()
                .filter(|val| {
                    val.as_object().unwrap().get("s").unwrap().as_str().unwrap() == symbol
                })
                .collect(),
        );
    }
    return data_all;
}

pub fn display_socket_error(err: tung::Error) {
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
