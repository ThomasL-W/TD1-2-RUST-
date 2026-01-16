// Modified version of your WebSocket server
// Same functionality, different phrasing, reorganized prints, emojis removed
// Names replaced, code behavior preserved

use dotenv::dotenv;
use futures_util::{SinkExt, StreamExt};
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, postgres::PgPoolOptions, postgres::PgListener, Row};
use std::collections::{HashMap, HashSet};
use std::env;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tokio_tungstenite::{accept_async, tungstenite::Message};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PriceUpdate {
    symbol: String,
    price: f64,
    source: String,
    timestamp: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    change_percent: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    direction: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SubscribeMessage {
    #[serde(rename = "type")]
    msg_type: String,
    symbols: Option<Vec<String>>,
}

type PriceHistory = Arc<RwLock<HashMap<String, f64>>>;

async fn handle_client(
    stream: tokio::net::TcpStream,
    mut rx: broadcast::Receiver<PriceUpdate>,
    price_history: PriceHistory,
) {
    let addr = stream.peer_addr().unwrap_or_else(|_| "unknown".parse().unwrap());
    info!("Client connected from {}", addr);

    let ws_stream = match accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            error!("WebSocket handshake error: {}", e);
            return;
        }
    };

    info!("WebSocket channel established with {}", addr);

    let (mut write, mut read) = ws_stream.split();

    let mut subscribed_symbols: Option<HashSet<String>> = None;

    let welcome = serde_json::json!({
        "type": "connected",
        "message": "WebSocket connection established. Use {type: 'subscribe', symbols: ['AAPL']} to filter updates."
    });

    let _ = write.send(Message::Text(welcome.to_string())).await;

    loop {
        tokio::select! {
            Ok(update) = rx.recv() => {
                if let Some(filter) = &subscribed_symbols {
                    if !filter.contains(&update.symbol) {
                        continue;
                    }
                }

                let json = match serde_json::to_string(&update) {
                    Ok(j) => j,
                    Err(e) => {
                        error!("Serialization error: {}", e);
                        continue;
                    }
                };

                if write.send(Message::Text(json)).await.is_err() {
                    info!("Client {} disconnected", addr);
                    break;
                }
            }

            incoming = read.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        info!("Received from {}: {}", addr, text);

                        if let Ok(msg) = serde_json::from_str::<SubscribeMessage>(&text) {
                            match msg.msg_type.as_str() {
                                "subscribe" => {
                                    if let Some(symbols) = msg.symbols {
                                        subscribed_symbols = Some(symbols.iter().cloned().collect());
                                        let resp = serde_json::json!({"type": "subscribed", "symbols": symbols});
                                        let _ = write.send(Message::Text(resp.to_string())).await;
                                    }
                                }
                                "unsubscribe" => {
                                    subscribed_symbols = None;
                                    let resp = serde_json::json!({"type": "unsubscribed"});
                                    let _ = write.send(Message::Text(resp.to_string())).await;
                                }
                                _ => {}
                            }
                        }
                    }

                    Some(Ok(Message::Close(_))) | None => {
                        info!("Connection closed: {}", addr);
                        break;
                    }

                    Some(Ok(Message::Ping(data))) => {
                        let _ = write.send(Message::Pong(data)).await;
                    }

                    Some(Err(e)) => {
                        warn!("WebSocket error: {}", e);
                        break;
                    }

                    _ => {}
                }
            }
        }
    }

    info!("Client {} connection terminated", addr);
}

async fn fetch_and_broadcast(
    pool: &PgPool,
    tx: &broadcast::Sender<PriceUpdate>,
    price_history: &PriceHistory,
) -> Result<(), sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT ON (symbol, source)
            symbol, price, source, timestamp
        FROM stock_prices
        ORDER BY symbol, source, id DESC
        "#
    )
    .fetch_all(pool)
    .await?;

    info!("Loaded {} price entries from DB", rows.len());

    let mut history = price_history.write().await;

    for row in rows {
        let symbol: String = row.get(0);
        let price: f64 = row.get(1);
        let source: String = row.get(2);
        let timestamp: i64 = row.get(3);

        let key = format!("{}-{}", symbol, source);

        let (change_percent, direction) = if let Some(prev) = history.get(&key) {
            if *prev != 0.0 {
                let diff = ((price - prev) / prev) * 100.0;
                let dir = if diff > 0.0 { "up" } else if diff < 0.0 { "down" } else { "same" };
                (Some(diff), Some(dir.to_string()))
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        history.insert(key, price);

        let update = PriceUpdate {
            symbol: symbol.clone(),
            price,
            source,
            timestamp,
            change_percent,
            direction,
        };

        let _ = tx.send(update);
    }

    Ok(())
}

async fn listen_for_updates(
    pool: PgPool,
    tx: broadcast::Sender<PriceUpdate>,
    price_history: PriceHistory,
) {
    info!("Starting LISTEN handler...");

    let mut listener = match PgListener::connect_with(&pool).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to create listener: {}", e);
            return;
        }
    };

    if let Err(e) = listener.listen("new_price").await {
        error!("LISTEN command failed: {}", e);
        return;
    }

    info!("Listening to PostgreSQL NOTIFY events...");

    loop {
        match listener.recv().await {
            Ok(_) => {
                info!("Database notified of new price, refreshing...");
                let _ = fetch_and_broadcast(&pool, &tx, &price_history).await;
            }
            Err(e) => {
                error!("Listener error: {}", e);
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    env_logger::Builder::from_default_env().init();

    info!("Starting WebSocket server...");

    let database_url = env::var("DATABASE_URL").expect("Missing DATABASE_URL");
    let pool = PgPoolOptions::new().max_connections(5).connect(&database_url).await?;

    info!("Connected to database");

    let price_history: PriceHistory = Arc::new(RwLock::new(HashMap::new()));
    let tx = broadcast::channel::<PriceUpdate>(100).0;

    fetch_and_broadcast(&pool, &tx, &price_history).await?;

    let listener_pool = pool.clone();
    let listener_tx = tx.clone();
    let listener_history = price_history.clone();

    tokio::spawn(async move {
        listen_for_updates(listener_pool, listener_tx, listener_history).await;
    });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
    info!("WebSocket server running on ws://127.0.0.1:8080");

    while let Ok((stream, _)) = listener.accept().await {
        let rx = tx.subscribe();
        let history_clone = price_history.clone();
        tokio::spawn(handle_client(stream, rx, history_clone));
    }

    Ok(())
}
