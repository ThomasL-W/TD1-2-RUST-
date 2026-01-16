# Rust WebSocket Workshop with Tokio (1h30)
## Building a Real-Time Stock Price Dashboard

### Prerequisites

Students should have completed the async workshop and have:
- Working stock price aggregator with database
- Understanding of `tokio`, async/await, and `tokio::spawn`
- Familiarity with the stock price data model

### Goal

By the end, students will have:
- A WebSocket server that broadcasts live stock prices
- Multiple concurrent client connections
- A browser-based real-time dashboard
- Proper connection lifecycle management
- Broadcasting patterns for pub/sub systems

---

## Workshop Schedule (1h30)

### **Part 1 – WebSocket Basics & First Connection (25 min)**

**Building Block**: Understanding WebSocket protocol and establishing connections

* **Concepts**:
  * What is WebSocket? (Full-duplex communication, upgrade from HTTP)
  * Difference between HTTP polling vs WebSocket
  * WebSocket handshake process
  * Using `tokio-tungstenite` for async WebSocket handling

* **Demo Code**: Minimal WebSocket echo server

```rust
use env_logger::{Builder, Target};
use futures_util::{SinkExt, StreamExt};
use log::{LevelFilter, debug, error, info, trace, warn};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::Message};

async fn handle_connection(stream: TcpStream) {
    let addr = stream
        .peer_addr()
        .expect("connected streams should have a peer address");
    info!("New connection from: {}", addr);

    let ws_stream = match accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            error!("WebSocket handshake failed: {}", e);
            return;
        }
    };

    info!("WebSocket connection established: {}", addr);

    let (mut write, mut read) = ws_stream.split();

    // Echo server: send back whatever we receive
    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                info!("Received: {}", text);
                if write.send(Message::Text(text)).await.is_err() {
                    break;
                }
            }
            Ok(Message::Close(_)) => {
                info!("Client closed connection: {}", addr);
                break;
            }
            Err(e) => {
                error!("WebSocket error: {}", e);
                break;
            }
            _ => {}
        }
    }

    info!("Connection closed: {}", addr);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    Builder::new()
        .target(Target::Stdout)
        .filter_level(LevelFilter::Info)
        .init();

    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    info!("WebSocket server listening on ws://127.0.0.1:8080");

    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(handle_connection(stream));
    }

    Ok(())
}

```

* **Test HTML Client**:

```html
<!DOCTYPE html>
<html>
<head>
    <title>WebSocket Test</title>
</head>
<body>
    <h1>WebSocket Echo Test</h1>
    <input type="text" id="message" placeholder="Type a message">
    <button onclick="send()">Send</button>
    <div id="output"></div>

    <script>
        const ws = new WebSocket('ws://127.0.0.1:8080');

        ws.onopen = () => {
            document.getElementById('output').innerHTML += '<p>Connected!</p>';
        };

        ws.onmessage = (event) => {
            document.getElementById('output').innerHTML +=
                '<p>Received: ' + event.data + '</p>';
        };

        function send() {
            const msg = document.getElementById('message').value;
            ws.send(msg);
            document.getElementById('message').value = '';
        }
    </script>
</body>
</html>
```
> You can also test you ws using the ``
websocat cli
* **Exercise**:
  * Run the echo server and test with the HTML client
  * Modify the server to send a welcome message when a client connects
  * Open multiple browser tabs - observe each connection in the logs

---

### **Part 2 – Broadcasting to Multiple Clients (35 min)**

**Building Block**: Pub/Sub pattern with broadcast channels

* **Concepts**:
  * `tokio::sync::broadcast` for one-to-many communication
  * Managing multiple concurrent WebSocket connections
  * Separating message production from distribution
  * Handling client disconnections gracefully

* **Demo Code**: Broadcast server with shared channel

```rust
use env_logger::Target;
use futures_util::{SinkExt, StreamExt};
use log::{LevelFilter, error, info, warn};
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_tungstenite::{accept_async, tungstenite::Message};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PriceUpdate {
    symbol: String,
    price: f64,
    source: String,
    timestamp: i64,
}

async fn handle_client(stream: TcpStream, mut rx: broadcast::Receiver<PriceUpdate>) {
    let addr = stream
        .peer_addr()
        .expect("connected streams should have a peer address");
    info!("New client connected: {}", addr);

    let ws_stream = match accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            error!("WebSocket handshake failed: {}", e);
            return;
        }
    };

    let (mut write, mut read) = ws_stream.split();

    // Send initial connection message
    let welcome = serde_json::json!({
        "type": "connected",
        "message": "Connected to stock price feed"
    });

    if write
        .send(Message::Text(welcome.to_string().into()))
        .await
        .is_err()
    {
        return;
    }

    loop {
        tokio::select! {
            // Receive broadcasts and send to this client
            Ok(price_update) = rx.recv() => {
                let json = match serde_json::to_string(&price_update) {
                    Ok(j) => j,
                    Err(e) => {
                        error!("Failed to serialize price update: {}", e);
                        continue;
                    }
                };

                if write.send(Message::Text(json.to_string().into())).await.is_err() {
                    info!("Client disconnected: {}", addr);
                    break;
                }
            }

            // Handle incoming messages from client (for control messages)
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        info!("Received from {}: {}", addr, text);
                        // Could handle subscription filters here
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        info!("Client closed connection: {}", addr);
                        break;
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

    info!("Connection handler finished: {}", addr);
}

async fn price_simulator(tx: broadcast::Sender<PriceUpdate>) {
    use rand::Rng;
    use tokio::time::{Duration, interval};

    let mut interval = interval(Duration::from_secs(2));
    let symbols = vec!["AAPL", "GOOGL", "MSFT"];
    let sources = vec!["alpha_vantage", "finnhub"];

    loop {
        interval.tick().await;

        let mut rng = rand::rng();
        let symbol = symbols[rng.random_range(0..symbols.len())];
        let source = sources[rng.random_range(0..sources.len())];
        let price = rng.random_range(100.0..200.0);

        let update = PriceUpdate {
            symbol: symbol.to_string(),
            price,
            source: source.to_string(),
            timestamp: chrono::Utc::now().timestamp(),
        };

        info!("Broadcasting: {} @ ${:.2} from {}", symbol, price, source);

        // Send will fail if no receivers, but that's ok
        let _ = tx.send(update);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .target(Target::Stdout)
        .filter_level(LevelFilter::Info)
        .init();

    // Create broadcast channel (capacity = 100 messages)
    let (tx, _rx) = broadcast::channel::<PriceUpdate>(100);

    // Spawn price simulator task
    let tx_clone = tx.clone();
    tokio::spawn(async move {
        price_simulator(tx_clone).await;
    });

    // Start WebSocket server
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    info!("WebSocket server listening on ws://127.0.0.1:8080");

    while let Ok((stream, _)) = listener.accept().await {
        let rx = tx.subscribe();
        tokio::spawn(handle_client(stream, rx));
    }

    Ok(())
}
```

* **Exercise**:
  * Add a connection counter that tracks active clients
  * Log when clients connect/disconnect with the current count
  * Add a `/stats` message that clients can send to get connection stats
  * Test with 3+ browser tabs simultaneously



---

### **Part 3 – Real-Time Dashboard Integration (30 min)**

**Building Block**: Connecting to real database and building production UI

* **Concepts**:
  * Integrating WebSocket server with existing stock aggregator
  * Database polling vs event-driven updates
  * Building a production-ready dashboard
  * Handling reconnection on the client side

* **Demo Code**: Integration with database

```rust
use env_logger::Target;
use log::{LevelFilter, error, info};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::time::{Duration, interval};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PriceUpdate {
    symbol: String,
    price: f64,
    source: String,
    timestamp: i64,
}

async fn poll_database(
    pool: &PgPool,
    tx: &broadcast::Sender<PriceUpdate>,
) -> Result<(), sqlx::Error> {
    // Get latest prices (one per symbol/source combo)
    let prices = sqlx::query_as::<_, (String, f64, String, i64)>(
        r#"
        SELECT DISTINCT ON (symbol, source)
            symbol, price, source, timestamp
        FROM stock_prices
        ORDER BY symbol, source, timestamp DESC
        "#,
    )
    .fetch_all(pool)
    .await?;

    for (symbol, price, source, timestamp) in prices {
        let update = PriceUpdate {
            symbol,
            price,
            source,
            timestamp,
        };
        let _ = tx.send(update);
    }

    Ok(())
}

async fn database_poller(pool: PgPool, tx: broadcast::Sender<PriceUpdate>) {
    let mut interval = interval(Duration::from_secs(5));

    loop {
        interval.tick().await;

        if let Err(e) = poll_database(&pool, &tx).await {
            error!("Database poll error: {}", e);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .target(Target::Stdout)
        .filter_level(LevelFilter::Info)
        .init();

    // Connect to database
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://user:password@localhost/stockdb".to_string());

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    info!("Connected to database");

    // Create broadcast channel
    let (tx, _rx) = broadcast::channel::<PriceUpdate>(100);

    // Spawn database poller
    let tx_clone = tx.clone();
    tokio::spawn(async move {
        database_poller(pool, tx_clone).await;
    });

    // Start WebSocket server
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    info!("WebSocket server listening on ws://127.0.0.1:8080");

    while let Ok((stream, _)) = listener.accept().await {
        let rx = tx.subscribe();
        tokio::spawn(handle_client(stream, rx));
    }

    Ok(())
}
```

* **Production Dashboard HTML**:

```html
<!DOCTYPE html>
<html>
<head>
    <title>Stock Price Dashboard</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        h1 {
            color: white;
            text-align: center;
            margin-bottom: 10px;
        }
        .status {
            text-align: center;
            color: white;
            margin-bottom: 30px;
            font-size: 14px;
        }
        .status.connected { color: #4ade80; }
        .status.disconnected { color: #f87171; }
        .stock-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
        }
        .stock-card {
            background: white;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            transition: transform 0.2s;
        }
        .stock-card:hover {
            transform: translateY(-5px);
        }
        .stock-card.updated {
            animation: pulse 0.5s;
        }
        @keyframes pulse {
            0%, 100% { transform: scale(1); }
            50% { transform: scale(1.05); }
        }
        .symbol {
            font-size: 24px;
            font-weight: bold;
            color: #667eea;
            margin-bottom: 10px;
        }
        .price {
            font-size: 36px;
            font-weight: bold;
            color: #1f2937;
            margin-bottom: 5px;
        }
        .source {
            font-size: 12px;
            color: #6b7280;
            text-transform: uppercase;
        }
        .timestamp {
            font-size: 11px;
            color: #9ca3af;
            margin-top: 10px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>📈 Real-Time Stock Prices</h1>
        <div class="status" id="status">Connecting...</div>
        <div class="stock-grid" id="stocks"></div>
    </div>

    <script>
        let ws;
        const stocks = new Map();
        const statusEl = document.getElementById('status');
        const stocksEl = document.getElementById('stocks');

        function connect() {
            ws = new WebSocket('ws://127.0.0.1:8080');

            ws.onopen = () => {
                statusEl.textContent = '🟢 Connected';
                statusEl.className = 'status connected';
            };

            ws.onclose = () => {
                statusEl.textContent = '🔴 Disconnected - Reconnecting...';
                statusEl.className = 'status disconnected';
                setTimeout(connect, 3000);
            };

            ws.onerror = (error) => {
                console.error('WebSocket error:', error);
            };

            ws.onmessage = (event) => {
                const data = JSON.parse(event.data);

                if (data.type === 'connected') {
                    console.log(data.message);
                    return;
                }

                // Update stock data
                const key = `${data.symbol}-${data.source}`;
                stocks.set(key, data);
                renderStocks();
            };
        }

        function renderStocks() {
            const sortedStocks = Array.from(stocks.values())
                .sort((a, b) => a.symbol.localeCompare(b.symbol));

            stocksEl.innerHTML = sortedStocks.map(stock => {
                const date = new Date(stock.timestamp * 1000);
                return `
                    <div class="stock-card updated" id="card-${stock.symbol}-${stock.source}">
                        <div class="symbol">${stock.symbol}</div>
                        <div class="price">$${stock.price.toFixed(2)}</div>
                        <div class="source">${stock.source}</div>
                        <div class="timestamp">Updated: ${date.toLocaleTimeString()}</div>
                    </div>
                `;
            }).join('');

            // Remove animation class after animation completes
            setTimeout(() => {
                document.querySelectorAll('.stock-card.updated').forEach(card => {
                    card.classList.remove('updated');
                });
            }, 500);
        }

        connect();
    </script>
</body>
</html>
```

* **Exercise**:
  * Modify to broadcast only when new data arrives (use LISTEN/NOTIFY in PostgreSQL)
  * Add filtering: allow clients to subscribe to specific symbols only
  * Add price change indicators (up/down arrows with percentage change)
  * Deploy: serve the HTML file from the Rust server using `warp` or `axum`

---

## Final Deliverable

Students will have built a complete real-time system with:

- **WebSocket server**: Handles multiple concurrent connections
- **Broadcasting**: Pub/sub pattern using `tokio::sync::broadcast`
- **Database integration**: Polls for new stock prices and broadcasts them
- **Production dashboard**: Browser-based real-time UI with auto-reconnect
- **Proper lifecycle**: Graceful handling of connections and disconnections

### Running the Complete System

```bash
# Terminal 1: Run the stock aggregator (from previous workshop)
cargo run --bin aggregator

# Terminal 2: Run the WebSocket server
cargo run --bin websocket_server

# Terminal 3: Open the dashboard
open dashboard.html
```

### Architecture Diagram

```
┌─────────────────┐
│ Stock Aggregator│ ──> PostgreSQL
└─────────────────┘         │
                            │ Poll every 5s
                            ↓
                    ┌───────────────┐
                    │ WebSocket     │
                    │ Server        │
                    └───────────────┘
                            │
                   broadcast::channel
                            │
        ┌───────────────────┼───────────────────┐
        ↓                   ↓                   ↓
   [Client 1]          [Client 2]          [Client 3]
    Browser             Browser             Browser
```

---

## Extensions (If Time Permits)

- **Subscription Filters**: Let clients subscribe to specific symbols
- **Historical Data**: Send last 10 prices on connection
- **Metrics**: Expose Prometheus metrics for connection count, message rate
- **Compression**: Enable WebSocket compression for large payloads
- **Authentication**: Add token-based auth for WebSocket connections

---

## Key Takeaways

1. **WebSocket vs HTTP**: When to use full-duplex communication
2. **Broadcasting**: One-to-many communication patterns with channels
3. **Connection Management**: Handling lifecycle of long-lived connections
4. **tokio::select!**: Concurrent event handling in async contexts
5. **Production Patterns**: Reconnection, error handling, graceful degradation

---

## Resources

- [Tokio-Tungstenite Docs](https://docs.rs/tokio-tungstenite)
- [Tokio Broadcast Channel](https://docs.rs/tokio/latest/tokio/sync/broadcast/index.html)
- [WebSocket Protocol RFC 6455](https://datatracker.ietf.org/doc/html/rfc6455)
- [MDN WebSocket API](https://developer.mozilla.org/en-US/docs/Web/API/WebSocket)
