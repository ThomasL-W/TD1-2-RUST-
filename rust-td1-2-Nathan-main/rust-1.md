# 🦀 Rust Async Workshop with Tokio (4 hours)
## Building a Multi-Platform Stock Price Aggregator

### Audience

Requires Rust basics (ownership, structs, enums, functions, basic error handling).

### Goal

By the end, students will have built a production-ready stock price aggregator that:
- Fetches prices from multiple APIs every minute
- Processes multiple platforms concurrently
- Stores data in PostgreSQL
- Handles graceful shutdown with signal handling
- Uses structured logging with `tracing`

---

## 📅 Workshop Schedule (4 hours)

### **Part 1 – Intro to Async & Tokio Runtime (30 min)**

**Building Block**: Understanding async foundations

* **Concepts**:
  * What is async/await in Rust? (Future-based execution, cooperative multitasking)
  * Why we need a runtime like Tokio
  * Difference between sync vs async IO
  * How this applies to fetching stock prices

* **Demo Code**: Minimal async example with `tokio::main`

```rust
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() {
    println!("Starting stock price simulator...");

    let fetch_price = async {
        sleep(Duration::from_millis(500)).await;
        println!("AAPL: $150.25");
    };

    fetch_price.await;
}
```

* **Exercise**:
  * Create an async function `fetch_mock_price(symbol: &str) -> f64` that sleeps for 500ms and returns a random price
  * Call it for 3 different stock symbols sequentially
  * Observe the total time taken

> When installing tokio, enable all features: `tokio = { version = "1.47.1", features = ["full"] }`
---

### **Part 2 – Async API Calls & Parallel Fetching (60 min)**

**Building Block**: Fetching real stock prices from multiple sources

* **Concepts**:
  * Using `reqwest` for async HTTP requests
  * Parsing JSON with `serde`
  * Error handling with `Result` and `?`
  * Running multiple API calls in parallel with `tokio::join!`

* **Demo Code**: Fetch stock price from Alpha Vantage API

```rust
use reqwest;
use serde::Deserialize;
use std::env;

#[derive(Deserialize, Debug)]
struct GlobalQuote {
    #[serde(rename = "Global Quote")]
    quote: Quote,
}

#[derive(Deserialize, Debug)]
struct Quote {
    #[serde(rename = "01. symbol")]
    symbol: String,
    #[serde(rename = "05. price")]
    price: String,
}

async fn fetch_alpha_vantage(symbol: &str) -> Result<f64, Box<dyn std::error::Error>> {
    let api_key = env::var("ALPHA_VANTAGE_KEY")?;
    let url = format!(
        "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={}&apikey={}",
        symbol, api_key
    );

    let resp = reqwest::get(&url)
        .await?
        .json::<GlobalQuote>()
        .await?;

    Ok(resp.quote.price.parse()?)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let price = fetch_alpha_vantage("AAPL").await?;
    println!("AAPL price: ${:.2}", price);
    Ok(())
}
```

* **Exercise**:
  * Create a second function `fetch_finnhub(symbol: &str)` for Finnhub API
  * Create a struct `StockPrice { symbol: String, price: f64, source: String, timestamp: i64 }`
  * Fetch the same stock from both APIs in parallel using `tokio::join!`
  * Compare the results

> When installing reqwest, enable json feature: `reqwest = { version = "0.12.23", features = ["json"] }`

---

### **Part 3 – Database Integration with PostgreSQL (50 min)**

**Building Block**: Persisting stock prices

* **Concepts**:
  * Using `sqlx` for async database operations
  * Database connection pooling
  * SQL migrations
  * Inserting data efficiently

* **Setup**: Create database and table

```sql
CREATE TABLE stock_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    source VARCHAR(50) NOT NULL,
    timestamp BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_symbol_timestamp ON stock_prices(symbol, timestamp DESC);
```

* **Demo Code**: Connect to PostgreSQL and insert prices

```rust
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

async fn save_price(pool: &PgPool, price: &StockPrice) -> Result<(), sqlx::Error> {
    // TODO: use sqlx::query to insert the new price
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgresql://user:password@localhost/stockdb")
        .await?;

    let price = StockPrice {
        symbol: "AAPL".to_string(),
        price: 150.25,
        source: "alpha_vantage".to_string(),
        timestamp: chrono::Utc::now().timestamp(),
    };

    save_price(&pool, &price).await?;
    println!("Saved price to database");

    Ok(())
}
```

* **Exercise**:
  * Modify the code to save the price in database
  * Fetch prices from 2 APIs for 3 stocks and save all 6 results to the database
  * Query the database to verify the data was saved

---

### **Part 4 – Complete Application: Periodic Fetching & Graceful Shutdown (75 min)**

**Building Block**: Putting it all together

* **Concepts**:
  * Using `tokio::time::interval` for periodic tasks
  * `tokio::select!` for handling multiple concurrent operations
  * Graceful shutdown with `tokio::signal::ctrl_c()`
  * Structured logging with `tracing`
  * Proper cleanup of resources

* **Demo Code**: Complete stock price aggregator

```rust
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tokio::time::{interval, Duration};
use tokio::signal;
use tracing::{info, error, instrument};
use chrono::Utc;

#[derive(Debug, Clone)]
struct StockPrice {
    symbol: String,
    price: f64,
    source: String,
    timestamp: i64,
}

struct Config {
    symbols: Vec<String>,
    sources: Vec<String>,
    database_url: String,
}

#[instrument(skip(pool))]
async fn save_price(pool: &PgPool, price: &StockPrice) -> Result<(), sqlx::Error> {
    // TODO: save the price in the database
}

#[instrument]
async fn fetch_alpha_vantage(symbol: &str) -> Result<StockPrice, Box<dyn std::error::Error>> {
    // TODO: Get API key from environment variable
    // TODO: Build the Alpha Vantage URL with GLOBAL_QUOTE function
    // TODO: Make HTTP GET request using reqwest
    // TODO: Parse JSON response and extract price from "05. price" field
    // TODO: Return StockPrice struct with current timestamp
    todo!("Implement Alpha Vantage API call")
}

#[instrument]
async fn fetch_finnhub(symbol: &str) -> Result<StockPrice, Box<dyn std::error::Error>> {
    // TODO: Get API key from environment variable
    // TODO: Build the Finnhub URL (/quote endpoint)
    // TODO: Make HTTP GET request with X-Finnhub-Token header
    // TODO: Parse JSON response and extract "c" (current price) field
    // TODO: Return StockPrice struct with current timestamp
    todo!("Implement Finnhub API call")
}

#[instrument(skip(pool))]
async fn fetch_and_save_all(pool: &PgPool, symbols: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting fetch cycle for {} symbols", symbols.len());

    for symbol in symbols {
        // Fetch from multiple sources in parallel
        let (alpha_result, finnhub_result) = tokio::join!(
            fetch_alpha_vantage(symbol),
            fetch_finnhub(symbol)
        );

        // Save results
        if let Ok(price) = alpha_result {
            if let Err(e) = save_price(pool, &price).await {
                error!("Failed to save alpha_vantage price: {}", e);
            }
        }

        if let Ok(price) = finnhub_result {
            if let Err(e) = save_price(pool, &price).await {
                error!("Failed to save finnhub price: {}", e);
            }
        }
    }

    info!("Completed fetch cycle");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup tracing
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    info!("Starting stock price aggregator");

    // Configuration
    let config = Config {
        symbols: vec!["AAPL".to_string(), "GOOGL".to_string(), "MSFT".to_string()],
        sources: vec!["alpha_vantage".to_string(), "finnhub".to_string()],
        database_url: "postgresql://user:password@localhost/stockdb".to_string(),
    };

    // Setup database connection pool
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&config.database_url)
        .await?;

    info!("Connected to database");

    // Create interval for periodic fetching (every minute)
    let mut fetch_interval = interval(Duration::from_secs(60));

    // Main loop
    loop {
        tokio::select! {
            _ = fetch_interval.tick() => {
                if let Err(e) = fetch_and_save_all(&pool, &config.symbols).await {
                    error!("Error during fetch cycle: {}", e);
                }
            }
            _ = signal::ctrl_c() => {
                info!("Shutdown signal received");
                break;
            }
        }
    }

    // Graceful shutdown
    info!("Closing database connections...");
    pool.close().await;
    info!("Shutdown complete");

    Ok(())
}
```

* **Exercise**:
  * Add a third data source (e.g., Yahoo Finance API or mock)
  * Implement error recovery: if one source fails, still save data from others
  * Add a query endpoint that shows the latest price for each symbol
  * Implement a `--fetch-once` CLI flag that fetches once and exits (useful for testing)


---

## ✅ Final Deliverable

Students will have a complete, production-ready application with:

- **Multi-source data fetching**: Pulls from Alpha Vantage, Finnhub, and custom sources
- **Parallel processing**: Fetches multiple stocks and sources concurrently
- **Periodic execution**: Runs every minute using `tokio::time::interval`
- **Database persistence**: Stores all prices with timestamps in PostgreSQL
- **Graceful shutdown**: Properly handles Ctrl+C and cleans up resources
- **Structured logging**: Uses `tracing` for observability
- **Error resilience**: Continues operating even if individual fetches fail

### Running the Application

```bash
# Setup database
createdb stockdb
psql stockdb < schema.sql

# Set environment variables
export DATABASE_URL="postgresql://user:password@localhost/stockdb"
export ALPHA_VANTAGE_KEY="your_key"
export FINNHUB_KEY="your_key"

# Run the application
cargo run

# In another terminal, observe logs and database
watch -n 5 'psql stockdb -c "SELECT * FROM stock_prices ORDER BY created_at DESC LIMIT 10;"'
```

### Extensions (Bonus)

- Add a REST API to query historical prices
- Implement a web dashboard with real-time updates
- Add alerts when prices cross thresholds
- Support for cryptocurrency prices
- Rate limiting for API calls

---

## 📚 Resources

- [Tokio Documentation](https://tokio.rs)
- [SQLx Documentation](https://docs.rs/sqlx)
- [Tracing Documentation](https://docs.rs/tracing)
- [Alpha Vantage API](https://www.alphavantage.co/documentation/)
- [Finnhub API](https://finnhub.io/docs/api)
