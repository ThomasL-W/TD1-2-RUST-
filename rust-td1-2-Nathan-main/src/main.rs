use tokio::time::{sleep, Duration, interval};
use rand::Rng;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use sqlx::{PgPool, postgres::PgPoolOptions, Row};
use chrono::Utc;
use tokio::signal;
use tracing::{info, error, instrument};
use std::env;
use dotenv::dotenv;

// Part 1: Original mock price code

async fn fetch_mock_price(symbol: &str) -> f64 {
    sleep(Duration::from_millis(500)).await;
    let mut rng = rand::thread_rng();
    let price = rng.gen_range(100.0..200.0);
    println!("Mock price for {} -> ${:.2}", symbol, price);
    price
}

// Part 2: Parallel fetch for multiple APIs

#[derive(Debug, Clone)]
struct StockPrice {
    symbol: String,
    price: f64,
    source: String,
    timestamp: i64,
}

#[instrument]
async fn fetch_alpha_vantage(symbol: &str) -> Result<StockPrice, Box<dyn std::error::Error>> {
    sleep(Duration::from_millis(500)).await;
    let mut rng = rand::thread_rng();
    let price = rng.gen_range(100.0..200.0);
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
    info!("Alpha Vantage price for {}: ${:.2}", symbol, price);

    Ok(StockPrice {
        symbol: symbol.to_string(),
        price,
        source: "Alpha Vantage".to_string(),
        timestamp,
    })
}

#[instrument]
async fn fetch_finnhub(symbol: &str) -> Result<StockPrice, Box<dyn std::error::Error>> {
    sleep(Duration::from_millis(500)).await;
    let mut rng = rand::thread_rng();
    let price = rng.gen_range(100.0..200.0);
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
    info!("Finnhub price for {}: ${:.2}", symbol, price);

    Ok(StockPrice {
        symbol: symbol.to_string(),
        price,
        source: "Finnhub".to_string(),
        timestamp,
    })
}

// Part 4: Third mock API (Yahoo Finance)

#[instrument]
async fn fetch_yahoo(symbol: &str) -> Result<StockPrice, Box<dyn std::error::Error>> {
    sleep(Duration::from_millis(500)).await;
    let mut rng = rand::thread_rng();
    let price = rng.gen_range(100.0..200.0);
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
    info!("Yahoo Finance price for {}: ${:.2}", symbol, price);

    Ok(StockPrice {
        symbol: symbol.to_string(),
        price,
        source: "Yahoo Finance".to_string(),
        timestamp,
    })
}

// Part 3: Database Integration with PostgreSQL

#[instrument(skip(pool))]
async fn save_price(pool: &PgPool, price: &StockPrice) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO stock_prices (symbol, price, source, timestamp)
        VALUES ($1, $2, $3, $4)
        "#
    )
    .bind(&price.symbol)
    .bind(price.price)
    .bind(&price.source)
    .bind(price.timestamp)
    .execute(pool)
    .await?;

    // EXERCISE 1: Send NOTIFY to trigger WebSocket broadcasts
    sqlx::query("NOTIFY new_price")
        .execute(pool)
        .await?;

    Ok(())
}

// Part 4: Fetch all sources with error recovery

#[instrument(skip(pool))]
async fn fetch_and_save_all(pool: &PgPool, symbols: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting fetch cycle for {} symbols", symbols.len());

    for symbol in symbols {
        // Fetch from multiple sources in parallel
        let (alpha_result, finnhub_result, yahoo_result) = tokio::join!(
            fetch_alpha_vantage(symbol),
            fetch_finnhub(symbol),
            fetch_yahoo(symbol)
        );

        // Save results with error recovery - if one fails, others still save
        if let Ok(price) = alpha_result {
            if let Err(e) = save_price(pool, &price).await {
                error!("Could not save Alpha Vantage price for {}: {}", symbol, e);
            }
        } else if let Err(e) = alpha_result {
            error!("Could not fetch Alpha Vantage price for {}: {}", symbol, e);
        }

        if let Ok(price) = finnhub_result {
            if let Err(e) = save_price(pool, &price).await {
                error!("Could not save Finnhub price for {}: {}", symbol, e);
            }
        } else if let Err(e) = finnhub_result {
            error!("Could not fetch Finnhub price for {}: {}", symbol, e);
        }

        if let Ok(price) = yahoo_result {
            if let Err(e) = save_price(pool, &price).await {
                error!("Could not save Yahoo Finance price for {}: {}", symbol, e);
            }
        } else if let Err(e) = yahoo_result {
            error!("Could not fetch Yahoo Finance price for {}: {}", symbol, e);
        }
    }

    info!("Fetch cycle completed");
    Ok(())
}

// Part 4: Query latest price endpoint

#[instrument(skip(pool))]
async fn query_latest_prices(pool: &PgPool) -> Result<(), sqlx::Error> {
    println!("LATEST PRICES IN DATABASE");

    let rows = sqlx::query(
        r#"
        SELECT symbol, price, source, timestamp
        FROM stock_prices
        WHERE id IN (
            SELECT MAX(id) FROM stock_prices GROUP BY symbol, source
        )
        ORDER BY symbol, source
        "#
    )
    .fetch_all(pool)
    .await?;

    for row in rows {
        let symbol: String = row.get(0);
        let price: f64 = row.get(1);
        let source: String = row.get(2);
        let timestamp: i64 = row.get(3);

        println!("║ {:6} | ${:8.2} | {:15} | {:10} ║", symbol, price, source, timestamp);
    }

    Ok(())
}

// Main function - Part 4 Complete

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load environment variables from .env file
    dotenv().ok();

    // Setup structured logging with tracing
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    info!("Starting stock price aggregator");

    // Check for --fetch-once flag
    let fetch_once = env::args().any(|arg| arg == "--fetch-once");

    // Setup database connection pool
    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set in environment");

    info!("Connecting to database...");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    info!("Connected to database successfully");

    // Configuration
    let symbols = vec![
        "AAPL".to_string(),
        "GOOGL".to_string(),
        "MSFT".to_string()
    ];


    // Part 1: Sequential mock fetch (demonstration)

    println!("Part 1: Sequential mock prices");
    let start = Instant::now();

    let _ = fetch_mock_price("AAPL").await;
    let _ = fetch_mock_price("GOOGL").await;
    let _ = fetch_mock_price("MSFT").await;

    let elapsed = start.elapsed();
    println!("Total elapsed time: {:.2?}\n", elapsed);


    // Part 2: Parallel fetch demonstration


    println!("Part 2: Parallel fetch from multiple APIs");
    for symbol in &symbols {
        info!("Fetching prices for {}...", symbol);
        let (alpha, finnhub) = tokio::join!(
            fetch_alpha_vantage(symbol),
            fetch_finnhub(symbol)
        );
        println!("Results for {}:", symbol);
        println!("Alpha Vantage: {:?}", alpha);
        println!("Finnhub: {:?}\n", finnhub);
    }


    // Part 3: Initial save to database

    println!("Part 3: Saving initial results to PostgreSQL");

    fetch_and_save_all(&pool, &symbols).await?;
    info!("All prices saved to database successfully");


    // Part 4: Handle --fetch-once flag

    if fetch_once {
        info!("--fetch-once flag detected, querying latest prices and exiting...");
        query_latest_prices(&pool).await?;

        info!("Closing database connections...");
        pool.close().await;
        info!("Shutdown complete");

        return Ok(());
    }


    // Part 4: Periodic fetching with graceful shutdown

    println!("Part 4: Periodic fetching (every 60 seconds)");
    println!("Press Ctrl+C to stop gracefully...\n");

    // Create interval for periodic fetching (every minute)
    let mut fetch_interval = interval(Duration::from_secs(60));

    // Main event loop
    loop {
        tokio::select! {
            // Periodic fetch trigger
            _ = fetch_interval.tick() => {
                info!("Starting periodic fetch cycle...");

                if let Err(e) = fetch_and_save_all(&pool, &symbols).await {
                    error!("Error during fetch cycle: {}", e);
                    // Continue running even if one cycle fails
                } else {
                    info!("Fetch cycle completed successfully");
                }
            }

            // Graceful shutdown on Ctrl+C
            _ = signal::ctrl_c() => {
                info!("Ctrl+C received, initiating graceful shutdown...");
                break;
            }
        }
    }

    // Cleanup resources
    info!("Closing database connections...");
    pool.close().await;
    info!("Shutdown complete - all resources cleaned up");

    Ok(())
}
