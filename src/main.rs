use std::collections::{HashMap, HashSet};
use std::env;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{extract::State, response::Html, routing::get, Json, Router};
use chrono::Utc;
use dotenv::dotenv;
use futures_util::StreamExt;
use log::{error, info, warn};
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::auth::{Credentials, LocalSigner, Signer};
use polymarket_client_sdk::clob::types::request::{
    BalanceAllowanceRequest, OrderBookSummaryRequest,
};
use polymarket_client_sdk::clob::types::response::OrderBookSummaryResponse;
use polymarket_client_sdk::clob::types::{AssetType, OrderType, Side};
use polymarket_client_sdk::clob::ws::{Client as WsClient, TradeMessage};
use polymarket_client_sdk::clob::{Client, Config as ClobConfig};
use polymarket_client_sdk::types::{Address, Decimal};
use polymarket_client_sdk::POLYGON;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use serde::Serialize;
use tokio::sync::RwLock;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

// ============================================================================
// CONSTANTS - Compile-time decimal values to avoid runtime parsing
// ============================================================================

/// Decimal constant for division by 2
const DECIMAL_TWO: Decimal = Decimal::from_parts(2, 0, 0, false, 0);

/// Epsilon for near-zero exposure cleanup (0.0001)
const EPSILON: Decimal = Decimal::from_parts(1, 0, 0, false, 4);

/// Skew factor for inventory-based pricing (0.5)
const SKEW_FACTOR: Decimal = Decimal::from_parts(5, 0, 0, false, 1);

/// Fraction of exposure to rehedge (0.8 = 80%)
const REHEDGE_FRACTION: Decimal = Decimal::from_parts(8, 0, 0, false, 1);

/// Maker rebate rate (0.5% = 0.005)
const MAKER_REBATE_RATE: Decimal = Decimal::from_parts(5, 0, 0, false, 3);

/// Taker fee rate (0.5% = 0.005)
const TAKER_FEE_RATE: Decimal = Decimal::from_parts(5, 0, 0, false, 3);

/// Maximum slippage for paper trades (0.2% = 0.002)
const MAX_SLIPPAGE: Decimal = Decimal::from_parts(2, 0, 0, false, 3);

/// Fallback timestamp when system clock has issues (2024-01-01 00:00:00 UTC)
const FALLBACK_TIMESTAMP: u64 = 1704067200;

/// Dashboard web server port
const DASHBOARD_PORT: u16 = 8082;

// ============================================================================
// CONFIGURATION
// ============================================================================

/// Bot configuration loaded from environment variables
#[derive(Clone)]
struct Config {
    wallet_private_key: String,
    min_balance: Decimal,
    offset: Decimal,
    max_spread: Decimal,
    market_pairs: Vec<(String, String)>,

    // Timing configuration
    stale_order_timeout_secs: u64,
    main_loop_interval_ms: u64,

    // Risk parameters
    rehedge_threshold_pct: f64,
    max_exposure_pct: f64,
    kelly_fraction: f64,
    edge: f64,
    variance: f64,
    rebate_rate: Decimal,

    // Volatility factors
    wide_spread_threshold: Decimal,
    high_vol_factor: f64,
    low_vol_factor: f64,

    // Retry configuration
    max_retry_attempts: usize,
    max_retry_delay_secs: u64,

    // API timeouts
    api_timeout_secs: u64,

    // Cleanup configuration
    cleanup_interval_secs: u64,
    max_tracked_orders: usize,
    max_tracked_exposures: usize,

    // Paper trading
    paper_trade: bool,
    paper_starting_balance: Decimal,
}

// ============================================================================
// STATE
// ============================================================================

/// A simulated paper order for fill tracking
#[derive(Clone, Debug)]
struct PaperOrder {
    id: String,
    token_id: String,
    side: Side,
    price: Decimal,
    size: Decimal,
    filled: Decimal,
    created_at: u64,
}

struct BotState {
    balance: Decimal,
    exposure: HashMap<String, Decimal>,
    open_orders: HashMap<String, u64>,
    // Paper trading state
    paper_balance: Decimal,
    paper_orders: HashMap<String, PaperOrder>,
    paper_fills: u64,
    paper_rebates_earned: Decimal,
    paper_fees_paid: Decimal,
    paper_realized_pnl: Decimal,
    paper_order_count: u64,
    // Track entry prices for P&L calculation
    paper_entry_prices: HashMap<String, Decimal>,
    // Recent fills for dashboard display
    recent_fills: Vec<FillRecord>,
    // Performance stats
    api_latency_ms: u64,
    ws_connected: bool,
    loop_count: u64,
    orders_placed: u64,
    last_error: Option<String>,
}

/// Record of a paper fill for dashboard display
#[derive(Clone, Debug, Serialize)]
struct FillRecord {
    time: String,
    side: String,
    size: String,
    price: String,
    value: String,
    rebate: String,
    pnl: String,
}

/// Dashboard API response
#[derive(Serialize)]
struct DashboardData {
    paper_mode: bool,
    uptime_secs: u64,
    starting_balance: String,
    current_balance: String,
    total_return_pct: String,
    realized_pnl: String,
    rebates_earned: String,
    fees_paid: String,
    total_fills: u64,
    open_orders: usize,
    tracked_positions: usize,
    recent_fills: Vec<FillRecord>,
    timestamp: String,
    // New stats
    market_count: usize,
    api_latency_ms: u64,
    ws_connected: bool,
    loop_count: u64,
    orders_placed: u64,
    last_error: Option<String>,
}

/// Shared state for web dashboard
#[derive(Clone)]
struct AppState {
    bot_state: Arc<RwLock<BotState>>,
    config: Config,
    start_time: Instant,
    market_count: usize,
}

/// WebSocket context for listener tasks
struct WsContext<S: Signer> {
    state: Arc<RwLock<BotState>>,
    credentials: Credentials,
    address: Address,
    markets: Vec<String>,
    shutdown: CancellationToken,
    config: Config,
    client: SharedClient,
    signer: Arc<S>,
}

/// Tracks last known good timestamp for clock skew recovery
static LAST_GOOD_TIMESTAMP: AtomicU64 = AtomicU64::new(FALLBACK_TIMESTAMP);

type AuthedClient = Client<Authenticated<Normal>>;
type SharedClient = Arc<AuthedClient>;

// ============================================================================
// MAIN
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    env_logger::init();

    let config = load_config()?;
    info!("Configuration loaded successfully");

    let signer = LocalSigner::from_str(&config.wallet_private_key)?.with_chain_id(Some(POLYGON));
    let signer = Arc::new(signer);

    let (client, credentials) = create_client(signer.as_ref()).await?;
    let client: SharedClient = Arc::new(client);
    info!("Client authenticated successfully");

    // Create cancellation token for graceful shutdown
    let shutdown_token = CancellationToken::new();
    let shutdown_clone = shutdown_token.clone();

    // Spawn signal handler with proper error handling (no panics)
    tokio::spawn(async move {
        if let Err(e) = wait_for_shutdown_signal().await {
            error!("Signal handler error: {:?}. Triggering shutdown anyway.", e);
        }
        info!("Shutdown signal received, initiating graceful shutdown...");
        shutdown_clone.cancel();
    });

    let start_time = Instant::now();
    let market_count = config.market_pairs.len();
    let state = Arc::new(RwLock::new(BotState {
        balance: if config.paper_trade { config.paper_starting_balance } else { Decimal::ZERO },
        exposure: HashMap::new(),
        open_orders: HashMap::new(),
        paper_balance: config.paper_starting_balance,
        paper_orders: HashMap::new(),
        paper_fills: 0,
        paper_rebates_earned: Decimal::ZERO,
        paper_fees_paid: Decimal::ZERO,
        paper_realized_pnl: Decimal::ZERO,
        paper_order_count: 0,
        paper_entry_prices: HashMap::new(),
        recent_fills: Vec::new(),
        api_latency_ms: 0,
        ws_connected: false,
        loop_count: 0,
        orders_placed: 0,
        last_error: None,
    }));

    if config.paper_trade {
        info!("══════════════════════════════════════════════════════════════");
        info!("   PAPER TRADING MODE - NO REAL ORDERS WILL BE PLACED");
        info!("   Starting balance: ${}", config.paper_starting_balance);
        info!("   Maker rebate: {}%  |  Taker fee: {}%",
              MAKER_REBATE_RATE * Decimal::from(100),
              TAKER_FEE_RATE * Decimal::from(100));
        info!("══════════════════════════════════════════════════════════════");
    }

    let tokens: Vec<String> = config
        .market_pairs
        .iter()
        .flat_map(|(yes, no)| vec![yes.clone(), no.clone()])
        .collect();

    let markets = fetch_market_ids(&client, &tokens, &config).await?;
    info!("Fetched {} market IDs", markets.len());

    // Spawn WebSocket listener with reconnection
    let ws_ctx = WsContext {
        state: state.clone(),
        credentials: credentials.clone(),
        address: client.address(),
        markets: markets.clone(),
        shutdown: shutdown_token.clone(),
        config: config.clone(),
        client: client.clone(),
        signer: signer.clone(),
    };

    tokio::spawn(async move {
        ws_listener_with_reconnect(ws_ctx).await;
    });

    // Spawn periodic cleanup task
    let cleanup_state = state.clone();
    let cleanup_shutdown = shutdown_token.clone();
    let cleanup_config = config.clone();

    tokio::spawn(async move {
        periodic_cleanup(cleanup_state, cleanup_shutdown, cleanup_config).await;
    });

    // Spawn dashboard web server
    let app_state = AppState {
        bot_state: state.clone(),
        config: config.clone(),
        start_time,
        market_count,
    };
    tokio::spawn(async move {
        run_dashboard_server(app_state).await;
    });

    // Main loop with shutdown support
    loop {
        tokio::select! {
            _ = shutdown_token.cancelled() => {
                info!("Main loop shutting down gracefully...");
                if let Err(e) = cancel_all_orders(&state, &client, &config).await {
                    warn!("Failed to cancel all orders during shutdown: {:?}", e);
                }
                break;
            }
            _ = run_main_loop_iteration(&state, &client, &signer, &config, &shutdown_token) => {}
        }
    }

    info!("Bot shutdown complete.");
    Ok(())
}

// ============================================================================
// MAIN LOOP
// ============================================================================

async fn run_main_loop_iteration<S: Signer + Sync>(
    state: &Arc<RwLock<BotState>>,
    client: &SharedClient,
    signer: &Arc<S>,
    config: &Config,
    shutdown_token: &CancellationToken,
) {
    // Increment loop counter
    {
        let mut locked = state.write().await;
        locked.loop_count += 1;
    }

    // In paper mode, use simulated balance instead of fetching from API
    let current_balance = if config.paper_trade {
        let locked = state.read().await;
        locked.paper_balance
    } else {
        // Fetch real balance with timeout
        match timeout(
            Duration::from_secs(config.api_timeout_secs),
            fetch_usdc_balance(client),
        )
        .await
        {
            Ok(Ok(b)) => b,
            Ok(Err(e)) => {
                warn!("Failed to fetch balance: {:?}", e);
                sleep(Duration::from_millis(config.main_loop_interval_ms)).await;
                return;
            }
            Err(_) => {
                warn!(
                    "Balance fetch timed out after {}s",
                    config.api_timeout_secs
                );
                sleep(Duration::from_millis(config.main_loop_interval_ms)).await;
                return;
            }
        }
    };

    if current_balance < config.min_balance {
        error!(
            "{}Balance too low: {} (minimum: {}). Shutting down.",
            if config.paper_trade { "[PAPER] " } else { "" },
            current_balance, config.min_balance
        );
        shutdown_token.cancel();
        return;
    }

    {
        let mut locked = state.write().await;
        locked.balance = current_balance;
    }

    if let Err(e) = cancel_stale_orders(state, client, config).await {
        warn!("Stale order cancel failed: {:?}", e);
    }

    for (yes_token, no_token) in &config.market_pairs {
        if let Err(e) = retry(
            || provide_liquidity(state, client, signer.as_ref(), yes_token, no_token, config),
            config,
        )
        .await
        {
            warn!("Liquidity provision failed for {}: {:?}", yes_token, e);
            // Continue to next market pair - no extra sleep (retry already handles backoff)
        }
    }

    sleep(Duration::from_millis(config.main_loop_interval_ms)).await;
}

// ============================================================================
// SIGNAL HANDLING (No panics)
// ============================================================================

/// Wait for shutdown signals (Ctrl+C and SIGTERM) without panicking
async fn wait_for_shutdown_signal() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut sigterm = signal(SignalKind::terminate())?;
        let mut sigint = signal(SignalKind::interrupt())?;

        tokio::select! {
            _ = sigterm.recv() => {
                info!("Received SIGTERM");
            }
            _ = sigint.recv() => {
                info!("Received SIGINT");
            }
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        info!("Received Ctrl+C");
    }

    Ok(())
}

// ============================================================================
// ORDER MANAGEMENT
// ============================================================================

/// Cancel all open orders during shutdown
async fn cancel_all_orders(
    state: &RwLock<BotState>,
    client: &SharedClient,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    let order_ids: Vec<String> = {
        let locked = state.read().await;
        locked.open_orders.keys().cloned().collect()
    };

    if order_ids.is_empty() {
        return Ok(());
    }

    info!(
        "Canceling {} open orders during shutdown...",
        order_ids.len()
    );

    for id in order_ids {
        match timeout(
            Duration::from_secs(config.api_timeout_secs),
            client.cancel_order(&id),
        )
        .await
        {
            Ok(Ok(_)) => info!("Canceled order: {}", id),
            Ok(Err(e)) => warn!("Failed to cancel order {}: {:?}", id, e),
            Err(_) => warn!("Timeout canceling order {}", id),
        }
    }

    // Clear local state
    {
        let mut locked = state.write().await;
        locked.open_orders.clear();
    }

    Ok(())
}

async fn cancel_stale_orders(
    state: &RwLock<BotState>,
    client: &SharedClient,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    let now = systemtime_now_secs();
    let stale: Vec<String> = {
        let locked = state.read().await;
        locked
            .open_orders
            .iter()
            .filter(|(_, &ts)| now.saturating_sub(ts) > config.stale_order_timeout_secs)
            .map(|(id, _)| id.clone())
            .collect()
    };

    if stale.is_empty() {
        return Ok(());
    }

    // Track which orders to remove from tracking
    let mut to_remove = Vec::new();

    for id in stale {
        match timeout(
            Duration::from_secs(config.api_timeout_secs),
            client.cancel_order(&id),
        )
        .await
        {
            Ok(Ok(_)) => {
                info!("Canceled stale order: {}", id);
                to_remove.push(id);
            }
            Ok(Err(e)) => {
                // Remove from tracking regardless of error type
                // Order is either canceled, filled, or doesn't exist - all mean we should stop tracking
                warn!("Cancel order {} returned error (removing from tracking): {:?}", id, e);
                to_remove.push(id);
            }
            Err(_) => {
                warn!("Timeout canceling order {} (keeping in tracking for retry)", id);
                // Don't remove - will retry on next cycle
            }
        }
    }

    // Update state in a single lock acquisition
    if !to_remove.is_empty() {
        let mut locked = state.write().await;
        for id in to_remove {
            locked.open_orders.remove(&id);
        }
    }

    Ok(())
}

// ============================================================================
// CLEANUP TASK
// ============================================================================

/// Periodic cleanup task to prevent unbounded HashMap growth
async fn periodic_cleanup(
    state: Arc<RwLock<BotState>>,
    shutdown: CancellationToken,
    config: Config,
) {
    let cleanup_interval = Duration::from_secs(config.cleanup_interval_secs);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("Cleanup task shutting down...");
                break;
            }
            _ = sleep(cleanup_interval) => {
                let mut locked = state.write().await;
                let now = systemtime_now_secs();

                // Clean up old orders (older than 2x stale timeout)
                let max_age = config.stale_order_timeout_secs.saturating_mul(2);
                let before_orders = locked.open_orders.len();
                locked.open_orders.retain(|_, &mut ts| {
                    now.saturating_sub(ts) < max_age
                });
                let removed_orders = before_orders - locked.open_orders.len();

                // Enforce maximum tracked orders (keep newest)
                if locked.open_orders.len() > config.max_tracked_orders {
                    let mut entries: Vec<_> = locked.open_orders.iter()
                        .map(|(k, v)| (k.clone(), *v))
                        .collect();
                    // Sort by timestamp ascending (oldest first)
                    entries.sort_by_key(|(_, ts)| *ts);

                    let to_remove = locked.open_orders.len() - config.max_tracked_orders;
                    for (key, _) in entries.into_iter().take(to_remove) {
                        locked.open_orders.remove(&key);
                    }
                }

                // Clean up near-zero exposure entries
                let before_exposure = locked.exposure.len();
                locked.exposure.retain(|_, v| v.abs() > EPSILON);
                let removed_exposure = before_exposure - locked.exposure.len();

                // Enforce maximum tracked exposures (keep LARGEST absolute exposures)
                if locked.exposure.len() > config.max_tracked_exposures {
                    let mut entries: Vec<_> = locked.exposure.iter()
                        .map(|(k, v)| (k.clone(), *v))
                        .collect();
                    // Sort by absolute value ASCENDING so smallest are first
                    entries.sort_by(|(_, a), (_, b)| a.abs().cmp(&b.abs()));

                    // Remove smallest exposures (they're at the front after ascending sort)
                    let to_remove = locked.exposure.len() - config.max_tracked_exposures;
                    for (key, _) in entries.into_iter().take(to_remove) {
                        locked.exposure.remove(&key);
                    }
                }

                // Paper trading: clean up stale paper orders and print summary
                if config.paper_trade {
                    let before_paper = locked.paper_orders.len();
                    locked.paper_orders.retain(|_, order| {
                        now.saturating_sub(order.created_at) < max_age
                    });
                    let removed_paper = before_paper - locked.paper_orders.len();

                    // Periodic paper trading summary
                    let starting_bal = config.paper_starting_balance;
                    let current_bal = locked.paper_balance;
                    let total_return = if starting_bal > Decimal::ZERO {
                        ((current_bal - starting_bal) / starting_bal) * Decimal::from(100)
                    } else {
                        Decimal::ZERO
                    };

                    info!("═══════════════════════════════════════════════════════════════");
                    info!("   PAPER TRADING SUMMARY");
                    info!("═══════════════════════════════════════════════════════════════");
                    info!("   Starting Balance:  ${:.2}", starting_bal);
                    info!("   Current Balance:   ${:.2}", current_bal);
                    info!("   Total Return:      {:.2}%", total_return);
                    info!("───────────────────────────────────────────────────────────────");
                    info!("   Total Fills:       {}", locked.paper_fills);
                    info!("   Rebates Earned:    ${:.4}", locked.paper_rebates_earned);
                    info!("   Fees Paid:         ${:.4}", locked.paper_fees_paid);
                    info!("   Realized P&L:      ${:.2}", locked.paper_realized_pnl);
                    info!("───────────────────────────────────────────────────────────────");
                    info!("   Open Orders:       {}", locked.paper_orders.len());
                    info!("   Tracked Positions: {}", locked.exposure.len());
                    if removed_paper > 0 {
                        info!("   Stale Orders Removed: {}", removed_paper);
                    }
                    info!("═══════════════════════════════════════════════════════════════");
                }

                if removed_orders > 0 || removed_exposure > 0 {
                    info!(
                        "Cleanup: removed {} stale orders, {} zero exposures. Current: {} orders, {} exposures",
                        removed_orders,
                        removed_exposure,
                        locked.open_orders.len(),
                        locked.exposure.len()
                    );
                }
            }
        }
    }
}

// ============================================================================
// CONFIGURATION LOADING
// ============================================================================

fn load_config() -> Result<Config, Box<dyn std::error::Error>> {
    let wallet_private_key =
        env::var("WALLET_PRIVATE_KEY").map_err(|_| "Missing WALLET_PRIVATE_KEY")?;

    let min_balance = parse_decimal_env("MIN_BALANCE", "50.0")?;
    let offset = parse_decimal_env("OFFSET", "0.001")?;
    let max_spread = parse_decimal_env("MAX_SPREAD", "0.01")?;

    let markets_str = env::var("MARKETS").map_err(|_| "Missing MARKETS")?;

    let market_pairs: Vec<(String, String)> = markets_str
        .split(';')
        .filter_map(|pair| {
            let parts: Vec<&str> = pair.split(',').collect();
            if parts.len() == 2 && !parts[0].trim().is_empty() && !parts[1].trim().is_empty() {
                Some((parts[0].trim().to_string(), parts[1].trim().to_string()))
            } else {
                warn!("Skipping invalid market pair: {}", pair);
                None
            }
        })
        .collect();

    if market_pairs.is_empty() {
        return Err("No valid markets configured".into());
    }

    // Timing configuration with validation
    let stale_order_timeout_secs = parse_u64_env("STALE_ORDER_TIMEOUT_SECS", "30")?;
    if stale_order_timeout_secs == 0 {
        return Err("STALE_ORDER_TIMEOUT_SECS must be > 0".into());
    }

    let main_loop_interval_ms = parse_u64_env("MAIN_LOOP_INTERVAL_MS", "500")?;
    if main_loop_interval_ms == 0 {
        return Err("MAIN_LOOP_INTERVAL_MS must be > 0".into());
    }

    // Risk parameters with validation
    let rehedge_threshold_pct = parse_f64_env("REHEDGE_THRESHOLD_PCT", "0.05")?;
    if !(0.0..=1.0).contains(&rehedge_threshold_pct) {
        return Err("REHEDGE_THRESHOLD_PCT must be between 0 and 1".into());
    }

    let max_exposure_pct = parse_f64_env("MAX_EXPOSURE_PCT", "0.3")?;
    if !(0.0..=1.0).contains(&max_exposure_pct) {
        return Err("MAX_EXPOSURE_PCT must be between 0 and 1".into());
    }

    let kelly_fraction = parse_f64_env("KELLY_FRACTION", "0.2")?;
    if !(0.0..=1.0).contains(&kelly_fraction) {
        return Err("KELLY_FRACTION must be between 0 and 1".into());
    }

    let edge = parse_f64_env("EDGE", "0.03")?;
    if edge < 0.0 {
        return Err("EDGE must be >= 0".into());
    }

    let variance = parse_f64_env("VARIANCE", "0.01")?;
    if variance <= 0.0 {
        return Err("VARIANCE must be > 0".into());
    }

    let rebate_rate = parse_decimal_env("REBATE_RATE", "0.005")?;

    // Volatility factors with validation
    let wide_spread_threshold = parse_decimal_env("WIDE_SPREAD_THRESHOLD", "0.005")?;

    let high_vol_factor = parse_f64_env("HIGH_VOL_FACTOR", "0.5")?;
    if high_vol_factor <= 0.0 {
        return Err("HIGH_VOL_FACTOR must be > 0".into());
    }

    let low_vol_factor = parse_f64_env("LOW_VOL_FACTOR", "1.5")?;
    if low_vol_factor <= 0.0 {
        return Err("LOW_VOL_FACTOR must be > 0".into());
    }

    // Retry configuration with validation
    let max_retry_attempts = parse_usize_env("MAX_RETRY_ATTEMPTS", "3")?;
    if max_retry_attempts == 0 {
        return Err("MAX_RETRY_ATTEMPTS must be >= 1".into());
    }

    let max_retry_delay_secs = parse_u64_env("MAX_RETRY_DELAY_SECS", "32")?;
    if max_retry_delay_secs == 0 {
        return Err("MAX_RETRY_DELAY_SECS must be > 0".into());
    }

    // API timeouts with validation
    let api_timeout_secs = parse_u64_env("API_TIMEOUT_SECS", "30")?;
    if api_timeout_secs == 0 {
        return Err("API_TIMEOUT_SECS must be > 0".into());
    }

    // Cleanup configuration with validation
    let cleanup_interval_secs = parse_u64_env("CLEANUP_INTERVAL_SECS", "300")?;
    if cleanup_interval_secs == 0 {
        return Err("CLEANUP_INTERVAL_SECS must be > 0".into());
    }

    let max_tracked_orders = parse_usize_env("MAX_TRACKED_ORDERS", "1000")?;
    if max_tracked_orders == 0 {
        return Err("MAX_TRACKED_ORDERS must be > 0".into());
    }

    let max_tracked_exposures = parse_usize_env("MAX_TRACKED_EXPOSURES", "100")?;
    if max_tracked_exposures == 0 {
        return Err("MAX_TRACKED_EXPOSURES must be > 0".into());
    }

    // Paper trading configuration
    let paper_trade = env::var("PAPER_TRADE")
        .map(|v| v.to_lowercase() == "true" || v == "1")
        .unwrap_or(false);
    let paper_starting_balance = parse_decimal_env("PAPER_STARTING_BALANCE", "1000.0")?;

    Ok(Config {
        wallet_private_key,
        min_balance,
        offset,
        max_spread,
        market_pairs,
        stale_order_timeout_secs,
        main_loop_interval_ms,
        rehedge_threshold_pct,
        max_exposure_pct,
        kelly_fraction,
        edge,
        variance,
        rebate_rate,
        wide_spread_threshold,
        high_vol_factor,
        low_vol_factor,
        max_retry_attempts,
        max_retry_delay_secs,
        api_timeout_secs,
        cleanup_interval_secs,
        max_tracked_orders,
        max_tracked_exposures,
        paper_trade,
        paper_starting_balance,
    })
}

fn parse_decimal_env(name: &str, default: &str) -> Result<Decimal, Box<dyn std::error::Error>> {
    let raw = env::var(name).unwrap_or_else(|_| default.to_string());
    Decimal::from_str(&raw).map_err(|e| format!("{name} parse error: {e}").into())
}

fn parse_u64_env(name: &str, default: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let raw = env::var(name).unwrap_or_else(|_| default.to_string());
    raw.parse::<u64>()
        .map_err(|e| format!("{name} parse error: {e}").into())
}

fn parse_f64_env(name: &str, default: &str) -> Result<f64, Box<dyn std::error::Error>> {
    let raw = env::var(name).unwrap_or_else(|_| default.to_string());
    raw.parse::<f64>()
        .map_err(|e| format!("{name} parse error: {e}").into())
}

fn parse_usize_env(name: &str, default: &str) -> Result<usize, Box<dyn std::error::Error>> {
    let raw = env::var(name).unwrap_or_else(|_| default.to_string());
    raw.parse::<usize>()
        .map_err(|e| format!("{name} parse error: {e}").into())
}

// ============================================================================
// CLIENT
// ============================================================================

async fn create_client<S: Signer>(
    signer: &S,
) -> Result<(AuthedClient, Credentials), Box<dyn std::error::Error>> {
    let client = Client::new("https://clob.polymarket.com", ClobConfig::default())?;
    let credentials = client.create_or_derive_api_key(signer, None).await?;
    let authed = client
        .authentication_builder(signer)
        .credentials(credentials.clone())
        .authenticate()
        .await?;
    Ok((authed, credentials))
}

async fn fetch_usdc_balance(client: &SharedClient) -> Result<Decimal, Box<dyn std::error::Error>> {
    let request = BalanceAllowanceRequest::builder()
        .asset_type(AssetType::Collateral)
        .build();
    let response = client.balance_allowance(request).await?;
    Ok(response.balance)
}

async fn fetch_market_ids(
    client: &SharedClient,
    token_ids: &[String],
    config: &Config,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut markets = HashSet::new();
    for token_id in token_ids {
        let request = OrderBookSummaryRequest::builder()
            .token_id(token_id)
            .build();
        match timeout(
            Duration::from_secs(config.api_timeout_secs),
            client.order_book(&request),
        )
        .await
        {
            Ok(Ok(book)) => {
                markets.insert(book.market);
            }
            Ok(Err(e)) => {
                warn!("Failed to fetch market for token {}: {:?}", token_id, e);
            }
            Err(_) => {
                warn!("Timeout fetching market for token {}", token_id);
            }
        }
    }
    Ok(markets.into_iter().collect())
}

// ============================================================================
// PRICE HELPERS
// ============================================================================

fn best_bid_ask(book: &OrderBookSummaryResponse) -> Option<(Decimal, Decimal)> {
    let best_bid = book.bids.iter().map(|order| order.price).max()?;
    let best_ask = book.asks.iter().map(|order| order.price).min()?;
    if best_ask <= best_bid {
        return None;
    }
    Some((best_bid, best_ask))
}

fn quantize_price(price: Decimal, tick: Decimal, round_up: bool) -> Decimal {
    if tick.is_zero() {
        return price;
    }

    let ticks = if round_up {
        (price / tick).ceil()
    } else {
        (price / tick).floor()
    };

    ticks * tick
}

fn decimal_to_f64(value: Decimal, label: &str) -> Result<f64, Box<dyn std::error::Error>> {
    value
        .to_f64()
        .ok_or_else(|| format!("Failed to convert {label} to f64").into())
}

fn decimal_from_f64(value: f64, label: &str) -> Result<Decimal, Box<dyn std::error::Error>> {
    Decimal::from_f64(value).ok_or_else(|| format!("Failed to convert {label} to Decimal").into())
}

// ============================================================================
// LIQUIDITY PROVISION
// ============================================================================

/// Place a single order and return the order ID if successful
/// Returns tuple of (order_id, Option<PaperOrder>) - paper order only in paper mode
#[allow(clippy::too_many_arguments)]
async fn place_single_order<S: Signer + Sync>(
    client: &SharedClient,
    signer: &S,
    token: &str,
    side: Side,
    price: Decimal,
    size: Decimal,
    label: &str,
    paper_trade: bool,
    paper_order_num: u64,
) -> Result<Option<(String, Option<PaperOrder>)>, Box<dyn std::error::Error + Send + Sync>> {
    // Paper trading mode - log and return fake order ID with order details
    if paper_trade {
        let order_id = format!("PAPER-{:06}", paper_order_num);
        info!(
            "[PAPER] {} order: {} {} @ {} (id: {})",
            label, size, token, price, order_id
        );
        let paper_order = PaperOrder {
            id: order_id.clone(),
            token_id: token.to_string(),
            side,
            price,
            size,
            filled: Decimal::ZERO,
            created_at: systemtime_now_secs(),
        };
        return Ok(Some((order_id, Some(paper_order))));
    }

    // Real trading mode
    let order = client
        .limit_order()
        .token_id(token)
        .side(side)
        .price(price)
        .size(size)
        .order_type(OrderType::GTC)
        .build()
        .await?;
    let signed = client.sign(signer, order).await?;
    let response = client.post_order(signed).await?;

    if response.success {
        Ok(Some((response.order_id, None))) // None for paper_order in real mode
    } else {
        warn!("{} failed: {:?}", label, response.error_msg);
        Ok(None)
    }
}

async fn provide_liquidity<S: Signer + Sync>(
    state: &RwLock<BotState>,
    client: &SharedClient,
    signer: &S,
    yes_token: &str,
    no_token: &str,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    // Track API latency
    let start = Instant::now();
    let book_yes = client
        .order_book(
            &OrderBookSummaryRequest::builder()
                .token_id(yes_token)
                .build(),
        )
        .await?;
    let book_no = client
        .order_book(
            &OrderBookSummaryRequest::builder()
                .token_id(no_token)
                .build(),
        )
        .await?;
    let latency_ms = start.elapsed().as_millis() as u64;

    // Update latency in state
    {
        let mut locked = state.write().await;
        locked.api_latency_ms = latency_ms;
    }

    let Some((best_bid_yes, best_ask_yes)) = best_bid_ask(&book_yes) else {
        warn!("Skipping {}: empty orderbook", yes_token);
        return Ok(());
    };

    let Some((best_bid_no, best_ask_no)) = best_bid_ask(&book_no) else {
        warn!("Skipping {}: empty orderbook", no_token);
        return Ok(());
    };

    let current_spread = best_ask_yes - best_bid_yes;
    if current_spread > config.max_spread {
        warn!(
            "Skipping {}: spread too wide ({})",
            yes_token, current_spread
        );
        return Ok(());
    }

    let (balance, total_exposure, yes_exposure, no_exposure) = {
        let locked = state.read().await;
        let total = locked
            .exposure
            .values()
            .fold(Decimal::ZERO, |acc, val| acc + val.abs());
        let yes_exp = locked.exposure.get(yes_token).copied().unwrap_or(Decimal::ZERO);
        let no_exp = locked.exposure.get(no_token).copied().unwrap_or(Decimal::ZERO);
        (locked.balance, total, yes_exp, no_exp)
    };

    let balance_f64 = decimal_to_f64(balance, "balance")?;
    let total_exposure_f64 = decimal_to_f64(total_exposure, "total_exposure")?;

    // Use configurable volatility factors
    let vol_factor = if current_spread > config.wide_spread_threshold {
        config.high_vol_factor
    } else {
        config.low_vol_factor
    };

    let dynamic_size =
        (config.edge / config.variance) * config.kelly_fraction * balance_f64 * vol_factor;
    let capped_size = dynamic_size.min(balance_f64 * config.max_exposure_pct / 4.0);

    if total_exposure_f64 + (capped_size * 4.0) > balance_f64 * config.max_exposure_pct {
        warn!("Exposure cap hit; skipping cycle for {}", yes_token);
        return Ok(());
    }

    let size = decimal_from_f64(capped_size, "order_size")?.round_dp(2);
    if size <= Decimal::ZERO {
        warn!("Skipping {}: non-positive order size", yes_token);
        return Ok(());
    }

    // Calculate mid prices using compile-time constant (no runtime unwrap)
    let mid_yes = (best_bid_yes + best_ask_yes) / DECIMAL_TWO;
    let mid_no = (best_bid_no + best_ask_no) / DECIMAL_TWO;

    let tick_yes = book_yes.tick_size.as_decimal();
    let tick_no = book_no.tick_size.as_decimal();

    // Calculate inventory skew for YES token
    // Positive exposure (long) -> widen sell spread, tighten buy spread
    // Negative exposure (short) -> opposite
    let max_exposure_dec = decimal_from_f64(balance_f64 * config.max_exposure_pct, "max_exp")?;
    let yes_skew = if max_exposure_dec > Decimal::ZERO {
        (yes_exposure / max_exposure_dec)
            .min(Decimal::ONE)
            .max(-Decimal::ONE)
    } else {
        Decimal::ZERO
    };
    let no_skew = if max_exposure_dec > Decimal::ZERO {
        (no_exposure / max_exposure_dec)
            .min(Decimal::ONE)
            .max(-Decimal::ONE)
    } else {
        Decimal::ZERO
    };

    // Skew multiplier: when long, less aggressive buy (wider), more aggressive sell (tighter to offload)
    // buy_skew < 1 when long (smaller offset = more aggressive buy price)
    // sell_skew > 1 when long (larger offset = less aggressive sell price)
    let yes_buy_skew = Decimal::ONE - (yes_skew * SKEW_FACTOR);
    let yes_sell_skew = Decimal::ONE + (yes_skew * SKEW_FACTOR);
    let no_buy_skew = Decimal::ONE - (no_skew * SKEW_FACTOR);
    let no_sell_skew = Decimal::ONE + (no_skew * SKEW_FACTOR);

    let buy_yes_price = quantize_price(mid_yes - config.offset * yes_buy_skew, tick_yes, false);
    let sell_yes_price = quantize_price(mid_yes + config.offset * yes_sell_skew, tick_yes, true);
    let buy_no_price = quantize_price(mid_no - config.offset * no_buy_skew, tick_no, false);
    let sell_no_price = quantize_price(mid_no + config.offset * no_sell_skew, tick_no, true);

    // Get paper order count for IDs (if in paper mode)
    let base_order_num = if config.paper_trade {
        let mut locked = state.write().await;
        let num = locked.paper_order_count;
        locked.paper_order_count += 4; // Reserve 4 order numbers
        num
    } else {
        0
    };

    // Place all 4 orders in parallel for better performance
    let order_specs = [
        (yes_token, Side::Buy, buy_yes_price, "Buy YES", base_order_num),
        (yes_token, Side::Sell, sell_yes_price, "Sell YES", base_order_num + 1),
        (no_token, Side::Buy, buy_no_price, "Buy NO", base_order_num + 2),
        (no_token, Side::Sell, sell_no_price, "Sell NO", base_order_num + 3),
    ];

    let order_futures = order_specs.map(|(token, side, price, label, order_num)| {
        place_single_order(client, signer, token, side, price, size, label, config.paper_trade, order_num)
    });

    let results = futures_util::future::join_all(order_futures).await;

    // Collect successful orders (both ID and paper order details if applicable)
    let mut order_ids: Vec<String> = Vec::new();
    let mut paper_orders_to_add: Vec<PaperOrder> = Vec::new();

    for result in results {
        match result {
            Ok(Some((id, paper_order))) => {
                order_ids.push(id);
                if let Some(po) = paper_order {
                    paper_orders_to_add.push(po);
                }
            }
            Ok(None) => {} // Order failed but not an error
            Err(e) => {
                warn!("Order placement error: {:?}", e);
            }
        }
    }

    if !order_ids.is_empty() {
        let now = systemtime_now_secs();
        let mut locked = state.write().await;

        for id in &order_ids {
            locked.open_orders.insert(id.clone(), now);
        }

        // Track total orders placed
        locked.orders_placed += order_ids.len() as u64;

        // Store paper orders for fill simulation
        if config.paper_trade {
            for po in paper_orders_to_add {
                locked.paper_orders.insert(po.id.clone(), po);
            }
            info!(
                "[PAPER] Placed {} orders. Balance: ${:.2}, Open orders: {}, Fills: {}, P&L: ${:.2}",
                order_ids.len(),
                locked.paper_balance,
                locked.paper_orders.len(),
                locked.paper_fills,
                locked.paper_realized_pnl
            );
        }
    }

    info!(
        "{}Placed hedged quotes with size {} for YES {} and NO {}",
        if config.paper_trade { "[PAPER] " } else { "" },
        size, yes_token, no_token
    );
    Ok(())
}

// ============================================================================
// WEBSOCKET
// ============================================================================

/// WebSocket listener with automatic reconnection
async fn ws_listener_with_reconnect<S: Signer + Sync + Send + 'static>(ctx: WsContext<S>) {
    let mut consecutive_failures = 0u32;
    const MAX_CONSECUTIVE_FAILURES: u32 = 10;

    loop {
        if ctx.shutdown.is_cancelled() {
            info!("WebSocket listener shutting down...");
            break;
        }

        info!("Starting WebSocket connection...");

        // Add timeout to WebSocket session to prevent indefinite hangs
        let ws_timeout = Duration::from_secs(ctx.config.api_timeout_secs * 2);
        let session_future = run_ws_session(&ctx);

        let result = timeout(ws_timeout, session_future).await;

        match result {
            Ok(Ok(())) => {
                // Clean exit (shutdown requested)
                break;
            }
            Ok(Err(e)) => {
                consecutive_failures += 1;
                error!(
                    "WebSocket session failed ({}/{}): {:?}",
                    consecutive_failures, MAX_CONSECUTIVE_FAILURES, e
                );
            }
            Err(_) => {
                consecutive_failures += 1;
                error!(
                    "WebSocket session timed out ({}/{})",
                    consecutive_failures, MAX_CONSECUTIVE_FAILURES
                );
            }
        }

        // Calculate backoff (error already dropped, safe to await)
        let backoff_secs = if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
            error!("Too many consecutive WebSocket failures, backing off longer...");
            consecutive_failures = 0;
            64u64 // Max backoff
        } else {
            // Exponential backoff: 2, 4, 8, 16, 32 seconds
            2u64.saturating_pow(consecutive_failures)
        };

        info!("Reconnecting WebSocket in {}s...", backoff_secs);
        sleep(Duration::from_secs(backoff_secs)).await;
    }
}

async fn run_ws_session<S: Signer + Sync>(
    ctx: &WsContext<S>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ws_client = WsClient::default().authenticate(ctx.credentials.clone(), ctx.address)?;
    let mut stream = Box::pin(ws_client.subscribe_trades(ctx.markets.clone())?);

    // Mark WebSocket as connected
    {
        let mut locked = ctx.state.write().await;
        locked.ws_connected = true;
    }

    let result = async {
        loop {
            tokio::select! {
                _ = ctx.shutdown.cancelled() => {
                    info!("WebSocket session received shutdown signal");
                    return Ok(());
                }
                event = stream.next() => {
                    match event {
                        Some(Ok(trade)) => {
                            // Process trade and check if rehedging is needed
                            let rehedge_info = {
                                let mut locked = ctx.state.write().await;
                                process_trade_sync(&mut locked, &trade, &ctx.config)
                            };

                            // If rehedging is needed, do it asynchronously (lock released)
                            if let Some((token_id, exposure)) = rehedge_info {
                                if let Err(e) = rehedge_position(&ctx.client, ctx.signer.as_ref(), &token_id, exposure, &ctx.config).await {
                                    warn!("Rehedge failed for {}: {:?}", token_id, e);
                                }
                            }
                        }
                        Some(Err(e)) => {
                            // Return ALL errors to trigger reconnection
                            // Don't try to be clever about which errors are "recoverable"
                            warn!("WebSocket stream error, reconnecting: {:?}", e);
                            return Err(e.into());
                        }
                        None => {
                            info!("WebSocket stream ended");
                            return Err("WebSocket stream ended".into());
                        }
                    }
                }
            }
        }
    }.await;

    // Mark WebSocket as disconnected on exit
    {
        let mut locked = ctx.state.write().await;
        locked.ws_connected = false;
        if let Err(ref e) = result {
            locked.last_error = Some(format!("{:?}", e));
        }
    }

    result
}

/// Process a trade and return rehedge info if needed: Some((token_id, exposure))
fn process_trade_sync(
    state: &mut BotState,
    trade: &TradeMessage,
    config: &Config,
) -> Option<(String, Decimal)> {
    // In paper mode, simulate fills for our open orders
    if config.paper_trade {
        simulate_paper_fills(state, trade, config);
    }

    let delta = if trade.side == Side::Buy {
        trade.size
    } else {
        -trade.size
    };

    let balance = state.balance;
    let exposure = state
        .exposure
        .entry(trade.asset_id.clone())
        .or_insert(Decimal::ZERO);
    *exposure += delta;
    let current_exposure = *exposure;
    let exposure_abs = current_exposure.abs();

    let rebate = trade.size * config.rebate_rate;
    info!(
        "Trade detected: Token {}, Side {:?}, Size {}. Estimated rebate: {}",
        trade.asset_id, trade.side, trade.size, rebate
    );

    // Convert to f64 for comparison
    let balance_f64 = balance.to_f64().unwrap_or(0.0);
    let exposure_f64 = exposure_abs.to_f64().unwrap_or(0.0);

    if exposure_f64 > balance_f64 * config.rehedge_threshold_pct {
        warn!(
            "Re-hedging triggered for token {} (exposure: {:.4}, threshold: {:.1}% of {:.2})",
            trade.asset_id,
            exposure_f64,
            config.rehedge_threshold_pct * 100.0,
            balance_f64
        );
        Some((trade.asset_id.clone(), current_exposure))
    } else {
        None
    }
}

/// Simulate paper order fills based on market trades
fn simulate_paper_fills(state: &mut BotState, trade: &TradeMessage, _config: &Config) {
    // Find paper orders for this token that would be filled by this trade
    // - Our BUY orders fill when someone SELLS at or below our price
    // - Our SELL orders fill when someone BUYS at or above our price
    let trade_price = trade.price;
    let mut trade_remaining = trade.size;

    // Collect orders to process (can't modify while iterating)
    let matching_orders: Vec<String> = state
        .paper_orders
        .iter()
        .filter(|(_, order)| {
            order.token_id == trade.asset_id && order.filled < order.size && {
                match (&order.side, &trade.side) {
                    // Our BUY fills when market SELLS at or below our price
                    (Side::Buy, Side::Sell) => trade_price <= order.price,
                    // Our SELL fills when market BUYS at or above our price
                    (Side::Sell, Side::Buy) => trade_price >= order.price,
                    _ => false, // Same side doesn't fill us
                }
            }
        })
        .map(|(id, _)| id.clone())
        .collect();

    // Process fills
    for order_id in matching_orders {
        if trade_remaining <= Decimal::ZERO {
            break;
        }

        if let Some(order) = state.paper_orders.get_mut(&order_id) {
            let unfilled = order.size - order.filled;
            let fill_size = unfilled.min(trade_remaining);

            if fill_size <= Decimal::ZERO {
                continue;
            }

            // Calculate slippage based on fill size relative to available liquidity
            // Larger fills relative to trade size = more slippage
            let fill_ratio = fill_size / (trade.size + Decimal::ONE); // +1 to avoid div by zero
            let slippage = MAX_SLIPPAGE * fill_ratio;

            // Apply slippage (worse for us: higher buy price, lower sell price)
            let fill_price = match order.side {
                Side::Buy => order.price * (Decimal::ONE + slippage),
                Side::Sell => order.price * (Decimal::ONE - slippage),
                _ => order.price, // Fallback for any future Side variants
            };

            // Calculate trade value
            let fill_value = fill_size * fill_price;

            // Maker rebate (we're makers since our orders were resting)
            let rebate = fill_value * MAKER_REBATE_RATE;
            state.paper_rebates_earned += rebate;

            // Update balance based on side
            match order.side {
                Side::Buy => {
                    // We buy: spend USDC, get tokens
                    state.paper_balance -= fill_value;
                    state.paper_balance += rebate; // Rebate offsets cost

                    // Track entry price for P&L (weighted average)
                    let entry = state
                        .paper_entry_prices
                        .entry(order.token_id.clone())
                        .or_insert(Decimal::ZERO);
                    if *entry == Decimal::ZERO {
                        *entry = fill_price;
                    } else {
                        // Weighted average entry
                        let existing_pos = state
                            .exposure
                            .get(&order.token_id)
                            .copied()
                            .unwrap_or(Decimal::ZERO)
                            .abs();
                        if existing_pos + fill_size > Decimal::ZERO {
                            *entry = (*entry * existing_pos + fill_price * fill_size)
                                / (existing_pos + fill_size);
                        }
                    }
                }
                Side::Sell => {
                    // We sell: get USDC, lose tokens
                    state.paper_balance += fill_value;
                    state.paper_balance += rebate; // Rebate adds to proceeds

                    // Calculate realized P&L if we had an entry price
                    if let Some(entry_price) = state.paper_entry_prices.get(&order.token_id) {
                        let pnl = (fill_price - *entry_price) * fill_size;
                        state.paper_realized_pnl += pnl;
                    }
                }
                _ => {} // Ignore any future Side variants
            }

            // Update fill tracking
            order.filled += fill_size;
            trade_remaining -= fill_size;
            state.paper_fills += 1;

            // Calculate P&L for this fill (for display)
            let fill_pnl = if order.side == Side::Sell {
                if let Some(entry_price) = state.paper_entry_prices.get(&order.token_id) {
                    (fill_price - *entry_price) * fill_size
                } else {
                    Decimal::ZERO
                }
            } else {
                Decimal::ZERO
            };

            // Record fill for dashboard
            let fill_record = FillRecord {
                time: Utc::now().format("%H:%M:%S").to_string(),
                side: if order.side == Side::Buy { "BUY".to_string() } else { "SELL".to_string() },
                size: format!("{:.2}", fill_size),
                price: format!("{:.4}", fill_price),
                value: format!("${:.2}", fill_value),
                rebate: format!("${:.4}", rebate),
                pnl: if fill_pnl != Decimal::ZERO { format!("${:.2}", fill_pnl) } else { "-".to_string() },
            };
            state.recent_fills.push(fill_record);

            // Keep only last 100 fills
            if state.recent_fills.len() > 100 {
                state.recent_fills.remove(0);
            }

            info!(
                "[PAPER] FILL: {} {} @ {:.4} (slippage: {:.4}%) | Value: ${:.2} | Rebate: ${:.4}",
                if order.side == Side::Buy { "BUY" } else { "SELL" },
                fill_size,
                fill_price,
                slippage * Decimal::from(100),
                fill_value,
                rebate
            );
            info!(
                "[PAPER] Balance: ${:.2} | Realized P&L: ${:.2} | Total fills: {}",
                state.paper_balance, state.paper_realized_pnl, state.paper_fills
            );
        }
    }

    // Clean up fully filled orders
    state
        .paper_orders
        .retain(|_, order| order.filled < order.size);
}

// ============================================================================
// REHEDGING
// ============================================================================

/// Place an aggressive order to reduce exposure
async fn rehedge_position<S: Signer + Sync>(
    client: &SharedClient,
    signer: &S,
    token_id: &str,
    exposure: Decimal,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Get current orderbook
    let book = match timeout(
        Duration::from_secs(config.api_timeout_secs),
        client.order_book(
            &OrderBookSummaryRequest::builder()
                .token_id(token_id)
                .build(),
        ),
    )
    .await
    {
        Ok(Ok(b)) => b,
        Ok(Err(e)) => {
            return Err(format!("Failed to fetch orderbook for rehedge: {:?}", e).into());
        }
        Err(_) => {
            return Err("Timeout fetching orderbook for rehedge".into());
        }
    };

    let Some((best_bid, best_ask)) = best_bid_ask(&book) else {
        info!("Cannot rehedge {}: no liquidity in orderbook", token_id);
        return Ok(());
    };

    // Calculate rehedge size (80% of exposure to leave buffer)
    let rehedge_size = (exposure.abs() * REHEDGE_FRACTION).round_dp(2);

    if rehedge_size <= Decimal::ZERO {
        return Ok(());
    }

    // Determine side and price based on exposure direction
    // Long exposure -> sell at bid (aggressive, will fill immediately)
    // Short exposure -> buy at ask (aggressive, will fill immediately)
    let (side, price) = if exposure > Decimal::ZERO {
        (Side::Sell, best_bid)
    } else {
        (Side::Buy, best_ask)
    };

    // Paper trading mode - just log the rehedge
    if config.paper_trade {
        info!(
            "[PAPER] Rehedge order: {:?} {} @ {} for token {}",
            side, rehedge_size, price, token_id
        );
        return Ok(());
    }

    // Real trading mode
    info!(
        "Placing rehedge order: {:?} {} @ {} for token {}",
        side, rehedge_size, price, token_id
    );

    let order = client
        .limit_order()
        .token_id(token_id)
        .side(side)
        .price(price)
        .size(rehedge_size)
        .order_type(OrderType::GTC)
        .build()
        .await?;

    let signed = client.sign(signer, order).await?;
    let response = client.post_order(signed).await?;

    if response.success {
        info!(
            "Rehedge order placed successfully: {} {:?} {} @ {} for token {}",
            response.order_id, side, rehedge_size, price, token_id
        );
    } else {
        warn!(
            "Rehedge order failed for token {}: {:?}",
            token_id, response.error_msg
        );
    }

    Ok(())
}

// ============================================================================
// UTILITIES
// ============================================================================

/// Retry with exponential backoff (overflow-safe)
async fn retry<F, Fut>(mut f: F, config: &Config) -> Result<(), Box<dyn std::error::Error>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error>>>,
{
    let mut attempts = 0usize;
    loop {
        match f().await {
            Ok(_) => return Ok(()),
            Err(e) if attempts < config.max_retry_attempts => {
                attempts += 1;
                // Safe exponential backoff: 2^attempts, capped at max_retry_delay_secs
                let delay_secs = 2u64
                    .saturating_pow(attempts as u32)
                    .min(config.max_retry_delay_secs);
                warn!(
                    "Attempt {}/{} failed, retrying in {}s: {:?}",
                    attempts, config.max_retry_attempts, delay_secs, e
                );
                sleep(Duration::from_secs(delay_secs)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Get current time in seconds since UNIX epoch (safe from clock skew)
fn systemtime_now_secs() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => {
            let secs = duration.as_secs();
            // Update last known good timestamp
            LAST_GOOD_TIMESTAMP.store(secs, Ordering::Relaxed);
            secs
        }
        Err(e) => {
            // Clock skew detected - use last known good timestamp
            let fallback = LAST_GOOD_TIMESTAMP.load(Ordering::Relaxed);
            error!(
                "System clock skew detected: {:?}. Using last good timestamp: {}",
                e, fallback
            );
            fallback
        }
    }
}

// ============================================================================
// DASHBOARD WEB SERVER
// ============================================================================

async fn run_dashboard_server(app_state: AppState) {
    let app = Router::new()
        .route("/", get(dashboard_handler))
        .route("/api/data", get(api_handler))
        .with_state(app_state);

    let addr = SocketAddr::from(([0, 0, 0, 0], DASHBOARD_PORT));
    info!("Dashboard server starting on http://0.0.0.0:{}", DASHBOARD_PORT);

    if let Err(e) = axum::serve(
        tokio::net::TcpListener::bind(addr).await.unwrap(),
        app,
    )
    .await
    {
        error!("Dashboard server error: {:?}", e);
    }
}

async fn api_handler(State(app_state): State<AppState>) -> Json<DashboardData> {
    let locked = app_state.bot_state.read().await;
    let starting = app_state.config.paper_starting_balance;
    let current = locked.paper_balance;
    let return_pct = if starting > Decimal::ZERO {
        ((current - starting) / starting) * Decimal::from(100)
    } else {
        Decimal::ZERO
    };

    Json(DashboardData {
        paper_mode: app_state.config.paper_trade,
        uptime_secs: app_state.start_time.elapsed().as_secs(),
        starting_balance: format!("{:.2}", starting),
        current_balance: format!("{:.2}", current),
        total_return_pct: format!("{:.2}", return_pct),
        realized_pnl: format!("{:.2}", locked.paper_realized_pnl),
        rebates_earned: format!("{:.4}", locked.paper_rebates_earned),
        fees_paid: format!("{:.4}", locked.paper_fees_paid),
        total_fills: locked.paper_fills,
        open_orders: locked.paper_orders.len(),
        tracked_positions: locked.exposure.len(),
        recent_fills: locked.recent_fills.iter().rev().take(20).cloned().collect(),
        timestamp: Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        market_count: app_state.market_count,
        api_latency_ms: locked.api_latency_ms,
        ws_connected: locked.ws_connected,
        loop_count: locked.loop_count,
        orders_placed: locked.orders_placed,
        last_error: locked.last_error.clone(),
    })
}

async fn dashboard_handler(State(_app_state): State<AppState>) -> Html<String> {
    Html(DASHBOARD_HTML.to_string())
}

const DASHBOARD_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Poly-Two Dashboard</title>
  <style>
    :root {
      --bg: #0d1117;
      --bg-secondary: #161b22;
      --bg-tertiary: #21262d;
      --border: #30363d;
      --text: #f0f6fc;
      --muted: #8b949e;
      --accent: #3fb950;
      --danger: #f85149;
      --warn: #d29922;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0; padding: 20px;
      background: var(--bg); color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }
    .container { max-width: 1200px; margin: 0 auto; }
    .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; padding-bottom: 16px; border-bottom: 1px solid var(--border); }
    h1 { font-size: 24px; margin: 0; }
    .badge { padding: 6px 14px; border-radius: 6px; font-weight: 600; font-size: 12px; text-transform: uppercase; }
    .badge.paper { background: var(--warn); color: #000; }
    .badge.live { background: var(--danger); color: #fff; }

    .pnl-hero {
      background: linear-gradient(135deg, var(--bg-secondary), var(--bg-tertiary));
      border: 2px solid var(--accent);
      border-radius: 12px;
      padding: 32px;
      margin-bottom: 20px;
      text-align: center;
    }
    .pnl-main {
      font-size: 56px;
      font-weight: 800;
      font-family: 'SF Mono', monospace;
      margin-bottom: 8px;
    }
    .pnl-positive { color: var(--accent); text-shadow: 0 0 30px rgba(63,185,80,0.4); }
    .pnl-negative { color: var(--danger); text-shadow: 0 0 30px rgba(248,81,73,0.4); }
    .pnl-label { color: var(--muted); font-size: 14px; text-transform: uppercase; letter-spacing: 1px; }

    .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 16px; margin-bottom: 20px; }
    .stat-card {
      background: var(--bg-secondary);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 20px;
    }
    .stat-label { color: var(--muted); font-size: 12px; text-transform: uppercase; margin-bottom: 8px; }
    .stat-value { font-size: 24px; font-weight: 700; font-family: 'SF Mono', monospace; }
    .stat-value.positive { color: var(--accent); }
    .stat-value.negative { color: var(--danger); }

    .panel {
      background: var(--bg-secondary);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 20px;
      margin-bottom: 20px;
    }
    .panel h2 { font-size: 14px; color: var(--muted); margin: 0 0 16px 0; text-transform: uppercase; }

    table { width: 100%; border-collapse: collapse; }
    th, td { text-align: left; padding: 12px; border-bottom: 1px solid var(--border); font-size: 13px; }
    th { color: var(--muted); font-weight: 600; text-transform: uppercase; font-size: 11px; }
    td { font-family: 'SF Mono', monospace; }
    tr:hover { background: var(--bg-tertiary); }
    .fill-buy { color: var(--accent); }
    .fill-sell { color: var(--danger); }

    .uptime { color: var(--muted); font-size: 13px; }
    .last-update { color: var(--muted); font-size: 12px; text-align: center; margin-top: 20px; }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>Poly-Two</h1>
      <div>
        <span class="badge" id="mode">PAPER</span>
        <span class="uptime" id="uptime">0h 0m</span>
      </div>
    </div>

    <div class="pnl-hero">
      <div class="pnl-main pnl-positive" id="balance">$1000.00</div>
      <div class="pnl-label">Current Balance</div>
      <div style="margin-top:20px; display:flex; justify-content:center; gap:40px;">
        <div><span style="font-size:28px; font-weight:700;" id="return-pct">0.00%</span><br><span class="pnl-label">Return</span></div>
        <div><span style="font-size:28px; font-weight:700;" id="pnl">$0.00</span><br><span class="pnl-label">Realized P&L</span></div>
      </div>
    </div>

    <div class="stats-grid">
      <div class="stat-card">
        <div class="stat-label">Starting Balance</div>
        <div class="stat-value" id="starting">$1000.00</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Total Fills</div>
        <div class="stat-value" id="fills">0</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Rebates Earned</div>
        <div class="stat-value positive" id="rebates">$0.0000</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Open Orders</div>
        <div class="stat-value" id="orders">0</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Positions</div>
        <div class="stat-value" id="positions">0</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Fees Paid</div>
        <div class="stat-value" id="fees">$0.0000</div>
      </div>
    </div>

    <div class="stats-grid">
      <div class="stat-card">
        <div class="stat-label">Markets</div>
        <div class="stat-value" id="markets">0</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">WebSocket</div>
        <div class="stat-value" id="ws-status">--</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">API Latency</div>
        <div class="stat-value" id="latency">--</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Loop Count</div>
        <div class="stat-value" id="loops">0</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Orders Placed</div>
        <div class="stat-value" id="orders-placed">0</div>
      </div>
    </div>

    <div id="error-panel" class="panel" style="display:none; border-color: var(--danger);">
      <h2 style="color: var(--danger);">Last Error</h2>
      <div id="last-error" style="font-family: 'SF Mono', monospace; font-size: 13px; color: var(--danger);"></div>
    </div>

    <div class="panel">
      <h2>Recent Fills</h2>
      <table>
        <thead><tr><th>Time</th><th>Side</th><th>Size</th><th>Price</th><th>Value</th><th>Rebate</th><th>P&L</th></tr></thead>
        <tbody id="fills-table"></tbody>
      </table>
    </div>

    <div class="last-update" id="timestamp">Loading...</div>
  </div>

  <script>
    function formatUptime(secs) {
      const h = Math.floor(secs / 3600);
      const m = Math.floor((secs % 3600) / 60);
      return h + 'h ' + m + 'm';
    }

    async function load() {
      try {
        const resp = await fetch('/api/data');
        const d = await resp.json();

        document.getElementById('mode').textContent = d.paper_mode ? 'PAPER' : 'LIVE';
        document.getElementById('mode').className = 'badge ' + (d.paper_mode ? 'paper' : 'live');
        document.getElementById('uptime').textContent = formatUptime(d.uptime_secs);

        const bal = parseFloat(d.current_balance);
        const start = parseFloat(d.starting_balance);
        const balEl = document.getElementById('balance');
        balEl.textContent = '$' + d.current_balance;
        balEl.className = 'pnl-main ' + (bal >= start ? 'pnl-positive' : 'pnl-negative');

        const retEl = document.getElementById('return-pct');
        const retPct = parseFloat(d.total_return_pct);
        retEl.textContent = d.total_return_pct + '%';
        retEl.style.color = retPct >= 0 ? 'var(--accent)' : 'var(--danger)';

        const pnlEl = document.getElementById('pnl');
        const pnl = parseFloat(d.realized_pnl);
        pnlEl.textContent = '$' + d.realized_pnl;
        pnlEl.style.color = pnl >= 0 ? 'var(--accent)' : 'var(--danger)';

        document.getElementById('starting').textContent = '$' + d.starting_balance;
        document.getElementById('fills').textContent = d.total_fills;
        document.getElementById('rebates').textContent = '$' + d.rebates_earned;
        document.getElementById('orders').textContent = d.open_orders;
        document.getElementById('positions').textContent = d.tracked_positions;
        document.getElementById('fees').textContent = '$' + d.fees_paid;

        // New stats
        document.getElementById('markets').textContent = d.market_count;
        const wsEl = document.getElementById('ws-status');
        wsEl.textContent = d.ws_connected ? 'Connected' : 'Disconnected';
        wsEl.style.color = d.ws_connected ? 'var(--accent)' : 'var(--danger)';
        document.getElementById('latency').textContent = d.api_latency_ms + 'ms';
        document.getElementById('loops').textContent = d.loop_count.toLocaleString();
        document.getElementById('orders-placed').textContent = d.orders_placed;

        // Error panel
        const errorPanel = document.getElementById('error-panel');
        if (d.last_error) {
          errorPanel.style.display = 'block';
          document.getElementById('last-error').textContent = d.last_error;
        } else {
          errorPanel.style.display = 'none';
        }

        document.getElementById('timestamp').textContent = 'Last updated: ' + d.timestamp;

        const tbody = document.getElementById('fills-table');
        if (d.recent_fills.length === 0) {
          tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:var(--muted);">No fills yet</td></tr>';
        } else {
          tbody.innerHTML = d.recent_fills.map(f => {
            const sideClass = f.side === 'BUY' ? 'fill-buy' : 'fill-sell';
            return '<tr><td>' + f.time + '</td><td class="' + sideClass + '">' + f.side + '</td><td>' + f.size + '</td><td>' + f.price + '</td><td>' + f.value + '</td><td>' + f.rebate + '</td><td>' + f.pnl + '</td></tr>';
          }).join('');
        }
      } catch(e) {
        console.error('Failed to load data:', e);
      }
    }

    load();
    setInterval(load, 3000);
  </script>
</body>
</html>
"##;

// Required for Decimal::from_str
use std::str::FromStr;
