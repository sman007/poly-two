use std::collections::{HashMap, HashSet};
use std::env;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{extract::State, response::Html, routing::get, Json, Router};
use chrono::Utc;
use dotenv::dotenv;
use futures_util::{future::join_all, StreamExt};
use log::{debug, error, info, warn};
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
use polymarket_client_sdk::types::U256;
use polymarket_client_sdk::types::{Address, Decimal, B256};
use polymarket_client_sdk::POLYGON;
use reqwest::Client as HttpClient;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
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
// ARBITRAGE CONSTANTS (Reference Wallet Strategy)
// ============================================================================

/// Arb threshold for maker execution: trigger when YES_ask + NO_ask < 0.99
const ARB_THRESHOLD_MAKER: Decimal = Decimal::from_parts(99, 0, 0, false, 2);

/// Arb threshold for taker fallback: trigger when sum < 0.98 (covers 1% taker fees)
const ARB_THRESHOLD_TAKER: Decimal = Decimal::from_parts(98, 0, 0, false, 2);

/// Size per arb trade as fraction of balance (0.2% = 0.002)
const ARB_SIZE_PCT: Decimal = Decimal::from_parts(2, 0, 0, false, 3);

/// Maximum size per side of arb trade ($5000)
const MAX_ARB_SIZE: Decimal = Decimal::from_parts(5000, 0, 0, false, 0);

/// Imbalance threshold for rehedging (5% of balance)
const IMBALANCE_THRESHOLD_PCT: Decimal = Decimal::from_parts(5, 0, 0, false, 2);

/// Gamma API base URL for market discovery
const GAMMA_API: &str = "https://gamma-api.polymarket.com";

/// Market refresh interval (how often to scan for new 15-min markets)
const MARKET_REFRESH_SECS: u64 = 60;

// ============================================================================
// GAMMA API TYPES (for market discovery)
// ============================================================================

/// Event from Gamma API (contains nested markets)
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaEvent {
    title: String,
    #[serde(default)]
    active: bool,
    #[serde(default)]
    closed: bool,
    #[serde(default)]
    markets: Vec<GammaMarket>,
}

/// Market nested within an event
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct GammaMarket {
    question: String,
    #[serde(default)]
    clob_token_ids: Option<String>,
    #[serde(default)]
    active: bool,
    #[serde(default)]
    closed: bool,
    #[serde(default)]
    volume24hr: f64,
}

/// Trade from the Polymarket data API
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiTrade {
    asset: String,
    side: String,
    size: f64,
    price: f64,
    timestamp: u64,
}

/// 15-minute interval in seconds
const INTERVAL_15M: u64 = 900;

/// Asset prefixes for 15-minute updown markets (Reference Wallet: BTC/ETH/SOL)
const UPDOWN_ASSETS: [&str; 3] = ["btc", "eth", "sol"];

/// Calculate 15-minute window timestamps for discovery
/// Returns timestamps for current window and next few windows
fn get_15min_window_timestamps() -> Vec<u64> {
    let now = systemtime_now_secs();
    // Round down to nearest 15-minute boundary
    let current_window = (now / INTERVAL_15M) * INTERVAL_15M;

    // Return current window plus next 3 windows (to catch markets about to start)
    vec![
        current_window,
        current_window + INTERVAL_15M,
        current_window + INTERVAL_15M * 2,
        current_window + INTERVAL_15M * 3,
    ]
}

/// Fetch 15-minute crypto UP/DOWN markets by querying specific slugs
/// Format: {asset}-updown-15m-{timestamp} (e.g., btc-updown-15m-1768587300)
async fn discover_15min_markets(
    http_client: &HttpClient,
) -> Result<Vec<(String, String)>, Box<dyn std::error::Error + Send + Sync>> {
    let timestamps = get_15min_window_timestamps();
    let mut pairs = Vec::new();
    let mut found_slugs = Vec::new();

    // Query each asset + timestamp combination
    for asset in UPDOWN_ASSETS {
        for &ts in &timestamps {
            let slug = format!("{}-updown-15m-{}", asset, ts);
            let url = format!("{}/events?slug={}", GAMMA_API, slug);

            let response = match http_client
                .get(&url)
                .timeout(Duration::from_secs(10))
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    debug!("Failed to query {}: {}", slug, e);
                    continue;
                }
            };

            if !response.status().is_success() {
                continue;
            }

            let events: Vec<GammaEvent> = match response.json().await {
                Ok(e) => e,
                Err(_) => continue,
            };

            // Process found events
            for event in events {
                if !event.active || event.closed {
                    continue;
                }

                for market in &event.markets {
                    if market.closed {
                        continue;
                    }

                    // Parse clobTokenIds - may be a JSON string or already an array
                    if let Some(tokens_str) = &market.clob_token_ids {
                        let tokens: Result<Vec<String>, _> = serde_json::from_str(tokens_str);
                        if let Ok(tokens) = tokens {
                            if tokens.len() == 2 {
                                // tokens[0] = Up, tokens[1] = Down
                                pairs.push((tokens[0].clone(), tokens[1].clone()));
                                found_slugs.push(slug.clone());
                                info!(
                                    "Found 15-min market: {} | Up: {}... Down: {}...",
                                    event.title,
                                    &tokens[0][..20.min(tokens[0].len())],
                                    &tokens[1][..20.min(tokens[1].len())]
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    if pairs.is_empty() {
        debug!("No active 15-minute UP/DOWN markets found");
    } else {
        info!(
            "Discovered {} 15-minute UP/DOWN market(s): {:?}",
            pairs.len(),
            found_slugs
        );
    }

    Ok(pairs)
}

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
    /// Manual markets (optional, fallback if auto-discovery finds nothing)
    manual_market_pairs: Vec<(String, String)>,

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

/// Arbitrage position tracking (Reference Wallet Strategy)
/// Tracks paired YES/NO positions that guarantee profit at resolution
#[derive(Clone, Debug, Serialize)]
struct ArbPosition {
    /// Unique ID for this arb position
    id: String,
    /// YES token ID
    yes_token: String,
    /// NO token ID
    no_token: String,
    /// Target size for each side
    target_size: Decimal,
    /// Cost per share (YES price + NO price at entry)
    cost_per_share: Decimal,
    /// Amount of YES tokens filled
    yes_filled: Decimal,
    /// Amount of NO tokens filled
    no_filled: Decimal,
    /// YES entry price
    yes_price: Decimal,
    /// NO entry price
    no_price: Decimal,
    /// Timestamp when position was opened
    created_at: u64,
    /// Whether this position has resolved (market ended)
    resolved: bool,
    /// Profit at resolution (if resolved)
    resolved_pnl: Decimal,
}

impl ArbPosition {
    /// Expected profit per share at resolution
    fn expected_profit_per_share(&self) -> Decimal {
        Decimal::ONE - self.cost_per_share
    }

    /// Total expected profit if fully filled
    fn expected_total_profit(&self) -> Decimal {
        let filled = self.yes_filled.min(self.no_filled);
        filled * self.expected_profit_per_share()
    }

    /// Check if position is balanced (YES and NO fills are equal)
    fn is_balanced(&self) -> bool {
        (self.yes_filled - self.no_filled).abs() < Decimal::from_parts(1, 0, 0, false, 2)
        // 0.01 tolerance
    }

    /// Get imbalance amount (positive = more YES, negative = more NO)
    fn imbalance(&self) -> Decimal {
        self.yes_filled - self.no_filled
    }
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
    // Track last trade prices for unrealized P&L
    last_trade_prices: HashMap<String, Decimal>,
    // Recent fills for dashboard display
    recent_fills: Vec<FillRecord>,
    // Performance stats
    api_latency_ms: u64,
    ws_connected: bool,
    loop_count: u64,
    orders_placed: u64,
    last_error: Option<String>,
    // Dynamic market discovery
    active_markets: Vec<(String, String)>,
    last_market_refresh: u64,
    // Arbitrage position tracking (Reference Wallet Strategy)
    arb_positions: HashMap<String, ArbPosition>,
    arb_position_count: u64,
    // Stats for arb strategy
    arb_opportunities_found: u64,
    arb_positions_resolved: u64,
    arb_total_profit: Decimal,
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

/// Arb position data for dashboard display (hedged pairs)
#[derive(Debug, Clone, Serialize)]
struct ArbPositionData {
    id_short: String,
    size: String,
    cost_per_share: String,
    guaranteed_profit: String,
    spread_pct: String,
    age_secs: u64,
    status: String,
}

/// Dashboard API response
#[derive(Serialize)]
struct DashboardData {
    paper_mode: bool,
    uptime_secs: u64,
    starting_balance: String,
    available_balance: String,
    deployed_capital: String,
    // Clear P&L metrics
    realized_profit: String,
    pending_profit: String,
    total_profit: String,
    rebates_earned: String,
    fees_paid: String,
    total_fills: u64,
    open_orders: usize,
    open_arbs: usize,
    arb_positions: Vec<ArbPositionData>,
    recent_fills: Vec<FillRecord>,
    timestamp: String,
    // Stats
    market_count: usize,
    api_latency_ms: u64,
    ws_connected: bool,
    loop_count: u64,
    orders_placed: u64,
    last_error: Option<String>,
    exposure_pct: String,
    max_exposure_pct: String,
}

/// Shared state for web dashboard
#[derive(Clone)]
struct AppState {
    bot_state: Arc<RwLock<BotState>>,
    config: Config,
    start_time: Instant,
}

/// WebSocket context for listener tasks
struct WsContext<S: Signer> {
    state: Arc<RwLock<BotState>>,
    credentials: Credentials,
    address: Address,
    markets: Vec<B256>,
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

    // Create HTTP client for market discovery
    let http_client = HttpClient::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    // Initial market discovery
    info!("Discovering 15-minute crypto markets...");
    let initial_markets = match discover_15min_markets(&http_client).await {
        Ok(markets) if !markets.is_empty() => markets,
        Ok(_) => {
            if config.manual_market_pairs.is_empty() {
                warn!("No 15-minute crypto markets found and no manual markets configured");
                warn!(
                    "Bot will keep scanning for markets every {} seconds",
                    MARKET_REFRESH_SECS
                );
            }
            config.manual_market_pairs.clone()
        }
        Err(e) => {
            warn!("Market discovery failed: {}. Using manual markets.", e);
            config.manual_market_pairs.clone()
        }
    };

    let state = Arc::new(RwLock::new(BotState {
        balance: if config.paper_trade {
            config.paper_starting_balance
        } else {
            Decimal::ZERO
        },
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
        last_trade_prices: HashMap::new(),
        recent_fills: Vec::new(),
        api_latency_ms: 0,
        ws_connected: false,
        loop_count: 0,
        orders_placed: 0,
        last_error: None,
        active_markets: initial_markets,
        last_market_refresh: systemtime_now_secs(),
        // Arbitrage position tracking (Reference Wallet Strategy)
        arb_positions: HashMap::new(),
        arb_position_count: 0,
        arb_opportunities_found: 0,
        arb_positions_resolved: 0,
        arb_total_profit: Decimal::ZERO,
    }));

    if config.paper_trade {
        info!("══════════════════════════════════════════════════════════════");
        info!("   PAPER TRADING MODE - ARBITRAGE STRATEGY (Reference Wallet)");
        info!(
            "   Strategy: Buy YES + NO when sum < ${:.2}",
            ARB_THRESHOLD_MAKER
        );
        info!("   Starting balance: ${}", config.paper_starting_balance);
        info!(
            "   Size per trade: {:.1}% of balance (max ${})",
            ARB_SIZE_PCT * Decimal::from(100),
            MAX_ARB_SIZE
        );
        info!(
            "   Maker rebate: {}%",
            MAKER_REBATE_RATE * Decimal::from(100)
        );
        info!("══════════════════════════════════════════════════════════════");
    }

    // Get initial token IDs for WebSocket subscription
    let tokens: Vec<String> = {
        let locked = state.read().await;
        locked
            .active_markets
            .iter()
            .flat_map(|(yes, no): &(String, String)| vec![yes.clone(), no.clone()])
            .collect()
    };

    let markets = if !tokens.is_empty() {
        match fetch_market_ids(&client, &tokens, &config).await {
            Ok(m) => m,
            Err(e) => {
                warn!(
                    "Failed to fetch market IDs: {}. WS will connect without subscriptions.",
                    e
                );
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };
    info!("WebSocket subscribing to {} market IDs", markets.len());

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

    // Spawn periodic market refresh task
    let refresh_state = state.clone();
    let refresh_shutdown = shutdown_token.clone();
    let refresh_config = config.clone();
    let refresh_http = http_client.clone();

    tokio::spawn(async move {
        periodic_market_refresh(
            refresh_state,
            refresh_shutdown,
            refresh_config,
            refresh_http,
        )
        .await;
    });

    // Spawn dashboard web server
    let app_state = AppState {
        bot_state: state.clone(),
        config: config.clone(),
        start_time,
    };
    tokio::spawn(async move {
        run_dashboard_server(app_state).await;
    });

    // Spawn paper trade polling task (polls data API as WebSocket backup)
    if config.paper_trade {
        let poll_state = state.clone();
        let poll_http = http_client.clone();
        let poll_config = config.clone();
        let poll_shutdown = shutdown_token.clone();
        tokio::spawn(async move {
            poll_trades_for_paper_fills(poll_state, poll_http, poll_config, poll_shutdown).await;
        });
        info!("Paper trade polling task spawned (polling data API every 5s)");
    }

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
                warn!("Balance fetch timed out after {}s", config.api_timeout_secs);
                sleep(Duration::from_millis(config.main_loop_interval_ms)).await;
                return;
            }
        }
    };

    if current_balance < config.min_balance {
        error!(
            "{}Balance too low: {} (minimum: {}). Shutting down.",
            if config.paper_trade { "[PAPER] " } else { "" },
            current_balance,
            config.min_balance
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

    // Get current active markets from state
    let market_pairs: Vec<(String, String)> = {
        let locked = state.read().await;
        locked.active_markets.clone()
    };

    if market_pairs.is_empty() {
        debug!("No active markets to trade - waiting for market discovery");
        sleep(Duration::from_millis(config.main_loop_interval_ms)).await;
        return;
    }

    // Parallel orderbook fetches - process all markets simultaneously
    // This reduces latency from O(n * 50ms) to O(50ms) for n markets
    let futures: Vec<_> = market_pairs
        .iter()
        .map(|(yes_token, no_token)| {
            let state = Arc::clone(state);
            let client = Arc::clone(client);
            let signer = Arc::clone(signer);
            let config = config.clone();
            let yes = yes_token.clone();
            let no = no_token.clone();
            async move {
                let result = retry(
                    || find_arb_opportunity(&state, &client, signer.as_ref(), &yes, &no, &config),
                    &config,
                )
                .await;
                (yes, result)
            }
        })
        .collect();

    let results = join_all(futures).await;
    for (yes_token, result) in results {
        if let Err(e) = result {
            warn!("Arb search failed for {}: {:?}", yes_token, e);
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
                warn!(
                    "Cancel order {} returned error (removing from tracking): {:?}",
                    id, e
                );
                to_remove.push(id);
            }
            Err(_) => {
                warn!(
                    "Timeout canceling order {} (keeping in tracking for retry)",
                    id
                );
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

                    // CRITICAL: Simulate 15-minute market resolution for arb positions
                    // Reference Wallet: positions resolve in 15 min, freeing capital for new arbs
                    const RESOLUTION_TIME_SECS: u64 = 15 * 60; // 15 minutes

                    // Collect resolutions first to avoid borrow conflicts
                    // Tuple: (id, filled, payout, profit, profit_per_share, yes_token, no_token)
                    let resolutions: Vec<(String, Decimal, Decimal, Decimal, Decimal, String, String)> = locked
                        .arb_positions
                        .iter()
                        .filter(|(_, arb)| {
                            !arb.resolved
                                && now.saturating_sub(arb.created_at) >= RESOLUTION_TIME_SECS
                        })
                        .map(|(id, arb)| {
                            // Paper mode: assume full fill at target_size
                            // (In live mode, this would use actual yes_filled.min(no_filled))
                            let filled = arb.target_size;
                            let payout = filled * Decimal::ONE;
                            let profit_per_share = Decimal::ONE - arb.cost_per_share;
                            let profit = filled * profit_per_share;
                            (id.clone(), filled, payout, profit, profit_per_share, arb.yes_token.clone(), arb.no_token.clone())
                        })
                        .collect();

                    // Apply resolutions (two passes to avoid borrow conflicts)
                    let mut total_payout = Decimal::ZERO;
                    let mut total_profit = Decimal::ZERO;

                    // First pass: update arb positions
                    for (id, _filled, payout, profit, _profit_per_share, _yes_token, _no_token) in &resolutions {
                        if let Some(arb) = locked.arb_positions.get_mut(id) {
                            arb.resolved = true;
                            arb.resolved_pnl = *profit;
                            total_payout += payout;
                            total_profit += profit;
                        }
                    }

                    // Second pass: clear exposure and log (separate borrow scope)
                    for (id, filled, payout, profit, profit_per_share, yes_token, no_token) in &resolutions {
                        // CRITICAL: Clear exposure for both tokens to free up capital
                        locked.exposure.remove(yes_token);
                        locked.exposure.remove(no_token);

                        info!(
                            "[PAPER] ARB RESOLVED: {} | Size: {:.2} | Payout: ${:.2} | Profit: ${:.2} ({:.2}%) | Exposure cleared",
                            id,
                            filled,
                            payout,
                            profit,
                            profit_per_share * Decimal::from(100)
                        );
                    }

                    if !resolutions.is_empty() {
                        locked.paper_balance += total_payout;
                        locked.paper_realized_pnl += total_profit;
                        locked.arb_positions_resolved += resolutions.len() as u64;
                        info!(
                            "[PAPER] Resolved {} arb positions, total payout: ${:.2}, profit: ${:.2}",
                            resolutions.len(),
                            total_payout,
                            total_profit
                        );
                    }

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

/// Periodically refresh market discovery to find new 15-min crypto markets
async fn periodic_market_refresh(
    state: Arc<RwLock<BotState>>,
    shutdown: CancellationToken,
    config: Config,
    http_client: HttpClient,
) {
    let refresh_interval = Duration::from_secs(MARKET_REFRESH_SECS);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("Market refresh task shutting down...");
                break;
            }
            _ = sleep(refresh_interval) => {
                debug!("Refreshing 15-minute crypto markets...");

                match discover_15min_markets(&http_client).await {
                    Ok(markets) if !markets.is_empty() => {
                        let mut locked = state.write().await;
                        let old_count = locked.active_markets.len();
                        locked.active_markets = markets;
                        locked.last_market_refresh = systemtime_now_secs();

                        if locked.active_markets.len() != old_count {
                            info!(
                                "Market refresh: {} -> {} active 15-min markets",
                                old_count, locked.active_markets.len()
                            );
                        }
                    }
                    Ok(_) => {
                        // No markets found, fall back to manual if available
                        if !config.manual_market_pairs.is_empty() {
                            let mut locked = state.write().await;
                            if locked.active_markets.is_empty() {
                                locked.active_markets = config.manual_market_pairs.clone();
                                info!("No 15-min markets found, using {} manual markets", locked.active_markets.len());
                            }
                        } else {
                            debug!("No 15-minute crypto markets available");
                        }
                    }
                    Err(e) => {
                        let mut locked = state.write().await;
                        locked.last_error = Some(format!("Market discovery: {}", e));
                        warn!("Market refresh failed: {}", e);
                    }
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

    // MARKETS is now optional - bot will auto-discover 15-min crypto markets
    let manual_market_pairs: Vec<(String, String)> = env::var("MARKETS")
        .ok()
        .map(|markets_str| {
            markets_str
                .split(';')
                .filter_map(|pair| {
                    let parts: Vec<&str> = pair.split(',').collect();
                    if parts.len() == 2
                        && !parts[0].trim().is_empty()
                        && !parts[1].trim().is_empty()
                    {
                        Some((parts[0].trim().to_string(), parts[1].trim().to_string()))
                    } else {
                        warn!("Skipping invalid market pair: {}", pair);
                        None
                    }
                })
                .collect()
        })
        .unwrap_or_default();

    if !manual_market_pairs.is_empty() {
        info!("Manual markets configured: {}", manual_market_pairs.len());
    } else {
        info!("No manual markets - will auto-discover 15-minute crypto markets");
    }

    // Timing configuration with validation
    let stale_order_timeout_secs = parse_u64_env("STALE_ORDER_TIMEOUT_SECS", "30")?;
    if stale_order_timeout_secs == 0 {
        return Err("STALE_ORDER_TIMEOUT_SECS must be > 0".into());
    }

    let main_loop_interval_ms = parse_u64_env("MAIN_LOOP_INTERVAL_MS", "100")?;
    if main_loop_interval_ms == 0 {
        return Err("MAIN_LOOP_INTERVAL_MS must be > 0".into());
    }

    // Risk parameters with validation
    let rehedge_threshold_pct = parse_f64_env("REHEDGE_THRESHOLD_PCT", "0.05")?;
    if !(0.0..=1.0).contains(&rehedge_threshold_pct) {
        return Err("REHEDGE_THRESHOLD_PCT must be between 0 and 1".into());
    }

    let max_exposure_pct = parse_f64_env("MAX_EXPOSURE_PCT", "0.8")?; // 80% - arb positions are fully hedged/risk-free
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
    let cleanup_interval_secs = parse_u64_env("CLEANUP_INTERVAL_SECS", "30")?; // Faster for resolution checks
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
    let paper_starting_balance = parse_decimal_env("PAPER_STARTING_BALANCE", "10000.0")?;

    Ok(Config {
        wallet_private_key,
        min_balance,
        offset,
        max_spread,
        manual_market_pairs,
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
) -> Result<Vec<B256>, Box<dyn std::error::Error>> {
    let mut markets = HashSet::new();
    for token_id in token_ids {
        let token_u256 = match token_to_u256(token_id) {
            Ok(u) => u,
            Err(e) => {
                warn!("Invalid token ID {}: {:?}", token_id, e);
                continue;
            }
        };
        let request = OrderBookSummaryRequest::builder()
            .token_id(token_u256)
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
// TOKEN ID HELPERS
// ============================================================================

/// Convert token ID string to U256 for SDK 0.4+
fn token_to_u256(token_id: &str) -> Result<U256, Box<dyn std::error::Error>> {
    U256::from_str(token_id).map_err(|e| -> Box<dyn std::error::Error> {
        format!("Invalid token ID '{}': {}", token_id, e).into()
    })
}

/// Convert token ID string to U256 (Send + Sync version for async contexts)
fn token_to_u256_sync(token_id: &str) -> Result<U256, Box<dyn std::error::Error + Send + Sync>> {
    U256::from_str(token_id).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
        format!("Invalid token ID '{}': {}", token_id, e).into()
    })
}

/// Convert U256 token ID back to string for storage/display
fn u256_to_token(u: &U256) -> String {
    u.to_string()
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
    let token_u256 = token_to_u256_sync(token)?;
    let order = client
        .limit_order()
        .token_id(token_u256)
        .side(side)
        .price(price)
        .size(size)
        .order_type(OrderType::GTC)
        .post_only(true) // Reference Wallet: use post-only to ensure maker rebates
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

/// Find arbitrage opportunity (Reference Wallet Strategy)
/// Pure arbitrage: buy YES + NO when sum < threshold, hold to resolution
/// Zero directional risk - always hedged
async fn find_arb_opportunity<S: Signer + Sync>(
    state: &RwLock<BotState>,
    client: &SharedClient,
    signer: &S,
    yes_token: &str,
    no_token: &str,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    // Track API latency - fetch both orderbooks in parallel
    let start = Instant::now();
    let yes_u256 = token_to_u256(yes_token)?;
    let no_u256 = token_to_u256(no_token)?;
    let req_yes = OrderBookSummaryRequest::builder()
        .token_id(yes_u256)
        .build();
    let req_no = OrderBookSummaryRequest::builder().token_id(no_u256).build();
    let (book_yes_result, book_no_result) =
        tokio::join!(client.order_book(&req_yes), client.order_book(&req_no));
    let book_yes = book_yes_result?;
    let book_no = book_no_result?;
    let latency_ms = start.elapsed().as_millis() as u64;

    // Update latency in state
    {
        let mut locked = state.write().await;
        locked.api_latency_ms = latency_ms;
    }

    // Get best BID prices (Reference Wallet: "Buy YES and NO at best bid")
    // We place limit orders at BID to be MAKER and earn rebates
    let Some((best_bid_yes, _)) = best_bid_ask(&book_yes) else {
        debug!("Skipping {}: empty orderbook", yes_token);
        return Ok(());
    };

    let Some((best_bid_no, _)) = best_bid_ask(&book_no) else {
        debug!("Skipping {}: empty orderbook", no_token);
        return Ok(());
    };

    // CORE ARB LOGIC (Reference Wallet): Check if YES_bid + NO_bid < threshold
    // Place bids, wait for sellers to fill us (MAKER), hold to resolution for $1
    let price_sum = best_bid_yes + best_bid_no;

    // Determine if arb opportunity exists
    let is_maker_arb = price_sum < ARB_THRESHOLD_MAKER; // < 0.99
    let _is_taker_arb = price_sum < ARB_THRESHOLD_TAKER; // < 0.98 (unused, maker-only)

    if !is_maker_arb {
        // No arb opportunity - sum >= 0.99
        debug!(
            "No arb: YES_bid={:.4} + NO_bid={:.4} = {:.4} (need < {:.2})",
            best_bid_yes, best_bid_no, price_sum, ARB_THRESHOLD_MAKER
        );
        return Ok(());
    }

    // ARB OPPORTUNITY FOUND!
    let expected_profit_pct = (Decimal::ONE - price_sum) * Decimal::from(100);
    info!(
        "{}ARB FOUND: YES_bid={:.4} + NO_bid={:.4} = {:.4} | Profit: {:.2}%",
        if config.paper_trade { "[PAPER] " } else { "" },
        best_bid_yes,
        best_bid_no,
        price_sum,
        expected_profit_pct
    );

    // Get current balance and calculate position size
    let (balance, total_arb_exposure) = {
        let locked = state.read().await;
        let arb_exposure: Decimal = locked
            .arb_positions
            .values()
            .filter(|p| !p.resolved)
            .map(|p| p.target_size * p.cost_per_share) // cost_per_share = YES_price + NO_price already
            .sum();
        (locked.balance, arb_exposure)
    };

    // Position sizing: Kelly criterion (Reference Wallet style)
    // Kelly formula: size = (edge / variance) * kelly_fraction * balance
    // Use ACTUAL arb spread as edge (not config.edge) for dynamic sizing
    let actual_edge = Decimal::ONE - price_sum; // e.g., 1.0 - 0.98 = 0.02 (2% spread)
    let variance =
        Decimal::from_f64(config.variance).unwrap_or(Decimal::from_parts(1, 0, 0, false, 2));
    let kelly_frac =
        Decimal::from_f64(config.kelly_fraction).unwrap_or(Decimal::from_parts(2, 0, 0, false, 1));
    let kelly_size = (actual_edge / variance) * kelly_frac * balance;

    // Cap position size by exposure limit (use config.max_exposure_pct, default 70% for risk-free arbs)
    let max_exposure_decimal = Decimal::from_f64(config.max_exposure_pct)
        .unwrap_or(Decimal::from_parts(7, 0, 0, false, 1));
    let max_arb_exposure = balance * max_exposure_decimal;
    let remaining_headroom = max_arb_exposure.saturating_sub(total_arb_exposure);
    // exposure = size * price_sum (cost_per_share = YES+NO already), so max_size = headroom / price_sum
    let max_size_from_exposure = remaining_headroom / price_sum;

    // Take minimum of Kelly, MAX_ARB_SIZE, and exposure-limited size
    let size = kelly_size
        .min(MAX_ARB_SIZE)
        .min(max_size_from_exposure)
        .round_dp(2);

    if size <= Decimal::ZERO {
        debug!(
            "Skipping arb: no headroom (exposure {:.2}/{:.2})",
            total_arb_exposure, max_arb_exposure
        );
        return Ok(());
    }

    // Generate arb position ID
    let (arb_id, base_order_num) = {
        let mut locked = state.write().await;
        let id = locked.arb_position_count;
        locked.arb_position_count += 1;
        locked.paper_order_count += 2; // Reserve 2 order numbers (YES and NO buys only)
        (format!("arb_{}", id), locked.paper_order_count - 2)
    };

    // Place BUY orders for BOTH YES and NO at BID prices (MAKER execution)
    // Reference Wallet: "Buy YES and NO at best bid (or mid - offset for safety)"
    // Orders rest on book, we earn maker rebate when sellers fill us
    let order_specs = [
        (
            yes_token,
            Side::Buy,
            best_bid_yes,
            "Arb Buy YES",
            base_order_num,
        ),
        (
            no_token,
            Side::Buy,
            best_bid_no,
            "Arb Buy NO",
            base_order_num + 1,
        ),
    ];

    let order_futures = order_specs.map(|(token, side, price, label, order_num)| {
        place_single_order(
            client,
            signer,
            token,
            side,
            price,
            size,
            label,
            config.paper_trade,
            order_num,
        )
    });

    let results = futures_util::future::join_all(order_futures).await;

    // Track results
    let mut yes_order_id: Option<String> = None;
    let mut no_order_id: Option<String> = None;
    let mut paper_orders_to_add: Vec<PaperOrder> = Vec::new();

    for (i, result) in results.into_iter().enumerate() {
        match result {
            Ok(Some((id, paper_order))) => {
                if i == 0 {
                    yes_order_id = Some(id.clone());
                } else {
                    no_order_id = Some(id.clone());
                }
                if let Some(po) = paper_order {
                    paper_orders_to_add.push(po);
                }
            }
            Ok(None) => {
                warn!("Arb order {} failed", if i == 0 { "YES" } else { "NO" });
            }
            Err(e) => {
                warn!("Arb order placement error: {:?}", e);
            }
        }
    }

    // Only create arb position if both orders were placed
    if yes_order_id.is_some() && no_order_id.is_some() {
        let now = systemtime_now_secs();
        let mut locked = state.write().await;

        // Track orders
        if let Some(ref id) = yes_order_id {
            locked.open_orders.insert(id.clone(), now);
        }
        if let Some(ref id) = no_order_id {
            locked.open_orders.insert(id.clone(), now);
        }
        locked.orders_placed += 2;

        // Paper mode: simulate IMMEDIATE fills for arb orders
        // Reference Wallet assumes fills happen instantly at bid price (liquidity is there)
        // We deduct cost and mark position as filled, then wait for 15-min resolution
        let (yes_filled, no_filled) = if config.paper_trade {
            // Calculate total cost: (YES_price + NO_price) * size
            let total_cost = price_sum * size;

            // Deduct from paper balance
            locked.paper_balance -= total_cost;

            // Add rebates for both fills
            let rebate = total_cost * config.rebate_rate;
            locked.paper_balance += rebate;
            locked.paper_rebates_earned += rebate;

            // Track exposure for both tokens
            *locked
                .exposure
                .entry(yes_token.to_string())
                .or_insert(Decimal::ZERO) += size;
            *locked
                .exposure
                .entry(no_token.to_string())
                .or_insert(Decimal::ZERO) += size;

            // Track entry prices
            locked
                .paper_entry_prices
                .insert(yes_token.to_string(), best_bid_yes);
            locked
                .paper_entry_prices
                .insert(no_token.to_string(), best_bid_no);

            // Count fills
            locked.paper_fills += 2;

            // Record fills for dashboard
            let fill_record_yes = FillRecord {
                time: chrono::Utc::now().format("%H:%M:%S").to_string(),
                side: "BUY".to_string(),
                size: format!("{:.2}", size),
                price: format!("{:.4}", best_bid_yes),
                value: format!("${:.2}", best_bid_yes * size),
                rebate: format!("${:.4}", rebate / Decimal::from(2)),
                pnl: "-".to_string(),
            };
            let fill_record_no = FillRecord {
                time: chrono::Utc::now().format("%H:%M:%S").to_string(),
                side: "BUY".to_string(),
                size: format!("{:.2}", size),
                price: format!("{:.4}", best_bid_no),
                value: format!("${:.2}", best_bid_no * size),
                rebate: format!("${:.4}", rebate / Decimal::from(2)),
                pnl: "-".to_string(),
            };
            locked.recent_fills.push(fill_record_yes);
            locked.recent_fills.push(fill_record_no);

            info!(
                "[PAPER] INSTANT FILL: BUY {} YES @ {:.4} + {} NO @ {:.4} = ${:.2} total cost",
                size, best_bid_yes, size, best_bid_no, total_cost
            );

            (size, size) // Both sides fully filled
        } else {
            // Live mode: store paper orders for fill simulation
            for po in paper_orders_to_add {
                locked.paper_orders.insert(po.id.clone(), po);
            }
            (Decimal::ZERO, Decimal::ZERO)
        };

        // Create and track arb position
        let arb_position = ArbPosition {
            id: arb_id.clone(),
            yes_token: yes_token.to_string(),
            no_token: no_token.to_string(),
            target_size: size,
            cost_per_share: price_sum,
            yes_filled,
            no_filled,
            yes_price: best_bid_yes,
            no_price: best_bid_no,
            created_at: now,
            resolved: false,
            resolved_pnl: Decimal::ZERO,
        };

        locked.arb_positions.insert(arb_id.clone(), arb_position);
        locked.arb_opportunities_found += 1;

        info!(
            "{}ARB POSITION OPENED: {} | Size: {} | Cost: {:.4}/share | Expected profit: {:.2}%",
            if config.paper_trade { "[PAPER] " } else { "" },
            arb_id,
            size,
            price_sum,
            expected_profit_pct
        );

        if config.paper_trade {
            info!(
                "[PAPER] Balance: ${:.2} | Open arb positions: {} | Total arb profit: ${:.2}",
                locked.paper_balance,
                locked.arb_positions.len(),
                locked.arb_total_profit
            );
        }
    } else {
        // One side failed - cancel the other if it was placed
        // In paper mode, just log
        warn!(
            "Arb position incomplete - YES: {:?}, NO: {:?}",
            yes_order_id, no_order_id
        );
    }

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
    let asset_id_str = u256_to_token(&trade.asset_id);
    let exposure = state
        .exposure
        .entry(asset_id_str.clone())
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
        Some((u256_to_token(&trade.asset_id), current_exposure))
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
            order.token_id == u256_to_token(&trade.asset_id) && order.filled < order.size && {
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
                side: if order.side == Side::Buy {
                    "BUY".to_string()
                } else {
                    "SELL".to_string()
                },
                size: format!("{:.2}", fill_size),
                price: format!("{:.4}", fill_price),
                value: format!("${:.2}", fill_value),
                rebate: format!("${:.4}", rebate),
                pnl: if fill_pnl != Decimal::ZERO {
                    format!("${:.2}", fill_pnl)
                } else {
                    "-".to_string()
                },
            };
            state.recent_fills.push(fill_record);

            // Keep only last 100 fills
            if state.recent_fills.len() > 100 {
                state.recent_fills.remove(0);
            }

            info!(
                "[PAPER] FILL: {} {} @ {:.4} (slippage: {:.4}%) | Value: ${:.2} | Rebate: ${:.4}",
                if order.side == Side::Buy {
                    "BUY"
                } else {
                    "SELL"
                },
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
    // Convert token_id to U256
    let token_u256 = token_to_u256_sync(token_id)?;

    // Get current orderbook
    let book = match timeout(
        Duration::from_secs(config.api_timeout_secs),
        client.order_book(
            &OrderBookSummaryRequest::builder()
                .token_id(token_u256)
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
        .token_id(token_u256)
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

/// Poll trades from the data API for paper fill simulation
/// This runs alongside WebSocket as a backup when WS fails
async fn poll_trades_for_paper_fills(
    state: Arc<RwLock<BotState>>,
    http_client: HttpClient,
    config: Config,
    shutdown: CancellationToken,
) {
    const POLL_INTERVAL_SECS: u64 = 5;
    const DATA_API: &str = "https://data-api.polymarket.com";

    // Start with 0 to process all trades on first poll, then track the max seen
    let mut last_seen_timestamp: u64 = 0;

    info!("Starting trade polling for paper fill simulation...");

    loop {
        if shutdown.is_cancelled() {
            info!("Trade polling shutting down...");
            break;
        }

        sleep(Duration::from_secs(POLL_INTERVAL_SECS)).await;

        // Get our tracked token IDs
        let tracked_tokens: HashSet<String> = {
            let locked = state.read().await;
            locked
                .active_markets
                .iter()
                .flat_map(|(yes, no)| vec![yes.clone(), no.clone()])
                .collect()
        };

        if tracked_tokens.is_empty() {
            info!("[POLL] No tracked tokens yet, skipping...");
            continue;
        }

        // Fetch recent trades from the data API
        let url = format!("{}/trades?limit=100", DATA_API);
        let trades = match http_client
            .get(&url)
            .timeout(Duration::from_secs(10))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => match resp.json::<Vec<ApiTrade>>().await {
                Ok(t) => t,
                Err(e) => {
                    debug!("Failed to parse trades: {:?}", e);
                    continue;
                }
            },
            Ok(resp) => {
                debug!("Trades API returned {}", resp.status());
                continue;
            }
            Err(e) => {
                debug!("Trades fetch failed: {:?}", e);
                continue;
            }
        };

        // Filter and process trades
        let mut fill_count = 0;
        let mut newer_count = 0;
        let mut matched_count = 0;

        // Log diagnostic info every poll
        let newest_ts = trades.iter().map(|t| t.timestamp).max().unwrap_or(0);
        if !trades.is_empty() {
            let oldest_ts = trades.iter().map(|t| t.timestamp).min().unwrap_or(0);
            info!(
                "[POLL] Fetched {} trades, timestamps: {} to {}, last_seen: {}, tracked tokens: {}",
                trades.len(),
                oldest_ts,
                newest_ts,
                last_seen_timestamp,
                tracked_tokens.len()
            );
        }

        for trade in &trades {
            // Only process trades newer than what we've already seen
            if trade.timestamp <= last_seen_timestamp {
                continue;
            }
            newer_count += 1;

            // Only process trades for our tracked assets
            if !tracked_tokens.contains(&trade.asset) {
                continue;
            }
            matched_count += 1;

            info!(
                "[POLL] Matched trade: {} {} @ {} on {}",
                trade.side,
                trade.size,
                trade.price,
                &trade.asset[..30]
            );

            // Convert trade data
            let trade_side = if trade.side.to_uppercase() == "BUY" {
                Side::Buy
            } else {
                Side::Sell
            };

            let trade_size = match Decimal::from_f64(trade.size) {
                Some(d) => d,
                None => continue,
            };

            let trade_price = match Decimal::from_f64(trade.price) {
                Some(d) => d,
                None => continue,
            };

            // Inline paper fill simulation (avoiding non-exhaustive TradeMessage)
            {
                let mut locked = state.write().await;

                // Track last trade price for unrealized P&L
                locked
                    .last_trade_prices
                    .insert(trade.asset.clone(), trade_price);

                let mut trade_remaining = trade_size;

                // Find matching paper orders with their data
                let matching_orders: Vec<(String, String, Side, Decimal, Decimal, Decimal)> =
                    locked
                        .paper_orders
                        .iter()
                        .filter(|(_, order)| {
                            order.token_id == trade.asset && order.filled < order.size && {
                                match (&order.side, &trade_side) {
                                    (Side::Buy, Side::Sell) => trade_price <= order.price,
                                    (Side::Sell, Side::Buy) => trade_price >= order.price,
                                    _ => false,
                                }
                            }
                        })
                        .map(|(id, order)| {
                            (
                                id.clone(),
                                order.token_id.clone(),
                                order.side.clone(),
                                order.price,
                                order.size,
                                order.filled,
                            )
                        })
                        .collect();

                for (order_id, token_id, order_side, order_price, order_size, order_filled) in
                    matching_orders
                {
                    if trade_remaining <= Decimal::ZERO {
                        break;
                    }

                    let unfilled = order_size - order_filled;
                    let fill_size = unfilled.min(trade_remaining);

                    if fill_size <= Decimal::ZERO {
                        continue;
                    }

                    // Calculate slippage
                    let fill_ratio = fill_size / (trade_size + Decimal::ONE);
                    let slippage = MAX_SLIPPAGE * fill_ratio;

                    let fill_price = match order_side {
                        Side::Buy => order_price * (Decimal::ONE + slippage),
                        Side::Sell => order_price * (Decimal::ONE - slippage),
                        _ => order_price,
                    };

                    // Calculate value and rebate
                    let fill_value = fill_price * fill_size;
                    let rebate = fill_value * config.rebate_rate;

                    // Update order filled amount
                    if let Some(order) = locked.paper_orders.get_mut(&order_id) {
                        order.filled = order.filled + fill_size;
                    }
                    trade_remaining = trade_remaining - fill_size;
                    let new_filled = order_filled + fill_size;

                    // Update balance, exposure, and track P&L
                    match order_side {
                        Side::Buy => {
                            // Deduct cash for purchase
                            locked.paper_balance = locked.paper_balance - fill_value + rebate;

                            // Track inventory - we now own these tokens
                            let current_exp = locked
                                .exposure
                                .entry(token_id.clone())
                                .or_insert(Decimal::ZERO);
                            let old_exp = *current_exp;
                            *current_exp = *current_exp + fill_size;

                            // Update entry price using weighted average with actual position
                            let entry = locked
                                .paper_entry_prices
                                .entry(token_id.clone())
                                .or_insert(Decimal::ZERO);
                            if old_exp + fill_size > Decimal::ZERO {
                                *entry = (*entry * old_exp + fill_price * fill_size)
                                    / (old_exp + fill_size);
                            } else {
                                *entry = fill_price;
                            }
                        }
                        Side::Sell => {
                            // Check if we have inventory to sell
                            let current_exp = locked
                                .exposure
                                .get(&token_id)
                                .copied()
                                .unwrap_or(Decimal::ZERO);
                            if current_exp < fill_size {
                                // Cannot sell more than we own - skip this fill
                                warn!(
                                    "[PAPER] Skipping sell of {} - only have {} inventory",
                                    fill_size, current_exp
                                );
                                continue;
                            }

                            // Calculate P&L before updating
                            let entry_price = locked
                                .paper_entry_prices
                                .get(&token_id)
                                .copied()
                                .unwrap_or(fill_price);
                            let pnl = (fill_price - entry_price) * fill_size;
                            locked.paper_realized_pnl = locked.paper_realized_pnl + pnl;

                            // Credit cash for sale
                            locked.paper_balance = locked.paper_balance + fill_value + rebate;

                            // Reduce inventory
                            let exp = locked
                                .exposure
                                .entry(token_id.clone())
                                .or_insert(Decimal::ZERO);
                            *exp = *exp - fill_size;

                            // Clear entry price if position is closed
                            if *exp <= Decimal::ZERO {
                                locked.paper_entry_prices.remove(&token_id);
                            }
                        }
                        _ => {}
                    }

                    locked.paper_rebates_earned = locked.paper_rebates_earned + rebate;
                    locked.paper_fills += 1;

                    // Calculate fill P&L for display
                    let fill_pnl = match order_side {
                        Side::Sell => {
                            let entry = locked
                                .paper_entry_prices
                                .get(&token_id)
                                .copied()
                                .unwrap_or(fill_price);
                            (fill_price - entry) * fill_size
                        }
                        _ => Decimal::ZERO,
                    };

                    // Record fill for dashboard
                    let fill_record = FillRecord {
                        time: chrono::Utc::now().format("%H:%M:%S").to_string(),
                        side: if order_side == Side::Buy {
                            "BUY".to_string()
                        } else {
                            "SELL".to_string()
                        },
                        size: format!("{:.2}", fill_size),
                        price: format!("{:.4}", fill_price),
                        value: format!("${:.2}", fill_value),
                        rebate: format!("${:.4}", rebate),
                        pnl: if fill_pnl != Decimal::ZERO {
                            format!("${:.2}", fill_pnl)
                        } else {
                            "-".to_string()
                        },
                    };
                    locked.recent_fills.push(fill_record);

                    // Keep only last 100 fills
                    if locked.recent_fills.len() > 100 {
                        locked.recent_fills.remove(0);
                    }

                    info!(
                        "[PAPER-POLL] FILL: {:?} {} @ {} (slippage: {:.2}%) | Value: ${:.2} | Rebate: ${:.4}",
                        order_side,
                        fill_size,
                        fill_price.round_dp(4),
                        slippage * Decimal::from(100),
                        fill_value,
                        rebate
                    );
                }
            }
            fill_count += 1;
        }

        if newer_count > 0 || matched_count > 0 {
            info!(
                "[POLL] Summary: {} newer, {} matched our tokens, {} processed fills",
                newer_count, matched_count, fill_count
            );
        }

        // Update to the max timestamp we've seen so far
        if newest_ts > last_seen_timestamp {
            last_seen_timestamp = newest_ts;
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
    info!(
        "Dashboard server starting on http://0.0.0.0:{}",
        DASHBOARD_PORT
    );

    if let Err(e) = axum::serve(tokio::net::TcpListener::bind(addr).await.unwrap(), app).await {
        error!("Dashboard server error: {:?}", e);
    }
}

async fn api_handler(State(app_state): State<AppState>) -> Json<DashboardData> {
    let locked = app_state.bot_state.read().await;
    let starting = app_state.config.paper_starting_balance;
    let available = locked.paper_balance;
    let now_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Calculate arb-focused metrics from arb_positions
    let mut deployed_capital = Decimal::ZERO;
    let mut pending_profit = Decimal::ZERO;
    let mut open_arb_count = 0usize;

    let arb_positions: Vec<ArbPositionData> = locked
        .arb_positions
        .values()
        .filter(|arb| !arb.resolved)
        .map(|arb| {
            // Filled amount (min of YES/NO for hedged calculation)
            let filled = arb.yes_filled.min(arb.no_filled);
            // Cost = filled * (yes_price + no_price)
            let cost = filled * arb.cost_per_share;
            // Guaranteed profit = filled * (1 - cost_per_share)
            let profit = filled * (Decimal::ONE - arb.cost_per_share);
            // Spread = (1 - cost_per_share) * 100
            let spread = (Decimal::ONE - arb.cost_per_share) * Decimal::from(100);
            // Age
            let age = now_ts.saturating_sub(arb.created_at);

            deployed_capital += cost;
            pending_profit += profit;
            open_arb_count += 1;

            let status = if arb.yes_filled == arb.no_filled && arb.yes_filled >= arb.target_size {
                "Filled".to_string()
            } else {
                format!("Y:{:.0}/N:{:.0}", arb.yes_filled, arb.no_filled)
            };

            ArbPositionData {
                id_short: format!("{}...", &arb.id[..8.min(arb.id.len())]),
                size: format!("{:.2}", filled),
                cost_per_share: format!("{:.4}", arb.cost_per_share),
                guaranteed_profit: format!("{:.2}", profit),
                spread_pct: format!("{:.2}%", spread),
                age_secs: age,
                status,
            }
        })
        .collect();

    // Total profit = realized + pending
    let realized = locked.paper_realized_pnl;
    let total_profit = realized + pending_profit;

    // Exposure percentage
    let total_capital = available + deployed_capital;
    let exposure_pct = if total_capital > Decimal::ZERO {
        (deployed_capital / total_capital) * Decimal::from(100)
    } else {
        Decimal::ZERO
    };
    let max_exp =
        Decimal::from_f64(app_state.config.max_exposure_pct * 100.0).unwrap_or(Decimal::from(70));

    Json(DashboardData {
        paper_mode: app_state.config.paper_trade,
        uptime_secs: app_state.start_time.elapsed().as_secs(),
        starting_balance: format!("{:.2}", starting),
        available_balance: format!("{:.2}", available),
        deployed_capital: format!("{:.2}", deployed_capital),
        realized_profit: format!("{:.2}", realized),
        pending_profit: format!("{:.2}", pending_profit),
        total_profit: format!("{:.2}", total_profit),
        rebates_earned: format!("{:.4}", locked.paper_rebates_earned),
        fees_paid: format!("{:.4}", locked.paper_fees_paid),
        total_fills: locked.paper_fills,
        open_orders: locked.paper_orders.len(),
        open_arbs: open_arb_count,
        arb_positions,
        recent_fills: locked.recent_fills.iter().rev().take(20).cloned().collect(),
        timestamp: Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        market_count: locked.active_markets.len(),
        api_latency_ms: locked.api_latency_ms,
        ws_connected: locked.ws_connected,
        loop_count: locked.loop_count,
        orders_placed: locked.orders_placed,
        last_error: locked.last_error.clone(),
        exposure_pct: format!("{:.1}%", exposure_pct),
        max_exposure_pct: format!("{:.0}%", max_exp),
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
      <div class="pnl-main pnl-positive" id="total-profit">$0.00</div>
      <div class="pnl-label">Total Profit (Realized + Pending)</div>
      <div style="margin-top:20px; display:flex; justify-content:center; gap:40px;">
        <div><span style="font-size:28px; font-weight:700;" id="realized">$0.00</span><br><span class="pnl-label">Realized</span></div>
        <div><span style="font-size:28px; font-weight:700;" id="pending">$0.00</span><br><span class="pnl-label">Pending</span></div>
        <div><span style="font-size:28px; font-weight:700;" id="rebates">$0.00</span><br><span class="pnl-label">Rebates</span></div>
      </div>
    </div>

    <div class="stats-grid">
      <div class="stat-card">
        <div class="stat-label">Starting Balance</div>
        <div class="stat-value" id="starting">$1000.00</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Available</div>
        <div class="stat-value" id="available">$1000.00</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Deployed</div>
        <div class="stat-value" id="deployed">$0.00</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Exposure</div>
        <div class="stat-value" id="exposure">0% / 70%</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Open Arbs</div>
        <div class="stat-value" id="open-arbs">0</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Total Fills</div>
        <div class="stat-value" id="fills">0</div>
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
      <h2>Arb Positions (Hedged Pairs)</h2>
      <table>
        <thead><tr><th>ID</th><th>Size</th><th>Cost/Share</th><th>Profit</th><th>Spread</th><th>Age</th><th>Status</th></tr></thead>
        <tbody id="arb-table"></tbody>
      </table>
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

    function formatAge(secs) {
      if (secs < 60) return secs + 's';
      const m = Math.floor(secs / 60);
      const s = secs % 60;
      return m + 'm ' + s + 's';
    }

    async function load() {
      try {
        const resp = await fetch('/api/data');
        const d = await resp.json();

        document.getElementById('mode').textContent = d.paper_mode ? 'PAPER' : 'LIVE';
        document.getElementById('mode').className = 'badge ' + (d.paper_mode ? 'paper' : 'live');
        document.getElementById('uptime').textContent = formatUptime(d.uptime_secs);

        // Main P&L display
        const totalProfit = parseFloat(d.total_profit);
        const profitEl = document.getElementById('total-profit');
        profitEl.textContent = '$' + d.total_profit;
        profitEl.className = 'pnl-main ' + (totalProfit >= 0 ? 'pnl-positive' : 'pnl-negative');

        const realizedEl = document.getElementById('realized');
        realizedEl.textContent = '$' + d.realized_profit;
        realizedEl.style.color = parseFloat(d.realized_profit) >= 0 ? 'var(--accent)' : 'var(--danger)';

        const pendingEl = document.getElementById('pending');
        pendingEl.textContent = '$' + d.pending_profit;
        pendingEl.style.color = 'var(--accent)';

        document.getElementById('rebates').textContent = '$' + d.rebates_earned;

        // Stats
        document.getElementById('starting').textContent = '$' + d.starting_balance;
        document.getElementById('available').textContent = '$' + d.available_balance;
        document.getElementById('deployed').textContent = '$' + d.deployed_capital;
        document.getElementById('exposure').textContent = d.exposure_pct + ' / ' + d.max_exposure_pct;
        document.getElementById('open-arbs').textContent = d.open_arbs;
        document.getElementById('fills').textContent = d.total_fills;

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

        // Arb positions table
        const arbBody = document.getElementById('arb-table');
        if (!d.arb_positions || d.arb_positions.length === 0) {
          arbBody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:var(--muted);">No open arb positions</td></tr>';
        } else {
          arbBody.innerHTML = d.arb_positions.map(a => {
            return '<tr><td>' + a.id_short + '</td><td>$' + a.size + '</td><td>$' + a.cost_per_share + '</td><td class="pnl-positive">$' + a.guaranteed_profit + '</td><td>' + a.spread_pct + '</td><td>' + formatAge(a.age_secs) + '</td><td>' + a.status + '</td></tr>';
          }).join('');
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

// (FromStr already imported at top of file)
