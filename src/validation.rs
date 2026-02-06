//! R6: User channel for exposure tracking
//! R7: Multi-tx trade reconciliation  
//! R8: Order validation (min_order_size, tick_size)
//! R9: Fee rate for 15-min crypto
//! R10: Rate limiting

use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// R8: Market constraints
#[derive(Debug, Clone, Default)]
pub struct MarketConstraints {
    pub min_order_size: Decimal,
    pub tick_size: Decimal,
    pub fee_rate_bps: u32,
}

/// R8: Validate order
pub fn validate_order(size: Decimal, price: Decimal, c: &MarketConstraints) -> Result<(), String> {
    if size < c.min_order_size {
        return Err(format!("R8: size {} < min {}", size, c.min_order_size));
    }
    if c.tick_size > Decimal::ZERO && price % c.tick_size != Decimal::ZERO {
        return Err(format!("R8: price {} bad tick {}", price, c.tick_size));
    }
    Ok(())
}

/// R6: User channel events for exposure
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "event_type")]
pub enum UserChannelEvent {
    #[serde(rename = "trade")]
    Trade(UserTrade),
    #[serde(rename = "order")]
    OrderUpdate(OrderUpdate),
}

#[derive(Debug, Clone, Deserialize)]
pub struct UserTrade {
    pub id: String,
    pub market_order_id: Option<String>,
    pub match_time: i64,
    pub bucket_index: Option<u32>,
    pub status: String, // MATCHED, MINED, CONFIRMED, RETRYING, FAILED
    pub size: String,
    pub price: String,
    pub side: String,
    pub asset_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrderUpdate {
    pub id: String,
    pub status: String,
}

/// R7: Multi-tx trade aggregator
pub struct TradeAggregator {
    pending: HashMap<(String, i64, u32), Vec<UserTrade>>,
}

impl TradeAggregator {
    pub fn new() -> Self {
        Self { pending: HashMap::new() }
    }

    pub fn process(&mut self, trade: UserTrade) -> Option<AggregatedFill> {
        let key = (
            trade.market_order_id.clone().unwrap_or_default(),
            trade.match_time,
            trade.bucket_index.unwrap_or(0),
        );

        if trade.status == "CONFIRMED" || trade.status == "FAILED" {
            let mut trades = self.pending.remove(&key).unwrap_or_default();
            trades.push(trade.clone());
            
            let size: Decimal = trades.iter()
                .filter_map(|t| t.size.parse().ok())
                .sum();
            let total_value: Decimal = trades.iter()
                .filter_map(|t| {
                    let s: Decimal = t.size.parse().ok()?;
                    let p: Decimal = t.price.parse().ok()?;
                    Some(s * p)
                })
                .sum();
            let avg_price = if size > Decimal::ZERO { total_value / size } else { Decimal::ZERO };

            return Some(AggregatedFill {
                asset_id: trade.asset_id,
                side: trade.side,
                size,
                avg_price,
                confirmed: trade.status == "CONFIRMED",
            });
        }

        self.pending.entry(key).or_default().push(trade);
        None
    }
}

#[derive(Debug, Clone)]
pub struct AggregatedFill {
    pub asset_id: String,
    pub side: String,
    pub size: Decimal,
    pub avg_price: Decimal,
    pub confirmed: bool,
}

/// R10: Rate limiter
pub struct RateLimiter {
    last: Instant,
    interval: Duration,
    backoff: Duration,
    max_backoff: Duration,
}

impl RateLimiter {
    pub fn new(rps: u32) -> Self {
        let interval = Duration::from_secs_f64(1.0 / rps as f64);
        Self {
            last: Instant::now() - interval,
            interval,
            backoff: interval,
            max_backoff: Duration::from_secs(60),
        }
    }

    pub async fn wait(&mut self) {
        let elapsed = self.last.elapsed();
        if elapsed < self.backoff {
            tokio::time::sleep(self.backoff - elapsed).await;
        }
        self.last = Instant::now();
    }

    pub fn success(&mut self) { self.backoff = self.interval; }
    
    pub fn rate_limited(&mut self) {
        self.backoff = std::cmp::min(self.backoff * 2, self.max_backoff);
        warn!("R10: Backoff increased to {:?}", self.backoff);
    }
}

/// R9: Check if 15-min crypto market
pub fn needs_fee_rate(question: &str) -> bool {
    let q = question.to_lowercase();
    ["bitcoin", "btc", "eth", "ethereum", "crypto", "solana"]
        .iter().any(|k| q.contains(k))
}
