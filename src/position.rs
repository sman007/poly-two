//! Position tracking and management
//!
//! Per-strategy wallet isolation: Each strategy has its own $10k wallet.
//! This provides clear attribution and prevents one strategy from consuming
//! all capital.

use crate::db::Database;
use crate::Opportunity;
use anyhow::Result;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// All active strategies that get their own wallet
pub const STRATEGY_WALLETS: &[&str] = &[
    "whale_copy",
    "pre_dispute",
    "pre_dispute_news", // News-confirmed pre_dispute gets same wallet
    "resolution_farming",
    "statistical_arb",
    "spread_farming",
    "momentum_lag",
    "high_conviction", // Added - was missing, causing $0 trades
    "range_mm",
    // "high_prob_scalp",  // Disabled
];

/// Per-strategy wallet tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyWallet {
    pub strategy: String,
    pub initial_capital: Decimal,
    pub cash: Decimal,
    pub realized_pnl: Decimal,
}

impl StrategyWallet {
    pub fn new(strategy: &str, initial_capital: Decimal) -> Self {
        Self {
            strategy: strategy.to_string(),
            initial_capital,
            cash: initial_capital,
            realized_pnl: Decimal::ZERO,
        }
    }

    /// Account value = cash + open positions value (calculated externally)
    #[allow(dead_code)]
    pub fn account_value(&self, positions_value: Decimal) -> Decimal {
        self.cash + positions_value
    }

    /// Total P&L = realized + unrealized
    #[allow(dead_code)]
    pub fn total_pnl(&self, unrealized: Decimal) -> Decimal {
        self.realized_pnl + unrealized
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub id: String,
    pub strategy: String,
    pub market_id: String,
    pub market_slug: String,
    pub side: String,
    pub entry_price: Decimal,
    pub current_price: Decimal,
    pub quantity: Decimal,
    pub size_usd: Decimal,
    pub entry_time: DateTime<Utc>,
    pub status: PositionStatus,
    pub pnl: Decimal,
    pub pnl_pct: f64,
    /// Whale copy delay: time from whale detection to position open (ms)
    /// Only set for whale_copy strategy positions
    pub copy_delay_ms: Option<u64>,
    /// Entry edge percentage - stored at position open for dashboard display
    /// This is the original edge when the opportunity was detected, NOT current P&L%
    pub edge_pct: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PositionStatus {
    Open,
    Closed,
    Partial,
}

pub struct PositionManager {
    /// Initial capital per strategy wallet
    capital_per_strategy: Decimal,
    /// Per-strategy wallets (strategy -> wallet)
    wallets: HashMap<String, StrategyWallet>,
    positions: HashMap<String, Position>,
    closed: Vec<Position>,
    db: Option<Database>,
}

impl PositionManager {
    /// Create wallets for all strategies with given capital each
    fn create_wallets(capital_per_strategy: Decimal) -> HashMap<String, StrategyWallet> {
        let mut wallets = HashMap::new();
        for &strategy in STRATEGY_WALLETS {
            // Map pre_dispute_news to pre_dispute wallet
            let wallet_name = if strategy == "pre_dispute_news" {
                continue; // pre_dispute_news uses pre_dispute wallet
            } else {
                strategy
            };
            wallets.insert(
                wallet_name.to_string(),
                StrategyWallet::new(wallet_name, capital_per_strategy),
            );
        }
        wallets
    }

    /// Get wallet name for a strategy (maps pre_dispute_news to pre_dispute)
    fn wallet_for_strategy(strategy: &str) -> &str {
        if strategy == "pre_dispute_news" {
            "pre_dispute"
        } else {
            strategy
        }
    }

    pub fn new(capital_per_strategy: Decimal) -> Self {
        Self {
            capital_per_strategy,
            wallets: Self::create_wallets(capital_per_strategy),
            positions: HashMap::new(),
            closed: Vec::new(),
            db: None,
        }
    }

    /// Create a new PositionManager with database persistence
    pub fn new_with_db(capital_per_strategy: Decimal, db_path: &str) -> Result<Self> {
        let db = Database::new(db_path)?;

        // Create default wallets - will be updated from DB if state exists
        let mut wallets = Self::create_wallets(capital_per_strategy);

        // Try to load wallet state from database
        // For now, use legacy single-wallet state as fallback
        let legacy_state = db.load_state()?;
        let mut legacy_cash_opt: Option<Decimal> = None;
        let mut legacy_realized_opt: Option<Decimal> = None;
        let loaded_state = legacy_state.is_some();
        if let Some((legacy_cash, legacy_realized)) = legacy_state {
            legacy_cash_opt = Some(legacy_cash);
            legacy_realized_opt = Some(legacy_realized);
            // If we have legacy state but no per-wallet state, distribute evenly
            // In production, would migrate to per-wallet DB schema
            let wallet_count = Decimal::from(wallets.len());
            let cash_per = legacy_cash / wallet_count;
            let realized_per = legacy_realized / wallet_count;
            for wallet in wallets.values_mut() {
                wallet.cash = cash_per;
                wallet.realized_pnl = realized_per;
            }
        }

        let mut manager = Self {
            capital_per_strategy,
            wallets,
            positions: HashMap::new(),
            closed: Vec::new(),
            db: Some(db),
        };

        // Load positions from database
        manager.load_from_db()?;

        // Recalculate wallet cash based on loaded positions ONLY when no legacy state exists.
        // bot_state.cash already excludes open positions, so subtracting again would double-deduct.
        if !loaded_state {
            for pos in manager.positions.values() {
                let wallet_name = Self::wallet_for_strategy(&pos.strategy);
                if let Some(wallet) = manager.wallets.get_mut(wallet_name) {
                    wallet.cash -= pos.size_usd;
                }
            }
        }

        // Repair legacy cash drift if it does not match expected totals.
        // This prevents repeated restarts from permanently shrinking cash balances.
        if loaded_state {
            if let (Some(legacy_cash), Some(legacy_realized)) =
                (legacy_cash_opt, legacy_realized_opt)
            {
                let wallet_count = Decimal::from(manager.wallets.len());
                if wallet_count > Decimal::ZERO {
                    let total_initial = manager.capital_per_strategy * wallet_count;
                    let open_value: Decimal = manager.positions.values().map(|p| p.size_usd).sum();
                    let expected_cash = total_initial + legacy_realized - open_value;
                    let drift = legacy_cash - expected_cash;
                    let drift_threshold =
                        total_initial * Decimal::from_f64_retain(0.05).unwrap_or(Decimal::ZERO);
                    if drift.abs() > drift_threshold {
                        let corrected = expected_cash / wallet_count;
                        for wallet in manager.wallets.values_mut() {
                            wallet.cash = corrected;
                        }
                        manager.persist_state();
                        tracing::warn!(
                            "Corrected legacy cash drift: stored=${:.2}, expected=${:.2}",
                            legacy_cash,
                            expected_cash
                        );
                    }
                }
            }
        }

        Ok(manager)
    }

    /// Load positions from database
    fn load_from_db(&mut self) -> Result<()> {
        if let Some(db) = &self.db {
            // Load open positions
            let open_positions = db.load_open_positions()?;
            for pos in open_positions {
                self.positions.insert(pos.id.clone(), pos);
            }

            // Load closed positions
            let closed_positions = db.load_closed_positions()?;
            self.closed = closed_positions;
        }
        Ok(())
    }

    /// Persist current state to database (legacy format - sum of all wallets)
    fn persist_state(&self) {
        if let Some(db) = &self.db {
            let total_cash: Decimal = self.wallets.values().map(|w| w.cash).sum();
            let total_realized: Decimal = self.wallets.values().map(|w| w.realized_pnl).sum();
            let _ = db.save_state(total_cash, total_realized).map_err(|e| {
                tracing::error!("Failed to persist bot state: {}", e);
            });
        }
    }

    /// Open a paper position (deducts from strategy's wallet)
    pub fn open_paper(&mut self, opp: Opportunity) -> Option<String> {
        // Validate entry_price to prevent division by zero
        if opp.entry_price <= Decimal::ZERO {
            tracing::error!(
                "Cannot open position: invalid entry_price {} for market {}",
                opp.entry_price,
                opp.market_id
            );
            return None;
        }

        // BUG FIX: Block zero-edge trades (entry at $1.00 or 99.9c+)
        // These have no profit potential and inflate fake win rates
        let entry_f64: f64 = opp.entry_price.try_into().unwrap_or(1.0);
        if entry_f64 >= 0.999 {
            tracing::warn!(
                "Blocking zero-edge trade: entry_price {} >= 0.999 for market {} ({})",
                opp.entry_price,
                opp.market_id,
                opp.strategy
            );
            return None;
        }

        // Also block if edge_pct is effectively zero or negative
        if opp.edge_pct <= 0.1 {
            tracing::warn!(
                "Blocking low-edge trade: edge_pct {:.3}% for market {} ({})",
                opp.edge_pct,
                opp.market_id,
                opp.strategy
            );
            return None;
        }

        // Validate strategy wallet exists and has sufficient cash BEFORE creating position
        let wallet_name = Self::wallet_for_strategy(&opp.strategy);
        let wallet_cash = self
            .wallets
            .get(wallet_name)
            .map(|w| w.cash)
            .unwrap_or(Decimal::ZERO);

        if wallet_cash < opp.size_usd {
            tracing::error!(
                "Cannot open position: insufficient cash in {} wallet ({} available, {} required)",
                wallet_name,
                wallet_cash,
                opp.size_usd
            );
            return None;
        }

        let id = uuid::Uuid::new_v4().to_string();

        // Apply entry slippage for paper trading realism
        // FIXED: 0.1% was unrealistically low - real markets have 0.5%+ slippage
        // BUY: pay 0.5% more (crossing the spread to hit the ask)
        // SELL: receive 0.5% less (crossing the spread to hit the bid)
        // Combined with exit slippage, this gives ~1% round-trip cost (realistic for Polymarket)
        let slipped_entry_price = if opp.side == "BUY" || opp.side == "YES" {
            opp.entry_price * Decimal::from_f64_retain(1.005).unwrap_or(Decimal::ONE)
        } else {
            opp.entry_price * Decimal::from_f64_retain(0.995).unwrap_or(Decimal::ONE)
        };

        // Calculate whale copy delay if applicable
        let copy_delay_ms = if opp.strategy == "whale_copy" && opp.detected_at_ms > 0 {
            let now_ms = Utc::now().timestamp_millis() as u64;
            Some(now_ms.saturating_sub(opp.detected_at_ms))
        } else {
            None
        };

        let position = Position {
            id: id.clone(),
            strategy: opp.strategy.clone(),
            market_id: opp.market_id.clone(),
            market_slug: opp.market_slug,
            side: opp.side,
            entry_price: slipped_entry_price,
            current_price: slipped_entry_price,
            quantity: opp.size_usd / slipped_entry_price,
            size_usd: opp.size_usd,
            entry_time: Utc::now(),
            status: PositionStatus::Open,
            pnl: Decimal::ZERO,
            pnl_pct: 0.0,
            copy_delay_ms,
            edge_pct: opp.edge_pct, // Store entry edge for dashboard display
        };

        // Deduct from strategy's wallet (validated above, safe to unwrap)
        if let Some(wallet) = self.wallets.get_mut(wallet_name) {
            wallet.cash -= opp.size_usd;
        }

        // Persist to database
        if let Some(db) = &self.db {
            let _ = db.save_position(&position).map_err(|e| {
                tracing::error!("Failed to persist position: {}", e);
            });
            let _ = db
                .save_trade(
                    &id,
                    "OPEN",
                    opp.entry_price,
                    &opp.strategy,
                    &opp.market_id,
                    None,
                    None,
                )
                .map_err(|e| {
                    tracing::error!("Failed to record trade: {}", e);
                });
        }

        self.positions.insert(id.clone(), position);
        self.persist_state();
        Some(id)
    }

    /// Update position with new price
    pub fn update_price(&mut self, market_id: &str, price: Decimal) {
        for pos in self.positions.values_mut() {
            if pos.market_id == market_id {
                pos.current_price = price;

                // Calculate P&L based on side
                let entry_f64: f64 = pos.entry_price.try_into().unwrap_or(1.0);
                let current_f64: f64 = price.try_into().unwrap_or(1.0);

                if pos.side == "BUY" || pos.side == "YES" {
                    pos.pnl = (price - pos.entry_price) * pos.quantity;
                    pos.pnl_pct = if entry_f64 > 0.0 {
                        (current_f64 - entry_f64) / entry_f64
                    } else {
                        0.0
                    };
                } else {
                    pos.pnl = (pos.entry_price - price) * pos.quantity;
                    pos.pnl_pct = if entry_f64 > 0.0 {
                        (entry_f64 - current_f64) / entry_f64
                    } else {
                        0.0
                    };
                }

                // Persist updated position
                if let Some(db) = &self.db {
                    let _ = db.save_position(pos).map_err(|e| {
                        tracing::error!("Failed to persist position update: {}", e);
                    });
                }
            }
        }
    }

    /// Close a position (credits back to strategy's wallet)
    pub fn close(&mut self, position_id: &str, exit_price: Decimal) -> Option<Position> {
        if let Some(mut pos) = self.positions.remove(position_id) {
            pos.current_price = exit_price;
            pos.status = PositionStatus::Closed;

            // Calculate final P&L
            if pos.side == "BUY" || pos.side == "YES" {
                pos.pnl = (exit_price - pos.entry_price) * pos.quantity;
            } else {
                pos.pnl = (pos.entry_price - exit_price) * pos.quantity;
            }

            let entry_f64: f64 = pos.entry_price.try_into().unwrap_or(1.0);
            let exit_f64: f64 = exit_price.try_into().unwrap_or(1.0);
            pos.pnl_pct = if entry_f64 > 0.0 {
                if pos.side == "BUY" || pos.side == "YES" {
                    (exit_f64 - entry_f64) / entry_f64
                } else {
                    (entry_f64 - exit_f64) / entry_f64
                }
            } else {
                0.0
            };

            // Return cash + P&L to strategy's wallet
            let wallet_name = Self::wallet_for_strategy(&pos.strategy);
            if let Some(wallet) = self.wallets.get_mut(wallet_name) {
                wallet.cash += pos.size_usd + pos.pnl;
                wallet.realized_pnl += pos.pnl;
            }

            // Persist to database
            if let Some(db) = &self.db {
                let _ = db.save_position(&pos).map_err(|e| {
                    tracing::error!("Failed to persist closed position: {}", e);
                });
                let _ = db
                    .save_trade(
                        &pos.id,
                        "CLOSE",
                        exit_price,
                        &pos.strategy,
                        &pos.market_id,
                        Some(pos.pnl),
                        Some(pos.pnl_pct),
                    )
                    .map_err(|e| {
                        tracing::error!("Failed to record close trade: {}", e);
                    });
            }

            self.closed.push(pos.clone());
            self.persist_state();

            Some(pos)
        } else {
            None
        }
    }

    /// Check if we should take profit or stop loss
    pub fn check_exits(&self, take_profit_pct: f64, stop_loss_pct: f64) -> Vec<String> {
        self.positions
            .iter()
            .filter(|(_, pos)| pos.pnl_pct >= take_profit_pct || pos.pnl_pct <= -stop_loss_pct)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Get total account value across all wallets (cash + positions nominal value)
    /// NOTE: Excludes unrealized P&L - only shows cash + position cost basis
    /// This provides more conservative/realistic paper trading metrics
    pub fn account_value(&self) -> Decimal {
        let total_cash: Decimal = self.wallets.values().map(|w| w.cash).sum();
        // FIXED: Use size_usd only (cost basis), NOT size_usd + pnl (which includes unrealized gains)
        // Previously inflated account value by counting unrealized P&L as real cash
        let positions_value: Decimal = self.positions.values().map(|p| p.size_usd).sum();
        total_cash + positions_value
    }

    /// Get total cash across all wallets (excluding unrealized position value)
    pub fn total_cash(&self) -> Decimal {
        self.wallets.values().map(|w| w.cash).sum()
    }

    /// Get account value for a specific strategy wallet
    pub fn account_value_for_strategy(&self, strategy: &str) -> Decimal {
        let wallet_name = Self::wallet_for_strategy(strategy);
        let wallet_cash = self
            .wallets
            .get(wallet_name)
            .map(|w| w.cash)
            .unwrap_or(Decimal::ZERO);
        let positions_value: Decimal = self
            .positions
            .values()
            .filter(|p| Self::wallet_for_strategy(&p.strategy) == wallet_name)
            .map(|p| p.size_usd + p.pnl)
            .sum();
        wallet_cash + positions_value
    }

    /// Get total REALIZED P&L across all wallets (only closed positions count)
    /// For paper trading accuracy, only count money actually gained/lost
    pub fn total_pnl(&self) -> Decimal {
        // Only realized P&L counts - unrealized gains are fake until closed
        self.wallets.values().map(|w| w.realized_pnl).sum()
    }

    /// Get total P&L INCLUDING unrealized (for display purposes)
    pub fn total_pnl_with_unrealized(&self) -> Decimal {
        let total_realized: Decimal = self.wallets.values().map(|w| w.realized_pnl).sum();
        let unrealized: Decimal = self.positions.values().map(|p| p.pnl).sum();
        total_realized + unrealized
    }

    /// Get P&L for a specific strategy wallet
    pub fn pnl_for_strategy(&self, strategy: &str) -> Decimal {
        let wallet_name = Self::wallet_for_strategy(strategy);
        let realized = self
            .wallets
            .get(wallet_name)
            .map(|w| w.realized_pnl)
            .unwrap_or(Decimal::ZERO);
        let unrealized: Decimal = self
            .positions
            .values()
            .filter(|p| Self::wallet_for_strategy(&p.strategy) == wallet_name)
            .map(|p| p.pnl)
            .sum();
        realized + unrealized
    }

    /// Get number of open positions
    pub fn open_count(&self) -> usize {
        self.positions.len()
    }

    /// Get number of closed trades
    pub fn closed_count(&self) -> usize {
        self.closed.len()
    }

    /// Get win rate
    #[allow(clippy::cast_precision_loss)]
    pub fn win_rate(&self) -> f64 {
        if self.closed.is_empty() {
            return 0.0;
        }
        let wins = self.closed.iter().filter(|p| p.pnl > Decimal::ZERO).count();
        wins as f64 / self.closed.len() as f64
    }

    /// Get total exposure across all wallets
    pub fn total_exposure(&self) -> Decimal {
        self.positions.values().map(|p| p.size_usd).sum()
    }

    /// Get exposure for a specific strategy wallet
    pub fn exposure_for_strategy(&self, strategy: &str) -> Decimal {
        let wallet_name = Self::wallet_for_strategy(strategy);
        self.positions
            .values()
            .filter(|p| Self::wallet_for_strategy(&p.strategy) == wallet_name)
            .map(|p| p.size_usd)
            .sum()
    }

    /// Get total open exposure (sum of size_usd) for a strategy (direct strategy name match)
    /// Use this for per-strategy exposure caps that should NOT include shared wallet strategies
    pub fn exposure_for_strategy_direct(&self, strategy: &str) -> Decimal {
        self.positions
            .values()
            .filter(|p| p.strategy == strategy && p.status == PositionStatus::Open)
            .map(|p| p.size_usd)
            .sum()
    }
    /// Check if we have a position in a market
    pub fn has_position(&self, market_id: &str) -> bool {
        self.positions.values().any(|p| p.market_id == market_id)
    }

    /// Get positions by strategy
    pub fn by_strategy(&self, strategy: &str) -> Vec<&Position> {
        self.positions
            .values()
            .filter(|p| p.strategy == strategy)
            .collect()
    }

    /// Get all open positions
    pub fn get_all_open(&self) -> Vec<&Position> {
        self.positions.values().collect()
    }

    /// Get all closed positions
    pub fn get_all_closed(&self) -> &Vec<Position> {
        &self.closed
    }

    /// Get available cash across all wallets (legacy, for compatibility)
    pub fn available_cash(&self) -> Decimal {
        self.wallets
            .values()
            .map(|w| {
                if w.cash > Decimal::ZERO {
                    w.cash
                } else {
                    Decimal::ZERO
                }
            })
            .sum()
    }

    /// Get available cash for a specific strategy wallet
    pub fn available_cash_for_strategy(&self, strategy: &str) -> Decimal {
        let wallet_name = Self::wallet_for_strategy(strategy);
        self.wallets
            .get(wallet_name)
            .map(|w| {
                if w.cash > Decimal::ZERO {
                    w.cash
                } else {
                    Decimal::ZERO
                }
            })
            .unwrap_or(Decimal::ZERO)
    }

    /// Check if we have enough cash to open a position (legacy, checks total)
    pub fn can_afford(&self, size_usd: Decimal) -> bool {
        self.available_cash() >= size_usd
    }

    /// Check if a specific strategy wallet can afford a position
    pub fn can_afford_for_strategy(&self, strategy: &str, size_usd: Decimal) -> bool {
        self.available_cash_for_strategy(strategy) >= size_usd
    }

    /// Get realized P&L across all wallets
    pub fn realized_pnl(&self) -> Decimal {
        self.wallets.values().map(|w| w.realized_pnl).sum()
    }

    /// Get unrealized P&L across all positions
    pub fn unrealized_pnl(&self) -> Decimal {
        self.positions.values().map(|p| p.pnl).sum()
    }

    /// Get unrealized P&L for a specific strategy wallet
    pub fn unrealized_pnl_for_strategy(&self, strategy: &str) -> Decimal {
        let wallet_name = Self::wallet_for_strategy(strategy);
        self.positions
            .values()
            .filter(|p| Self::wallet_for_strategy(&p.strategy) == wallet_name)
            .map(|p| p.pnl)
            .sum()
    }

    /// Get unrealized P&L percentage for a strategy relative to wallet capital
    pub fn unrealized_pnl_pct_for_strategy(&self, strategy: &str) -> f64 {
        let unrealized = self.unrealized_pnl_for_strategy(strategy);
        let capital = self.capital_per_strategy();
        if capital == Decimal::ZERO {
            return 0.0;
        }
        ((unrealized / capital * Decimal::from(100)).try_into()).unwrap_or(0.0)
    }

    /// Get total initial capital (sum of all wallets)
    pub fn initial_capital(&self) -> Decimal {
        self.wallets.values().map(|w| w.initial_capital).sum()
    }

    /// Get capital per strategy wallet
    pub fn capital_per_strategy(&self) -> Decimal {
        self.capital_per_strategy
    }

    /// Get P&L as percentage of total initial capital
    pub fn total_pnl_pct(&self) -> f64 {
        let pnl: f64 = self.total_pnl().try_into().unwrap_or(0.0);
        let initial: f64 = self.initial_capital().try_into().unwrap_or(1.0);
        if initial > 0.0 {
            (pnl / initial) * 100.0
        } else {
            0.0
        }
    }

    /// Get all wallet summaries for status display
    pub fn wallet_summaries(&self) -> Vec<(String, Decimal, Decimal, Decimal, usize)> {
        let mut summaries: Vec<(String, Decimal, Decimal, Decimal, usize)> = self
            .wallets
            .values()
            .map(|w| {
                let positions_value: Decimal = self
                    .positions
                    .values()
                    .filter(|p| Self::wallet_for_strategy(&p.strategy) == w.strategy)
                    .map(|p| p.size_usd + p.pnl)
                    .sum();
                let unrealized: Decimal = self
                    .positions
                    .values()
                    .filter(|p| Self::wallet_for_strategy(&p.strategy) == w.strategy)
                    .map(|p| p.pnl)
                    .sum();
                let position_count = self
                    .positions
                    .values()
                    .filter(|p| Self::wallet_for_strategy(&p.strategy) == w.strategy)
                    .count();
                let account_value = w.cash + positions_value;
                let total_pnl = w.realized_pnl + unrealized;
                (
                    w.strategy.clone(),
                    account_value,
                    total_pnl,
                    w.initial_capital,
                    position_count,
                )
            })
            .collect();
        summaries.sort_by(|a, b| a.0.cmp(&b.0));
        summaries
    }

    /// Get per-strategy performance summary
    pub fn strategy_summary(&self) -> std::collections::HashMap<String, (usize, usize, f64)> {
        let mut summary: std::collections::HashMap<String, (usize, usize, f64)> =
            std::collections::HashMap::new();

        for pos in &self.closed {
            let entry = summary.entry(pos.strategy.clone()).or_insert((0, 0, 0.0));
            entry.0 += 1; // total trades
            if pos.pnl > Decimal::ZERO {
                entry.1 += 1; // wins
            }
            let pnl_f64: f64 = pos.pnl.try_into().unwrap_or(0.0);
            entry.2 += pnl_f64; // total P&L
        }

        summary
    }
}
