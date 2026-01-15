# Polymarket Maker Rebate Farming Bot

Rust bot for providing liquidity in Polymarket 15-minute crypto markets to earn USDC maker rebates.

## Setup

1. Copy .env.example to .env and fill in your real values
2. Install Rust: https://www.rust-lang.org/tools/install
3. Build & run:
   cargo build --release
   cargo run --release

## Important
- NEVER commit .env to GitHub
- Use a test wallet with small funds only
- Monitor logs during first run
