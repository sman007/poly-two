# Paper Trading Mode

Paper trading allows you to test the bot's strategy without risking real funds. The bot connects to real market data but simulates order placement and fills locally.

## Configuration

Add these to your `.env` file:

```env
# Enable paper trading mode
PAPER_TRADE=true

# Starting balance for simulation (default: $1000)
PAPER_STARTING_BALANCE=1000.0
```

## How It Works

### Real Market Data
- Connects to Polymarket WebSocket for live trade feeds
- Fetches real orderbook data for pricing decisions
- Calculates order prices and sizes using the same logic as live trading

### Simulated Orders
- Orders are logged but not sent to the exchange
- Each paper order is assigned a unique ID (e.g., `PAPER-000001`)
- Orders are tracked with price, size, side, and fill status

### Fill Simulation
When market trades occur, the bot checks if paper orders would be filled:
- **BUY orders** fill when someone SELLS at or below our price
- **SELL orders** fill when someone BUYS at or above our price
- Partial fills are supported based on available trade size

### Slippage Model
Slippage is calculated based on fill size relative to available liquidity:
- Maximum slippage: 0.2%
- Larger fills relative to trade size = more slippage
- Slippage works against us (higher buy price, lower sell price)

### Fees & Rebates
Polymarket uses a maker/taker fee model:
- **Maker rebate**: 0.5% (we earn this since our limit orders rest in the book)
- **Taker fee**: 0.5% (not applied in paper mode since we're always makers)

Rebates are added to proceeds on sells and offset costs on buys.

### P&L Tracking
- Entry prices are tracked using weighted average cost basis
- Realized P&L is calculated when positions are closed: `(sell_price - entry_price) × size`

## Output

### Per-Fill Logging
Each simulated fill logs:
```
[PAPER] FILL: BUY 10.50 @ 0.5200 (slippage: 0.05%) | Value: $5.46 | Rebate: $0.0273
[PAPER] Balance: $994.57 | Realized P&L: $0.00 | Total fills: 1
```

### Periodic Summary
Every 5 minutes, a detailed summary is displayed:
```
═══════════════════════════════════════════════════════════════
   PAPER TRADING SUMMARY
═══════════════════════════════════════════════════════════════
   Starting Balance:  $1000.00
   Current Balance:   $1023.45
   Total Return:      2.35%
───────────────────────────────────────────────────────────────
   Total Fills:       47
   Rebates Earned:    $1.2340
   Fees Paid:         $0.0000
   Realized P&L:      $22.21
───────────────────────────────────────────────────────────────
   Open Orders:       4
   Tracked Positions: 2
═══════════════════════════════════════════════════════════════
```

## Limitations

Paper trading is an approximation and differs from live trading:

1. **No queue position** - Real orders wait in queue; paper orders fill immediately when price crosses
2. **Simplified slippage** - Real slippage depends on orderbook depth and other participants
3. **No failed orders** - Real orders can fail due to insufficient balance, API errors, etc.
4. **Instant execution** - No network latency simulation

## Transitioning to Live Trading

When ready to trade with real funds:

1. Fund your Polygon wallet with USDC
2. Set `PAPER_TRADE=false` in `.env`
3. Start with small position sizes (`MAX_EXPOSURE_PCT=0.1`)
4. Monitor the first few trades closely
