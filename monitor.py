#!/usr/bin/env python3
"""
Polymarket Bot Monitor - Proactive Health Check & Alert System
Runs continuously and alerts on anomalies, auto-restarts on failures.
"""

import requests
import time
import subprocess
import sys
from datetime import datetime
from typing import Optional, Dict, Any

# Configuration
BOT_API = "http://95.179.138.245:8082/api/data"
CHECK_INTERVAL = 30  # seconds
SERVER = "root@95.179.138.245"
BOT_PATH = "/opt/poly-two"

# Alert thresholds
MAX_REASONABLE_RETURN_PCT = 50.0  # Alert if return > 50% (unrealistic)
MIN_REASONABLE_RETURN_PCT = -30.0  # Alert if return < -30% (big loss)
MAX_UPTIME_WITHOUT_FILLS = 300  # Alert if no fills after 5 minutes
MAX_CONSECUTIVE_ERRORS = 3  # Restart after 3 consecutive API failures


class BotMonitor:
    def __init__(self):
        self.consecutive_errors = 0
        self.last_fill_count = 0
        self.last_fill_time = time.time()
        self.alerts_sent = set()

    def log(self, level: str, msg: str):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] [{level}] {msg}")

    def fetch_stats(self) -> Optional[Dict[str, Any]]:
        try:
            resp = requests.get(BOT_API, timeout=10)
            resp.raise_for_status()
            self.consecutive_errors = 0
            return resp.json()
        except Exception as e:
            self.consecutive_errors += 1
            self.log("ERROR", f"Failed to fetch stats: {e}")
            return None

    def restart_bot(self):
        self.log("WARN", "Restarting bot...")
        try:
            # Kill existing bot
            subprocess.run(
                ["ssh", SERVER, "killall -9 polymarket-maker-bot 2>/dev/null"],
                timeout=30
            )
            time.sleep(2)

            # Start fresh
            subprocess.run(
                ["ssh", SERVER, f"cd {BOT_PATH} && RUST_LOG=info nohup ./target/release/polymarket-maker-bot > bot.log 2>&1 &"],
                timeout=30
            )
            self.log("INFO", "Bot restarted successfully")
            self.consecutive_errors = 0
            self.last_fill_count = 0
            self.last_fill_time = time.time()
            self.alerts_sent.clear()
            time.sleep(10)  # Give bot time to start
        except Exception as e:
            self.log("ERROR", f"Failed to restart bot: {e}")

    def alert(self, alert_id: str, msg: str):
        if alert_id not in self.alerts_sent:
            self.log("ALERT", f"🚨 {msg}")
            self.alerts_sent.add(alert_id)

    def check_health(self, stats: Dict[str, Any]) -> bool:
        """Check for anomalies. Returns True if healthy."""
        healthy = True

        # Check 1: Unrealistic returns
        return_pct = float(stats.get("total_return_pct", 0))
        if return_pct > MAX_REASONABLE_RETURN_PCT:
            self.alert("high_return",
                f"UNREALISTIC RETURNS: {return_pct:.2f}% - possible accounting bug!")
            healthy = False
        elif return_pct < MIN_REASONABLE_RETURN_PCT:
            self.alert("big_loss",
                f"BIG LOSS: {return_pct:.2f}% - check strategy!")
            healthy = False

        # Check 2: Balance vs P&L consistency
        starting = float(stats.get("starting_balance", 1000))
        current = float(stats.get("current_balance", 1000))
        realized_pnl = float(stats.get("realized_pnl", 0))
        rebates = float(stats.get("rebates_earned", 0))

        # Unrealized P&L from positions is the difference
        expected_if_no_positions = starting + realized_pnl + rebates
        position_value = current - expected_if_no_positions

        # If position value is hugely negative or positive relative to balance, flag it
        if abs(position_value) > starting * 0.5:  # More than 50% in positions
            self.alert("large_positions",
                f"Large position exposure: ${position_value:.2f}")

        # Check 3: No fills for too long
        fill_count = stats.get("total_fills", 0)
        uptime = stats.get("uptime_secs", 0)

        if fill_count > self.last_fill_count:
            self.last_fill_count = fill_count
            self.last_fill_time = time.time()
        elif uptime > MAX_UPTIME_WITHOUT_FILLS and fill_count == 0:
            self.alert("no_fills",
                f"No fills after {uptime}s uptime - check market connectivity")

        # Check 4: WebSocket disconnected
        if not stats.get("ws_connected", True):
            self.alert("ws_disconnected", "WebSocket disconnected!")
            healthy = False

        # Check 5: Recent error
        last_error = stats.get("last_error")
        if last_error:
            self.alert("last_error", f"Bot error: {last_error}")

        return healthy

    def run(self):
        self.log("INFO", "Starting bot monitor...")
        self.log("INFO", f"Checking {BOT_API} every {CHECK_INTERVAL}s")

        while True:
            try:
                stats = self.fetch_stats()

                if stats is None:
                    if self.consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                        self.log("ERROR", f"API failed {self.consecutive_errors} times - restarting bot")
                        self.restart_bot()
                else:
                    # Log current status
                    self.log("INFO",
                        f"Balance: ${stats.get('current_balance')} | "
                        f"Return: {stats.get('total_return_pct')}% | "
                        f"Fills: {stats.get('total_fills')} | "
                        f"Uptime: {stats.get('uptime_secs')}s"
                    )

                    # Check for anomalies
                    healthy = self.check_health(stats)

                    if not healthy:
                        self.log("WARN", "Health check failed - monitoring closely")

            except KeyboardInterrupt:
                self.log("INFO", "Monitor stopped by user")
                break
            except Exception as e:
                self.log("ERROR", f"Monitor error: {e}")

            time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    monitor = BotMonitor()
    monitor.run()
