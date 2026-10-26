"""Stock price simulator that publishes synthetic price updates to Kafka 'stock-prices' topic."""

import logging
import random
import signal
import sys
import time
from typing import List, Optional
from events.schemas import PriceUpdate
from events.state import simulator_state
from event_pipeline.kafka_producer import StockEventProducer

logger = logging.getLogger(__name__)

# Canonical deterministic sequence specified in design requirements
DETERMINISTIC_AAPL_PRICES = [
    185.00,
    183.50,
    181.00,
    180.40,
    179.80,  # Crossing point for threshold $180 (downward)
    179.20,  # Remains below threshold (should not re-trigger)
    180.50,  # Bounces back above $180
    179.50,  # Crosses downward again
]

# Flink CEP canonical sequence from Proposal Section 11 & 12:
# 185.00 -> 183.00 -> 181.00 -> 179.00 (3 consecutive drops within 10s -> triggers PRICE_DOWNTREND_DETECTED and crosses $180)
CEP_DOWNTREND_AAPL_PRICES = [
    185.00,
    183.00,  # Drop 1 (< 185.00)
    181.00,  # Drop 2 (< 183.00)
    179.00,  # Drop 3 (< 181.00) -> Triggers Flink CEP DOWN_3_WITHIN_10S + Crosses $180 threshold
    178.40,  # Drop 4 (Suppressed by skipPastLastEvent for CEP; suppressed by threshold hysteresis)
    181.50,  # Rise to $181.50 -> Re-arms threshold; resets strict contiguity candidate
    180.80,  # Drop 1
    179.80,  # Drop 2 -> Crosses $180 downward threshold again
    180.20,  # Flat/Rise
]


class PriceSimulator:
    """Simulates realistic or deterministic stock prices and publishes to Kafka 'stock-prices'."""

    def __init__(
        self,
        symbols: Optional[List[str]] = None,
        interval_seconds: float = 1.0,
        mode: str = "deterministic",
        producer: Optional[StockEventProducer] = None,
    ):
        self.symbols = symbols or ["AAPL"]
        self.interval = interval_seconds
        self.mode = mode
        self.producer = producer or StockEventProducer()
        self._running = False
        self._seq_indices = {s: 0 for s in self.symbols}
        self._current_prices = {
            "AAPL": 185.00,
            "MSFT": 420.00,
            "GOOGL": 175.00,
        }

    def start(self, iterations: Optional[int] = None):
        """Runs the simulation loop."""
        self._running = True
        simulator_state.set_running(True, mode=self.mode, interval=self.interval)
        logger.info(
            "Starting PriceSimulator [mode=%s, interval=%.2fs, symbols=%s]",
            self.mode,
            self.interval,
            self.symbols,
        )

        count = 0
        try:
            while self._running:
                for symbol in self.symbols:
                    if not self._running:
                        break

                    price = self._generate_next_price(symbol)
                    update = PriceUpdate(symbol=symbol, price=price)

                    success = self.producer.publish_price_update(update)
                    if success:
                        simulator_state.update_price(symbol, price)
                        logger.info("Published Price: %s = $%.2f", symbol, price)
                    else:
                        logger.warning("Failed to publish price update for %s", symbol)

                count += 1
                if iterations is not None and count >= iterations:
                    logger.info("Reached target iterations count (%d). Stopping simulator.", iterations)
                    break

                time.sleep(self.interval)

        except KeyboardInterrupt:
            logger.info("Received interrupt signal. Shutting down PriceSimulator.")
        finally:
            self.stop()

    def stop(self):
        """Stops the simulator cleanly."""
        self._running = False
        simulator_state.set_running(False, mode=self.mode, interval=self.interval)
        self.producer.flush()
        logger.info("PriceSimulator stopped cleanly.")

    def _generate_next_price(self, symbol: str) -> float:
        """Determines the next price based on current simulation mode."""
        if self.mode in ("cep_demo", "downtrend") and symbol == "AAPL":
            idx = self._seq_indices[symbol]
            price = CEP_DOWNTREND_AAPL_PRICES[idx % len(CEP_DOWNTREND_AAPL_PRICES)]
            self._seq_indices[symbol] = (idx + 1)
            self._current_prices[symbol] = price
            return price

        if self.mode == "deterministic" and symbol == "AAPL":
            idx = self._seq_indices[symbol]
            price = DETERMINISTIC_AAPL_PRICES[idx % len(DETERMINISTIC_AAPL_PRICES)]
            self._seq_indices[symbol] = (idx + 1)
            self._current_prices[symbol] = price
            return price

        # Random walk for other symbols or random mode
        prev = self._current_prices.get(symbol, 100.0)
        # Random step between -0.8% and +0.8%
        pct_change = random.uniform(-0.008, 0.008)
        new_price = round(max(1.0, prev * (1.0 + pct_change)), 2)
        self._current_prices[symbol] = new_price
        return new_price


def run_standalone_simulator(
    interval: float = 1.0,
    mode: str = "deterministic",
    symbols: Optional[List[str]] = None,
    iterations: Optional[int] = None,
):
    """Entry point for running simulator via CLI or management command."""
    simulator = PriceSimulator(
        symbols=symbols or ["AAPL"],
        interval_seconds=interval,
        mode=mode,
    )

    def sig_handler(sig, frame):
        logger.info("Caught signal %s, initiating graceful shutdown...", sig)
        simulator.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    simulator.start(iterations=iterations)
