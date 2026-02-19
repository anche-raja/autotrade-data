"""IBKR contract definitions for equities, ETFs, and breadth indices."""

from __future__ import annotations

from ib_async import Contract, Index, Stock


def equity_contract(symbol: str, exchange: str = "SMART", currency: str = "USD") -> Stock:
    """Return an IB Stock contract for the given symbol."""
    return Stock(symbol, exchange, currency)


def spy_contract() -> Stock:
    return equity_contract("SPY")


def qqq_contract() -> Stock:
    return equity_contract("QQQ")


# ---------------------------------------------------------------------------
# Breadth / Market-statistics candidates
# ---------------------------------------------------------------------------

# Each entry: (display_name, IB_contract)
# AD-NYSE = Advancing Issues, TICK-NYSE = tick index, TRIN-NYSE = Arms index
BREADTH_CANDIDATES: list[tuple[str, Contract]] = [
    ("AD-NYSE", Index("AD", "NYSE")),
    ("AD-NASD", Index("AD", "NASDAQ")),
    ("TICK-NYSE", Index("TICK", "NYSE")),
    ("TRIN-NYSE", Index("TRIN", "NYSE")),
    ("ADVN", Index("ADVN", "NYSE")),
    ("DECN", Index("DECN", "NYSE")),
]


def get_bar_contract(symbol: str) -> Stock:
    """Map a CLI symbol string to an IB contract."""
    return equity_contract(symbol.upper())


def breadth_contract_by_name(name: str) -> Contract | None:
    """Look up a breadth contract by its display name (case-insensitive)."""
    for display, contract in BREADTH_CANDIDATES:
        if display.upper() == name.upper():
            return contract
    return None
