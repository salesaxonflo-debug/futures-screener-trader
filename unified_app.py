import asyncio
import json
import logging
import sqlite3
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import requests
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("unified-screener-db")

DB_FILE = "paper_trading.db"
INITIAL_BALANCE = 100.0
LEVERAGE = 5.0
RISK_PER_TRADE_PCT = 0.02

# Dynamic runtime limits (persisted in SQLite)
STRATEGY_LIMITS = {
    "CraigPer1": 3,
    "SneakyPivot": 3
}

ACCOUNT = {
    "balance": INITIAL_BALANCE,
    "equity": INITIAL_BALANCE,
    "realized_pnl": 0.0,
    "unrealized_pnl": 0.0,
    "total_trades": 0,
    "wins": 0,
    "losses": 0,
    "positions": {},  # Key: f"{symbol}_{strategy}"
    "order_history": []
}

LATEST_PRICES = {}
CRAIG_SCREENER = []
SNEAKY_SCREENER = []
CONNECTED_CLIENTS = set()


# -------------------------------------------------------------
# DATABASE MANAGEMENT (SQLITE WAL MODE)
# -------------------------------------------------------------
def get_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    global ACCOUNT, STRATEGY_LIMITS
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL;")

        # 1. Account State
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS account_state
                       (
                           id
                           INTEGER
                           PRIMARY
                           KEY,
                           balance
                           REAL,
                           realized_pnl
                           REAL,
                           total_trades
                           INTEGER,
                           wins
                           INTEGER,
                           losses
                           INTEGER
                       );
                       """)

        # 2. Strategy Config Limits
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS strategy_config
                       (
                           strategy_name
                           TEXT
                           PRIMARY
                           KEY,
                           max_slots
                           INTEGER
                       );
                       """)

        # 3. Open Positions (keyed by symbol_strategy)
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS open_positions
                       (
                           pos_key
                           TEXT
                           PRIMARY
                           KEY,
                           symbol
                           TEXT,
                           strategy
                           TEXT,
                           direction
                           TEXT,
                           units
                           REAL,
                           entry_price
                           REAL,
                           current_price
                           REAL,
                           sl
                           REAL,
                           tp
                           REAL,
                           margin
                           REAL,
                           unrealized_pnl
                           REAL,
                           breakeven_active
                           INTEGER,
                           opened_at
                           TEXT,
                           reason
                           TEXT
                       );
                       """)

        # 4. Closed Order History
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS order_history
                       (
                           id
                           INTEGER
                           PRIMARY
                           KEY
                           AUTOINCREMENT,
                           symbol
                           TEXT,
                           strategy
                           TEXT,
                           direction
                           TEXT,
                           entry
                           REAL,
                           exit
                           REAL,
                           pnl
                           REAL,
                           return_pct
                           REAL,
                           margin
                           REAL,
                           reason
                           TEXT,
                           closed_at
                           TEXT
                       );
                       """)

        # Seed/Load Strategy Config
        cursor.execute("SELECT * FROM strategy_config")
        cfg_rows = cursor.fetchall()
        if cfg_rows:
            for r in cfg_rows:
                STRATEGY_LIMITS[r["strategy_name"]] = r["max_slots"]
        else:
            cursor.execute("INSERT INTO strategy_config VALUES ('CraigPer1', 3)")
            cursor.execute("INSERT INTO strategy_config VALUES ('SneakyPivot', 3)")
            conn.commit()

        # Seed/Load Account State
        cursor.execute("SELECT * FROM account_state WHERE id = 1")
        acc_row = cursor.fetchone()
        if acc_row:
            ACCOUNT["balance"] = acc_row["balance"]
            ACCOUNT["realized_pnl"] = acc_row["realized_pnl"]
            ACCOUNT["total_trades"] = acc_row["total_trades"]
            ACCOUNT["wins"] = acc_row["wins"]
            ACCOUNT["losses"] = acc_row["losses"]
            logger.info(
                f"Loaded existing account: Balance=${ACCOUNT['balance']:.2f}, Realized PnL=${ACCOUNT['realized_pnl']:.2f}")
        else:
            cursor.execute("""
                           INSERT INTO account_state (id, balance, realized_pnl, total_trades, wins, losses)
                           VALUES (1, ?, 0.0, 0, 0, 0)
                           """, (INITIAL_BALANCE,))
            conn.commit()

        # Load Open Positions
        cursor.execute("SELECT * FROM open_positions")
        for r in cursor.fetchall():
            ACCOUNT["positions"][r["pos_key"]] = {
                "key": r["pos_key"],
                "symbol": r["symbol"],
                "strategy": r["strategy"],
                "direction": r["direction"],
                "units": r["units"],
                "entry_price": r["entry_price"],
                "current_price": r["current_price"],
                "sl": r["sl"],
                "tp": r["tp"],
                "margin": r["margin"],
                "unrealized_pnl": r["unrealized_pnl"],
                "breakeven_active": bool(r["breakeven_active"]),
                "opened_at": r["opened_at"],
                "reason": r["reason"]
            }

        # Load Order History
        cursor.execute("SELECT * FROM order_history ORDER BY id DESC LIMIT 50")
        for h in cursor.fetchall():
            ACCOUNT["order_history"].append({
                "symbol": h["symbol"],
                "strategy": h["strategy"],
                "direction": h["direction"],
                "entry": h["entry"],
                "exit": h["exit"],
                "pnl": h["pnl"],
                "return_pct": h["return_pct"],
                "margin": h["margin"],
                "reason": h["reason"],
                "closed_at": h["closed_at"]
            })


def save_account_state_db():
    with get_db() as conn:
        conn.execute("""
                     UPDATE account_state
                     SET balance      = ?,
                         realized_pnl = ?,
                         total_trades = ?,
                         wins         = ?,
                         losses       = ?
                     WHERE id = 1
                     """, (
                         ACCOUNT["balance"],
                         ACCOUNT["realized_pnl"],
                         ACCOUNT["total_trades"],
                         ACCOUNT["wins"],
                         ACCOUNT["losses"]
                     ))
        conn.commit()


def db_save_strategy_limit(strategy_name: str, slots: int):
    with get_db() as conn:
        conn.execute("INSERT OR REPLACE INTO strategy_config VALUES (?, ?)", (strategy_name, slots))
        conn.commit()


def db_insert_position(pos: dict):
    with get_db() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO open_positions (
                pos_key, symbol, strategy, direction, units, entry_price, current_price,
                sl, tp, margin, unrealized_pnl, breakeven_active, opened_at, reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            pos["key"], pos["symbol"], pos["strategy"], pos["direction"], pos["units"],
            pos["entry_price"], pos["current_price"], pos["sl"], pos["tp"],
            pos["margin"], pos["unrealized_pnl"], 1 if pos["breakeven_active"] else 0,
            pos["opened_at"], pos["reason"]
        ))
        conn.commit()


def db_update_position(pos: dict):
    with get_db() as conn:
        conn.execute("""
                     UPDATE open_positions
                     SET current_price    = ?,
                         sl               = ?,
                         unrealized_pnl   = ?,
                         breakeven_active = ?
                     WHERE pos_key = ?
                     """, (
                         pos["current_price"], pos["sl"], pos["unrealized_pnl"],
                         1 if pos["breakeven_active"] else 0, pos["key"]
                     ))
        conn.commit()


def db_remove_position(pos_key: str):
    with get_db() as conn:
        conn.execute("DELETE FROM open_positions WHERE pos_key = ?", (pos_key,))
        conn.commit()


def db_insert_trade_history(hist: dict):
    with get_db() as conn:
        conn.execute("""
                     INSERT INTO order_history (symbol, strategy, direction, entry, exit, pnl, return_pct, margin,
                                                reason, closed_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                     """, (
                         hist["symbol"], hist["strategy"], hist["direction"], hist["entry"],
                         hist["exit"], hist["pnl"], hist["return_pct"], hist["margin"],
                         hist["reason"], hist["closed_at"]
                     ))
        conn.commit()


# -------------------------------------------------------------
# MARKET DATA FETCHER (MEXC CONTRACT - 0% CLOUD IP BLOCKS)
# -------------------------------------------------------------
MEXC_API = "https://contract.mexc.com/api/v1/contract"


def fetch_top_gainers():
    url = f"{MEXC_API}/ticker"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json"
    }
    try:
        resp = requests.get(url, headers=headers, timeout=6)
        if resp.status_code == 200:
            data = resp.json().get("data", [])
            valid = []
            for t in data:
                sym = t.get("symbol", "")
                if sym.endswith("_USDT"):
                    clean_sym = sym.replace("_", "")
                    try:
                        chg = float(t.get("riseFallRate", 0)) * 100
                        last_p = float(t.get("lastPrice", 0))
                        valid.append({
                            "raw_symbol": sym,
                            "symbol": clean_sym,
                            "lastPrice": last_p,
                            "openPrice": last_p / (1 + (chg / 100)) if chg != -100 else last_p,
                            "highPrice": float(t.get("high24Price", 0)),
                            "lowPrice": float(t.get("low24Price", 0)),
                            "priceChangePercent": chg
                        })
                    except Exception:
                        continue

            valid.sort(key=lambda x: x["priceChangePercent"], reverse=True)
            top_30 = valid[:30]
            if top_30:
                return top_30
        else:
            logger.warning(f"MEXC ticker API returned {resp.status_code}")
    except Exception as e:
        logger.error(f"Error fetching tickers from MEXC: {e}")
    return []


def fetch_klines(raw_symbol: str, interval: str = "15m", limit: int = 50):
    mexc_interval = "Min15" if interval == "15m" else "Day1"
    url = f"{MEXC_API}/kline/{raw_symbol}"
    params = {
        "interval": mexc_interval
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=5)
        if resp.status_code == 200:
            res_data = resp.json().get("data", {})
            times = res_data.get("time", [])
            opens = res_data.get("open", [])
            highs = res_data.get("high", [])
            lows = res_data.get("low", [])
            closes = res_data.get("close", [])
            vols = res_data.get("vol", [])

            if times and len(times) > 0:
                df = pd.DataFrame({
                    "time": times,
                    "open": opens,
                    "high": highs,
                    "low": lows,
                    "close": closes,
                    "volume": vols
                })
                for c in ["open", "high", "low", "close", "volume"]:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
                return df.dropna().tail(limit)
    except Exception as e:
        logger.warning(f"Failed klines for {raw_symbol}: {e}")
    return pd.DataFrame()


# -------------------------------------------------------------
# STRATEGY 1: CRAIGPER1
# -------------------------------------------------------------
def detect_fractal_swings(df: pd.DataFrame, n: int = 2):
    swings = []
    for i in range(n, len(df) - n):
        h_win = df["high"].iloc[i - n: i + n + 1]
        l_win = df["low"].iloc[i - n: i + n + 1]
        if df["high"].iloc[i] == h_win.max():
            swings.append({"index": i, "price": df["high"].iloc[i], "type": "high"})
        elif df["low"].iloc[i] == l_win.min():
            swings.append({"index": i, "price": df["low"].iloc[i], "type": "low"})
    return swings


def detect_fvgs(df: pd.DataFrame):
    fvgs = []
    for i in range(len(df) - 2):
        c1 = df.iloc[i]
        c3 = df.iloc[i + 2]
        if c1["high"] < c3["low"]:
            fvgs.append({"type": "bullish", "midpoint": (c1["high"] + c3["low"]) / 2.0})
        elif c1["low"] > c3["high"]:
            fvgs.append({"type": "bearish", "midpoint": (c1["low"] + c3["high"]) / 2.0})
    return fvgs


def evaluate_craigper1(df: pd.DataFrame, current_price: float):
    if df.empty or len(df) < 20:
        return None

    swings = detect_fractal_swings(df, n=2)
    fvgs = detect_fvgs(df)

    last_high = next((s["price"] for s in reversed(swings) if s["type"] == "high"), None)
    last_low = next((s["price"] for s in reversed(swings) if s["type"] == "low"), None)

    if last_high and current_price > last_high:
        bull_fvg = next((f for f in reversed(fvgs) if f["type"] == "bullish"), None)
        entry = bull_fvg["midpoint"] if bull_fvg else current_price
        sl = last_low if last_low else (entry * 0.985)
        dist = abs(entry - sl)
        if dist > 0 and entry > sl:
            return {
                "signal": "BUY",
                "entry": entry,
                "sl": sl,
                "tp": entry + (4.0 * dist),
                "strategy": "CraigPer1",
                "reason": f"Bullish CHOCH > {last_high:.4f} + FVG Midpoint"
            }

    elif last_low and current_price < last_low:
        bear_fvg = next((f for f in reversed(fvgs) if f["type"] == "bearish"), None)
        entry = bear_fvg["midpoint"] if bear_fvg else current_price
        sl = last_high if last_high else (entry * 1.015)
        dist = abs(sl - entry)
        if dist > 0 and sl > entry:
            return {
                "signal": "SELL",
                "entry": entry,
                "sl": sl,
                "tp": entry - (4.0 * dist),
                "strategy": "CraigPer1",
                "reason": f"Bearish CHOCH < {last_low:.4f} + FVG Midpoint"
            }

    return {
        "signal": "NEUTRAL",
        "entry": 0.0, "sl": 0.0, "tp": 0.0,
        "strategy": "CraigPer1",
        "reason": "Range / Inside Swings"
    }


# -------------------------------------------------------------
# STRATEGY 2: SNEAKY PIVOT
# -------------------------------------------------------------
def compute_magic_lines(df_daily: pd.DataFrame):
    if len(df_daily) < 5:
        return None

    prev_day = df_daily.iloc[-2]
    range_high = float(prev_day["high"])
    range_low = float(prev_day["low"])
    historical = df_daily.iloc[:-2]

    swing_high = None
    for _, row in historical.iloc[::-1].iterrows():
        if row["high"] > range_high:
            swing_high = float(row["high"])
            break

    swing_low = None
    for _, row in historical.iloc[::-1].iterrows():
        if row["low"] < range_low:
            swing_low = float(row["low"])
            break

    if swing_high is None:
        swing_high = range_high * 1.025
    if swing_low is None:
        swing_low = range_low * 0.975

    return {
        "range_high": range_high,
        "range_low": range_low,
        "swing_high": swing_high,
        "swing_low": swing_low
    }


def evaluate_sneaky_pivot(df_15m: pd.DataFrame, lines: dict, current_price: float):
    if df_15m.empty or len(df_15m) < 4 or not lines:
        return None

    r_high = lines["range_high"]
    r_low = lines["range_low"]
    s_high = lines["swing_high"]
    s_low = lines["swing_low"]

    c1 = df_15m.iloc[-3]
    c2 = df_15m.iloc[-2]

    c1_range = c1["high"] - c1["low"]
    if c1_range <= 0:
        return None
    c1_body_pct = abs(c1["close"] - c1["open"]) / c1_range

    tentative_bias = None
    tol = 0.0075

    if (c1["low"] <= r_low or abs(c1["low"] - r_low) <= r_low * tol) and c1_body_pct >= 0.50:
        tentative_bias = "bullish"
    elif (c1["high"] >= r_high or abs(c1["high"] - r_high) <= r_high * tol) and c1_body_pct >= 0.50:
        tentative_bias = "bearish"

    if tentative_bias == "bullish" and c2["close"] > c2["open"]:
        entry = float(c2["high"])
        sl = min(c1["low"], c2["low"]) * 0.998
        signal = "BUY" if current_price >= entry else "ARMED_BUY"
        return {
            "signal": signal,
            "entry": entry,
            "sl": sl,
            "tp": r_high,
            "stretch_tp": s_high,
            "strategy": "SneakyPivot",
            "reason": f"Buy Zone probe @ {r_low:.4f} -> Sneaky Green c2"
        }

    elif tentative_bias == "bearish" and c2["close"] < c2["open"]:
        entry = float(c2["low"])
        sl = max(c1["high"], c2["high"]) * 1.002
        signal = "SELL" if current_price <= entry else "ARMED_SELL"
        return {
            "signal": signal,
            "entry": entry,
            "sl": sl,
            "tp": r_low,
            "stretch_tp": s_low,
            "strategy": "SneakyPivot",
            "reason": f"Sell Zone probe @ {r_high:.4f} -> Sneaky Red c2"
        }

    return {
        "signal": "NEUTRAL",
        "entry": 0.0, "sl": 0.0, "tp": 0.0, "stretch_tp": 0.0,
        "strategy": "SneakyPivot",
        "reason": "Inside No-Trade Zone or unconfirmed c2"
    }


# -------------------------------------------------------------
# TRADE EXECUTION & PORTFOLIO ENGINE
# -------------------------------------------------------------
def open_position(symbol: str, signal: str, entry_price: float, sl: float, tp: float, strategy: str, reason: str):
    pos_key = f"{symbol}_{strategy}"
    if pos_key in ACCOUNT["positions"]:
        return

    max_slots = STRATEGY_LIMITS.get(strategy, 3)
    strat_count = sum(1 for p in ACCOUNT["positions"].values() if p["strategy"] == strategy)
    if strat_count >= max_slots:
        return

    stop_distance = abs(entry_price - sl)
    if stop_distance <= 0:
        return

    risk_amount = ACCOUNT["equity"] * RISK_PER_TRADE_PCT
    units = risk_amount / stop_distance
    notional = units * entry_price
    required_margin = notional / LEVERAGE

    if required_margin > ACCOUNT["balance"] or required_margin < 2.0:
        return

    ACCOUNT["balance"] -= required_margin
    pos_data = {
        "key": pos_key,
        "symbol": symbol,
        "strategy": strategy,
        "direction": signal,
        "units": units,
        "entry_price": entry_price,
        "current_price": entry_price,
        "sl": sl,
        "tp": tp,
        "margin": required_margin,
        "unrealized_pnl": 0.0,
        "breakeven_active": False,
        "opened_at": datetime.now().strftime("%H:%M:%S"),
        "reason": reason
    }
    ACCOUNT["positions"][pos_key] = pos_data

    db_insert_position(pos_data)
    save_account_state_db()
    logger.info(
        f"[{strategy.upper()} ORDER EXECUTED] {signal} {symbol} @ {entry_price:.4f} | Margin: ${required_margin:.2f}")


def close_position(pos_key: str, exit_price: float, exit_reason: str):
    pos = ACCOUNT["positions"].pop(pos_key, None)
    if not pos:
        return

    units = pos["units"]
    margin = pos["margin"]
    entry = pos["entry_price"]
    direction = pos["direction"]

    pnl = (exit_price - entry) * units if direction == "BUY" else (entry - exit_price) * units
    ACCOUNT["balance"] += max(0.0, margin + pnl)
    ACCOUNT["realized_pnl"] += pnl
    ACCOUNT["total_trades"] += 1

    if pnl > 0:
        ACCOUNT["wins"] += 1
    else:
        ACCOUNT["losses"] += 1

    trade_record = {
        "symbol": pos["symbol"],
        "strategy": pos["strategy"],
        "direction": direction,
        "entry": entry,
        "exit": exit_price,
        "pnl": round(pnl, 2),
        "return_pct": round((pnl / margin) * 100, 2),
        "margin": round(margin, 2),
        "reason": exit_reason,
        "closed_at": datetime.now().strftime("%H:%M:%S")
    }
    ACCOUNT["order_history"].insert(0, trade_record)

    db_remove_position(pos_key)
    db_insert_trade_history(trade_record)
    save_account_state_db()
    logger.info(
        f"[TRADE CLOSED] {pos['strategy']} {pos['symbol']} @ {exit_price:.4f} | PnL: ${pnl:.2f} ({exit_reason})")


def update_portfolio_state():
    total_unrealized = 0.0

    for key, pos in list(ACCOUNT["positions"].items()):
        sym = pos["symbol"]
        curr = LATEST_PRICES.get(sym, pos["current_price"])
        pos["current_price"] = curr
        entry = pos["entry_price"]
        units = pos["units"]

        if pos["direction"] == "BUY":
            pnl = (curr - entry) * units
            if curr >= pos["tp"]:
                close_position(key, pos["tp"], "Target (TP) Hit")
                continue
            elif curr <= pos["sl"]:
                close_position(key, pos["sl"], "Stop Loss Hit")
                continue
            if not pos["breakeven_active"] and curr >= (entry + (pos["tp"] - entry) * 0.5):
                pos["sl"] = entry
                pos["breakeven_active"] = True
                db_update_position(pos)

        else:  # SELL
            pnl = (entry - curr) * units
            if curr <= pos["tp"]:
                close_position(key, pos["tp"], "Target (TP) Hit")
                continue
            elif curr >= pos["sl"]:
                close_position(key, pos["sl"], "Stop Loss Hit")
                continue
            if not pos["breakeven_active"] and curr <= (entry - (entry - pos["tp"]) * 0.5):
                pos["sl"] = entry
                pos["breakeven_active"] = True
                db_update_position(pos)

        pos["unrealized_pnl"] = round(pnl, 2)
        total_unrealized += pnl

    ACCOUNT["unrealized_pnl"] = round(total_unrealized, 2)
    margin_in_use = sum(p["margin"] for p in ACCOUNT["positions"].values())
    ACCOUNT["equity"] = round(ACCOUNT["balance"] + margin_in_use + total_unrealized, 2)


async def broadcast_ws():
    if CONNECTED_CLIENTS:
        payload = json.dumps(get_dashboard_payload())
        for ws in list(CONNECTED_CLIENTS):
            try:
                await ws.send_text(payload)
            except Exception:
                CONNECTED_CLIENTS.remove(ws)


# -------------------------------------------------------------
# ENGINE BACKGROUND LOOP
# -------------------------------------------------------------
async def unified_engine_loop():
    global LATEST_PRICES, CRAIG_SCREENER, SNEAKY_SCREENER
    while True:
        try:
            gainers = fetch_top_gainers()
            c_rows = []
            s_rows = []

            for item in gainers:
                sym = item["symbol"]
                raw_sym = item["raw_symbol"]
                curr_price = float(item.get("lastPrice", 0))
                chg = float(item.get("priceChangePercent", 0))
                o = float(item.get("openPrice", 0))
                h = float(item.get("highPrice", 0))
                l = float(item.get("lowPrice", 0))
                LATEST_PRICES[sym] = curr_price

                df_15m = fetch_klines(raw_sym, interval="15m", limit=35)
                df_daily = fetch_klines(raw_sym, interval="1d", limit=35)

                # 1. CraigPer1
                c_setup = evaluate_craigper1(df_15m, curr_price)
                if c_setup:
                    c_rows.append({
                        "pair": sym, "open": o, "high": h, "low": l, "close": curr_price,
                        "change": chg, "signal": c_setup["signal"], "entry": c_setup["entry"],
                        "sl": c_setup["sl"], "tp": c_setup["tp"], "reason": c_setup["reason"]
                    })
                    if c_setup["signal"] in ["BUY", "SELL"]:
                        open_position(sym, c_setup["signal"], curr_price, c_setup["sl"], c_setup["tp"], "CraigPer1",
                                      c_setup["reason"])

                # 2. Sneaky Pivot
                lines = compute_magic_lines(df_daily)
                s_setup = evaluate_sneaky_pivot(df_15m, lines, curr_price) if lines else None
                if s_setup and lines:
                    s_rows.append({
                        "pair": sym, "close": curr_price, "change": chg,
                        "swing_high": lines["swing_high"], "range_high": lines["range_high"],
                        "range_low": lines["range_low"], "swing_low": lines["swing_low"],
                        "signal": s_setup["signal"], "entry": s_setup["entry"],
                        "sl": s_setup["sl"], "tp": s_setup["tp"], "stretch_tp": s_setup.get("stretch_tp", 0),
                        "reason": s_setup["reason"]
                    })

                    if s_setup["signal"] in ["BUY", "SELL"]:
                        open_position(sym, s_setup["signal"], curr_price, s_setup["sl"], s_setup["tp"], "SneakyPivot",
                                      s_setup["reason"])
                    elif s_setup["signal"] == "ARMED_BUY" and curr_price >= s_setup["entry"]:
                        open_position(sym, "BUY", curr_price, s_setup["sl"], s_setup["tp"], "SneakyPivot",
                                      s_setup["reason"])
                    elif s_setup["signal"] == "ARMED_SELL" and curr_price <= s_setup["entry"]:
                        open_position(sym, "SELL", curr_price, s_setup["sl"], s_setup["tp"], "SneakyPivot",
                                      s_setup["reason"])

                await asyncio.sleep(0.04)

            CRAIG_SCREENER = c_rows
            SNEAKY_SCREENER = s_rows
            update_portfolio_state()
            await broadcast_ws()

        except Exception as e:
            logger.error(f"Unified engine error: {e}")

        await asyncio.sleep(3)


def get_dashboard_payload():
    return {
        "account": {
            "balance": round(ACCOUNT["balance"], 2),
            "equity": round(ACCOUNT["equity"], 2),
            "realized_pnl": round(ACCOUNT["realized_pnl"], 2),
            "unrealized_pnl": round(ACCOUNT["unrealized_pnl"], 2),
            "total_trades": ACCOUNT["total_trades"],
            "win_rate": round((ACCOUNT["wins"] / ACCOUNT["total_trades"] * 100), 1) if ACCOUNT[
                                                                                           "total_trades"] > 0 else 0.0,
            "leverage": f"{int(LEVERAGE)}x",
            "limits": STRATEGY_LIMITS
        },
        "positions": list(ACCOUNT["positions"].values()),
        "history": ACCOUNT["order_history"][:15],
        "craigper1": CRAIG_SCREENER,
        "sneakypivot": SNEAKY_SCREENER
    }


# -------------------------------------------------------------
# REST & WEBSOCKET ENDPOINTS
# -------------------------------------------------------------
class LimitUpdateRequest(BaseModel):
    strategy: str
    max_slots: int


class ManualExitRequest(BaseModel):
    pos_key: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    task = asyncio.create_task(unified_engine_loop())
    yield
    task.cancel()


app = FastAPI(lifespan=lifespan)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    CONNECTED_CLIENTS.add(websocket)
    await websocket.send_text(json.dumps(get_dashboard_payload()))
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        CONNECTED_CLIENTS.remove(websocket)


@app.get("/api/state")
def get_state():
    return get_dashboard_payload()


@app.post("/api/update-limit")
async def update_limit(req: LimitUpdateRequest):
    if req.strategy not in STRATEGY_LIMITS:
        raise HTTPException(status_code=400, detail="Invalid strategy name")
    if req.max_slots < 0 or req.max_slots > 20:
        raise HTTPException(status_code=400, detail="Slots must be between 0 and 20")

    STRATEGY_LIMITS[req.strategy] = req.max_slots
    db_save_strategy_limit(req.strategy, req.max_slots)
    logger.info(f"Updated position limit for {req.strategy} to {req.max_slots}")
    await broadcast_ws()
    return {"status": "success", "limits": STRATEGY_LIMITS}


@app.post("/api/manual-exit")
async def manual_exit(req: ManualExitRequest):
    pos = ACCOUNT["positions"].get(req.pos_key)
    if not pos:
        raise HTTPException(status_code=404, detail="Position not found or already closed")

    curr = LATEST_PRICES.get(pos["symbol"], pos["current_price"])
    close_position(req.pos_key, curr, "Manual Exit from Dashboard")
    update_portfolio_state()
    await broadcast_ws()
    return {"status": "success", "closed_key": req.pos_key, "exit_price": curr}


@app.get("/", response_class=HTMLResponse)
def serve_dashboard():
    with open("unified_dashboard.html", "r", encoding="utf-8") as f:
        return f.read()