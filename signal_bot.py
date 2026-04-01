"""
signal_bot.py — 100 Coin Sinyal Botu (İşlem Açmaz)
═══════════════════════════════════════════════════════
Strateji:
  GİRİŞ  → Fiyat NW bandı dışında (curr veya prev bar)
            + HA renk dönüşü
  ÇIKIŞ  → HA renk değişti VE fiyat NW dinamik mid geçti
  STOP   → ATR × 2 (bilgi amaçlı)

100 coin: Binance Futures en yüksek hacimli USDT perp
Paralel tarama: ThreadPoolExecutor
═══════════════════════════════════════════════════════
"""

import os, time, logging, requests
import numpy as np
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from binance.um_futures import UMFutures

# ══════════════════════════════════════════════════
#  ⚙️  YAPILANDIRMA
# ══════════════════════════════════════════════════
TIMEFRAME   = "1h"
TOP_N       = 100        # En yüksek hacimli N coin
MAX_WORKERS = 10         # Paralel iş parçacığı

NW_H        = 8.0
NW_MULT     = 3.0
NW_LOOKBACK = 500
ATR_PERIOD  = 14
ATR_MULT    = 2.0
KLINES_LIMIT      = 600
LOOP_INTERVAL_SEC = 60

TELEGRAM_TOKEN     = os.environ.get("TELEGRAM_TOKEN",     "8349458683:AAEi-AFSYxn0Skds7r4VQIaogVl3Fugftyw")
TELEGRAM_ID_GUNLUK = os.environ.get("TELEGRAM_ID_GUNLUK", "1484256652")
TELEGRAM_ID_KANAL  = os.environ.get("TELEGRAM_ID_KANAL",  "-1003792245773")

# ══════════════════════════════════════════════════
#  📋  LOGGING
# ══════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("signal_bot.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("BOT")

# ══════════════════════════════════════════════════
#  📨  TELEGRAM
# ══════════════════════════════════════════════════
_TG_URL     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
_TG_TARGETS = [TELEGRAM_ID_GUNLUK, TELEGRAM_ID_KANAL]

def tg_send(text: str):
    for chat_id in _TG_TARGETS:
        try:
            r = requests.post(
                _TG_URL,
                json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
                timeout=10,
            )
            if r.status_code != 200:
                logger.warning(f"Telegram hata [{chat_id}]: {r.text[:100]}")
        except Exception as e:
            logger.error(f"Telegram bağlantı hatası: {e}")

def tg_entry(signal, symbol, price, stop, atr, nw_upper, nw_lower, nw_mid):
    emoji  = "🟢" if signal == "LONG" else "🔴"
    action = "LONG AL 📈" if signal == "LONG" else "SHORT SAT 📉"
    risk_pct = abs(price - stop) / price * 100
    bant_ref = f"NW Alt: <b>{nw_lower:.4f}</b>" if signal == "LONG" else f"NW Üst: <b>{nw_upper:.4f}</b>"
    msg = (
        f"{emoji} <b>{action} — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💵  Fiyat       : <b>{price:.4f}</b>\n"
        f"🛑  ATR×2 Stop  : <b>{stop:.4f}</b>  (%{risk_pct:.2f})\n"
        f"🎯  Hedef (Mid) : <b>{nw_mid:.4f}</b>\n"
        f"📐  {bant_ref}\n"
        f"📊  ATR         : <b>{atr:.4f}</b>\n"
        f"⏱️  Timeframe   : <b>{TIMEFRAME}</b>\n"
        f"⏰  {datetime.utcnow().strftime('%d %b %Y %H:%M')} UTC\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <i>Sinyal botu — işlem açmaz</i>"
    )
    tg_send(msg)

def tg_exit(side, symbol, entry, curr_price, nw_mid, reason):
    pnl_pct  = (curr_price - entry) / entry if side == "LONG" else (entry - curr_price) / entry
    emoji    = "✅" if pnl_pct >= 0 else "❌"
    reason_tr = {"HA_MID": "HA + NW Mid", "STOP": "ATR Stop"}
    action   = "LONG KAPAT" if side == "LONG" else "SHORT KAPAT"
    msg = (
        f"{emoji} <b>{action} — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📌  Sebep    : <b>{reason_tr.get(reason, reason)}</b>\n"
        f"💵  Giriş   : <b>{entry:.4f}</b>\n"
        f"💵  Şimdi   : <b>{curr_price:.4f}</b>\n"
        f"🎯  NW Mid  : <b>{nw_mid:.4f}</b>\n"
        f"💰  Tahmini : <b>{pnl_pct*100:+.2f}%</b>\n"
        f"⏰  {datetime.utcnow().strftime('%d %b %Y %H:%M')} UTC\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <i>Sinyal botu — işlem açmaz</i>"
    )
    tg_send(msg)

# ══════════════════════════════════════════════════
#  🌐  BİNANCE
# ══════════════════════════════════════════════════
client = UMFutures()

def get_top_symbols(n=TOP_N) -> list:
    """24h hacme göre en yüksek N USDT perp sembolü."""
    try:
        tickers = client.ticker_24hr_price_change()
        usdt = [
            t for t in tickers
            if t["symbol"].endswith("USDT") and float(t.get("quoteVolume", 0)) > 0
        ]
        usdt.sort(key=lambda x: float(x["quoteVolume"]), reverse=True)
        symbols = [t["symbol"] for t in usdt[:n]]
        logger.info(f"Top {n} sembol alındı. İlk 5: {symbols[:5]}")
        return symbols
    except Exception as e:
        logger.error(f"Sembol listesi alınamadı: {e}")
        return []

def get_klines(symbol: str) -> pd.DataFrame | None:
    try:
        raw = client.klines(symbol=symbol, interval=TIMEFRAME, limit=KLINES_LIMIT)
        df  = pd.DataFrame(raw, columns=[
            "timestamp","open","high","low","close","volume",
            "close_time","quote_vol","trades",
            "taker_buy_base","taker_buy_quote","ignore",
        ])
        for col in ["open","high","low","close"]:
            df[col] = df[col].astype(float)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        return df.set_index("timestamp")
    except Exception as e:
        logger.debug(f"Kline hatası [{symbol}]: {e}")
        return None

# ══════════════════════════════════════════════════
#  📐  İNDİKATÖRLER
# ══════════════════════════════════════════════════
def calc_ha(df: pd.DataFrame) -> pd.DataFrame:
    ha = df.copy()
    ha["ha_close"] = (df["open"] + df["high"] + df["low"] + df["close"]) / 4
    ha_open = [(df["open"].iloc[0] + df["close"].iloc[0]) / 2]
    for i in range(1, len(df)):
        ha_open.append((ha_open[i-1] + ha["ha_close"].iloc[i-1]) / 2)
    ha["ha_open"]  = ha_open
    ha["ha_high"]  = ha[["high","ha_open","ha_close"]].max(axis=1)
    ha["ha_low"]   = ha[["low","ha_open","ha_close"]].min(axis=1)
    ha["ha_color"] = (ha["ha_close"] > ha["ha_open"]).map({True:"green",False:"red"})
    return ha

def calc_nw(close: np.ndarray):
    """Pine Script birebir — repaint=False"""
    n     = len(close)
    LB    = NW_LOOKBACK
    coefs = np.array([np.exp(-(i**2)/(NW_H**2*2)) for i in range(LB)])
    den   = coefs.sum()
    nw_out = np.full(n, np.nan)
    for i in range(LB-1, n):
        seg = close[i-LB+1:i+1][::-1]
        nw_out[i] = np.dot(coefs, seg) / den
    abs_diff = np.abs(close - nw_out)
    nw_mae   = np.full(n, np.nan)
    for i in range(LB-1, n):
        nw_mae[i] = np.mean(abs_diff[i-LB+1:i+1])
    return nw_out, nw_out + NW_MULT*nw_mae, nw_out - NW_MULT*nw_mae

def calc_atr(df: pd.DataFrame) -> pd.Series:
    tr = pd.concat([
        (df["high"]-df["low"]),
        (df["high"]-df["close"].shift()).abs(),
        (df["low"] -df["close"].shift()).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(span=ATR_PERIOD, adjust=False).mean()

def compute(df: pd.DataFrame) -> pd.DataFrame:
    df = calc_ha(df)
    nw_mid, nw_upper, nw_lower = calc_nw(df["close"].values)
    df["nw_mid"]   = nw_mid
    df["nw_upper"] = nw_upper
    df["nw_lower"] = nw_lower
    df["atr"]      = calc_atr(df)
    return df

# ══════════════════════════════════════════════════
#  🎯  SİNYAL
# ══════════════════════════════════════════════════
def check_entry(df: pd.DataFrame) -> dict | None:
    curr = df.iloc[-2]; prev = df.iloc[-3]
    if pd.isna(curr["nw_upper"]) or pd.isna(prev["nw_upper"]): return None

    above = (curr["close"] > curr["nw_upper"]) or (prev["close"] > prev["nw_upper"])
    below = (curr["close"] < curr["nw_lower"]) or (prev["close"] < prev["nw_lower"])
    long_ha  = (prev["ha_color"] == "red"   and curr["ha_color"] == "green")
    short_ha = (prev["ha_color"] == "green" and curr["ha_color"] == "red")

    if below and long_ha:
        atr = curr["atr"]; price = curr["close"]
        return {"signal":"LONG","price":price,"stop":price-ATR_MULT*atr,
                "atr":atr,"nw_upper":curr["nw_upper"],"nw_lower":curr["nw_lower"],
                "nw_mid":curr["nw_mid"]}
    if above and short_ha:
        atr = curr["atr"]; price = curr["close"]
        return {"signal":"SHORT","price":price,"stop":price+ATR_MULT*atr,
                "atr":atr,"nw_upper":curr["nw_upper"],"nw_lower":curr["nw_lower"],
                "nw_mid":curr["nw_mid"]}
    return None

def check_exit(df: pd.DataFrame, pos: dict) -> str | None:
    curr = df.iloc[-2]; prev = df.iloc[-3]
    side = pos["side"]; price = curr["close"]
    nw_mid = curr["nw_mid"]
    if pd.isna(nw_mid): return None

    if side == "LONG"  and price <= pos["stop"]: return "STOP"
    if side == "SHORT" and price >= pos["stop"]: return "STOP"

    flip_down = (prev["ha_color"] == "green" and curr["ha_color"] == "red")
    flip_up   = (prev["ha_color"] == "red"   and curr["ha_color"] == "green")

    if side == "LONG"  and flip_down and price < nw_mid: return "HA_MID"
    if side == "SHORT" and flip_up   and price > nw_mid: return "HA_MID"
    return None

# ══════════════════════════════════════════════════
#  🔍  TEK SEMBOL TARA
# ══════════════════════════════════════════════════
def scan(symbol: str, positions: dict) -> dict:
    """
    Sembolü tara.
    Döner: {"action": "ENTRY"/"EXIT"/"NONE", ...}
    """
    df = get_klines(symbol)
    if df is None or len(df) < NW_LOOKBACK + 5:
        return {"action": "NONE"}
    try:
        df = compute(df)
    except Exception as e:
        logger.debug(f"Hesap hatası [{symbol}]: {e}")
        return {"action": "NONE"}

    # Açık pozisyon var mı?
    if symbol in positions:
        reason = check_exit(df, positions[symbol])
        if reason:
            return {
                "action":  "EXIT",
                "symbol":  symbol,
                "reason":  reason,
                "price":   df["close"].iloc[-2],
                "nw_mid":  df["nw_mid"].iloc[-2],
            }
    else:
        sig = check_entry(df)
        if sig:
            return {"action": "ENTRY", "symbol": symbol, **sig}

    return {"action": "NONE"}

# ══════════════════════════════════════════════════
#  🚀  ANA DÖNGÜ
# ══════════════════════════════════════════════════
def run():
    logger.info("═" * 55)
    logger.info("  100 COİN SİNYAL BOTU BAŞLADI")
    logger.info(f"  Top {TOP_N} Binance Futures | {TIMEFRAME}")
    logger.info(f"  NW h={NW_H} mult={NW_MULT} | ATR×{ATR_MULT} Stop")
    logger.info("═" * 55)

    tg_send(
        f"🤖 <b>100 Coin Sinyal Botu Başladı</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔢 Tarama     : Top {TOP_N} Binance Futures\n"
        f"⏱️ Timeframe  : <b>{TIMEFRAME}</b>\n"
        f"📐 NW         : h={NW_H}, mult={NW_MULT}\n"
        f"🛑 Stop       : ATR×{ATR_MULT}\n"
        f"🔄 Kontrol    : Her {LOOP_INTERVAL_SEC} sn\n"
        f"⏰ {datetime.utcnow().strftime('%d %b %Y %H:%M')} UTC\n"
        f"⚠️ <i>Sadece sinyal gönderir, işlem açmaz!</i>"
    )

    positions = {}   # {symbol: {side, entry, stop, ...}}
    symbols   = []
    last_symbol_update = 0

    while True:
        try:
            # Her 1 saatte bir sembol listesini güncelle
            if time.time() - last_symbol_update > 3600:
                new_symbols = get_top_symbols(TOP_N)
                if new_symbols:
                    symbols = new_symbols
                    last_symbol_update = time.time()
                    logger.info(f"Sembol listesi güncellendi: {len(symbols)} coin")

            if not symbols:
                time.sleep(10)
                continue

            # Paralel tara
            results = []
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                futures = {ex.submit(scan, s, positions): s for s in symbols}
                for f in as_completed(futures):
                    try:
                        r = f.result()
                        if r["action"] != "NONE":
                            results.append(r)
                    except Exception as e:
                        logger.debug(f"Tarama hatası: {e}")

            # Sonuçları işle
            for r in results:
                sym = r["symbol"]

                if r["action"] == "ENTRY":
                    sig = r["signal"]
                    tg_entry(
                        signal    = sig,
                        symbol    = sym,
                        price     = r["price"],
                        stop      = r["stop"],
                        atr       = r["atr"],
                        nw_upper  = r["nw_upper"],
                        nw_lower  = r["nw_lower"],
                        nw_mid    = r["nw_mid"],
                    )
                    positions[sym] = {
                        "side":  sig,
                        "entry": r["price"],
                        "stop":  r["stop"],
                        "time":  datetime.utcnow(),
                    }
                    logger.info(f"SİNYAL: {sig} {sym} @ {r['price']:.4f} stop={r['stop']:.4f}")

                elif r["action"] == "EXIT":
                    pos = positions.pop(sym, {})
                    tg_exit(
                        side        = pos.get("side", "?"),
                        symbol      = sym,
                        entry       = pos.get("entry", 0),
                        curr_price  = r["price"],
                        nw_mid      = r["nw_mid"],
                        reason      = r["reason"],
                    )
                    logger.info(f"ÇIKIŞ: {pos.get('side','?')} {sym} sebep={r['reason']}")

            # Özet log
            logger.info(
                f"Tarama tamamlandı: {len(symbols)} coin | "
                f"Açık poz: {len(positions)} | "
                f"Sinyal: {sum(1 for r in results if r['action']=='ENTRY')}"
            )

        except Exception as e:
            logger.exception(f"Ana döngü hatası: {e}")
            tg_send(f"⚠️ <b>Bot Hatası</b>\n<code>{str(e)[:200]}</code>")

        time.sleep(LOOP_INTERVAL_SEC)


if __name__ == "__main__":
    run()
