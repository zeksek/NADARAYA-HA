"""
signal_bot.py — 60 Coin Sinyal Botu (İşlem Açmaz)
Pine NW (repaint=False, h=8, mult=3) + Heikin Ashi + ATR×2 Stop
Sabit 60 coin | Saat başı tarama | Her tarama sonrası özet mesaj
"""

import os, time, logging, requests
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from binance.um_futures import UMFutures

# ══════════════════════════════════════════════════
#  ⚙️  YAPILANDIRMA
# ══════════════════════════════════════════════════
TIMEFRAME   = "1h"
MAX_WORKERS = 10

# Sabit Top 60 Binance Futures USDT Perp
SYMBOLS = [
    "BTCUSDT",   "ETHUSDT",   "SOLUSDT",   "BNBUSDT",   "XRPUSDT",
    "DOGEUSDT",  "ADAUSDT",   "AVAXUSDT",  "TRXUSDT",   "LTCUSDT",
    "LINKUSDT",  "DOTUSDT",   "BCHUSDT",   "UNIUSDT",   "NEARUSDT",
    "ATOMUSDT",  "XLMUSDT",   "ETCUSDT",   "APTUSDT",   "ARBUSDT",
    "OPUSDT",    "INJUSDT",   "SUIUSDT",   "STXUSDT",   "RUNEUSDT",
    "TIAUSDT",   "FETUSDT",   "WLDUSDT",   "RENDERUSDT","SEIUSDT",
    "SANDUSDT",  "MANAUSDT",  "AXSUSDT",   "GALAUSDT",  "FLOWUSDT",
    "AAVEUSDT",  "MKRUSDT",   "CRVUSDT",   "SNXUSDT",   "COMPUSDT",
    "LDOUSDT",   "GMTUSDT",   "FILUSDT",   "EGLDUSDT",  "HBARUSDT",
    "QNTUSDT",   "XTZUSDT",   "EOSUSDT",   "ALGOUSDT",  "VETUSDT",
    "ZILUSDT",   "ICXUSDT",   "ANKRUSDT",  "BATUSDT",   "ZECUSDT",
    "DASHUSDT",  "KSMUSDT",   "ROSEUSDT",  "APEUSDT",   "SKLUSDT",
]

NW_H        = 8.0
NW_MULT     = 3.0
NW_LOOKBACK = 300
ATR_PERIOD  = 14
ATR_MULT    = 2.0
KLINES_LIMIT = 400

# Saat başı tarama — her saatin 1. dakikasında çalışır
# Aradaki dakikalarda sadece açık pozisyon stop kontrolü yapılır
SCAN_INTERVAL_MIN  = 60   # tam tarama aralığı (dakika)
STOP_CHECK_SEC     = 60   # stop kontrolü aralığı (saniye)

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
            logger.error(f"Telegram hata: {e}")

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
        f"⏰  {datetime.now(timezone.utc).strftime('%d %b %Y %H:%M')} UTC\n"
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
        f"⏰  {datetime.now(timezone.utc).strftime('%d %b %Y %H:%M')} UTC\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <i>Sinyal botu — işlem açmaz</i>"
    )
    tg_send(msg)

def tg_ozet(tarama_no, long_sinyaller, short_sinyaller, positions, sure_sn):
    """Her tarama sonrası özet mesaj."""
    poz_listesi = ""
    if positions:
        for sym, pos in positions.items():
            sure = int((datetime.now(timezone.utc) - pos["time"]).total_seconds() / 3600)
            poz_listesi += f"\n  • {pos['side']} {sym} ({sure}s önce)"
    else:
        poz_listesi = "\n  Açık pozisyon yok"

    msg = (
        f"📊 <b>Saatlik Tarama Tamamlandı #{tarama_no}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔍  Taranan coin  : <b>{len(SYMBOLS)}</b>\n"
        f"🟢  LONG sinyali  : <b>{long_sinyaller}</b>\n"
        f"🔴  SHORT sinyali : <b>{short_sinyaller}</b>\n"
        f"📂  Açık pozisyon : <b>{len(positions)}</b>{poz_listesi}\n"
        f"⚡  Süre          : <b>{sure_sn:.1f} sn</b>\n"
        f"⏰  {datetime.now(timezone.utc).strftime('%d %b %Y %H:%M')} UTC\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━"
    )
    tg_send(msg)

# ══════════════════════════════════════════════════
#  🌐  BİNANCE
# ══════════════════════════════════════════════════
client = UMFutures()

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
def calc_ha(df):
    ha = df.copy()
    ha["ha_close"] = (df["open"]+df["high"]+df["low"]+df["close"]) / 4
    ha_open = [(df["open"].iloc[0]+df["close"].iloc[0]) / 2]
    for i in range(1, len(df)):
        ha_open.append((ha_open[i-1]+ha["ha_close"].iloc[i-1]) / 2)
    ha["ha_open"]  = ha_open
    ha["ha_high"]  = ha[["high","ha_open","ha_close"]].max(axis=1)
    ha["ha_low"]   = ha[["low","ha_open","ha_close"]].min(axis=1)
    ha["ha_color"] = (ha["ha_close"]>ha["ha_open"]).map({True:"green",False:"red"})
    return ha

def calc_nw(close):
    n=len(close); LB=NW_LOOKBACK
    coefs=np.array([np.exp(-(i**2)/(NW_H**2*2)) for i in range(LB)])
    den=coefs.sum()
    nw_out=np.full(n,np.nan)
    for i in range(LB-1,n):
        seg=close[i-LB+1:i+1][::-1]
        nw_out[i]=np.dot(coefs,seg)/den
    abs_diff=np.abs(close-nw_out)
    nw_mae=np.full(n,np.nan)
    for i in range(LB-1,n):
        nw_mae[i]=np.mean(abs_diff[i-LB+1:i+1])
    return nw_out, nw_out+NW_MULT*nw_mae, nw_out-NW_MULT*nw_mae

def calc_atr(df):
    tr=pd.concat([
        (df["high"]-df["low"]),
        (df["high"]-df["close"].shift()).abs(),
        (df["low"] -df["close"].shift()).abs(),
    ],axis=1).max(axis=1)
    return tr.ewm(span=ATR_PERIOD,adjust=False).mean()

def compute(df):
    df=calc_ha(df)
    nw_mid,nw_upper,nw_lower=calc_nw(df["close"].values)
    df["nw_mid"]=nw_mid; df["nw_upper"]=nw_upper; df["nw_lower"]=nw_lower
    df["atr"]=calc_atr(df)
    return df

# ══════════════════════════════════════════════════
#  🎯  SİNYAL
# ══════════════════════════════════════════════════
def check_entry(df):
    curr=df.iloc[-2]; prev=df.iloc[-3]
    if pd.isna(curr["nw_upper"]) or pd.isna(prev["nw_upper"]): return None
    above=(curr["close"]>curr["nw_upper"]) or (prev["close"]>prev["nw_upper"])
    below=(curr["close"]<curr["nw_lower"]) or (prev["close"]<prev["nw_lower"])
    long_ha =(prev["ha_color"]=="red"   and curr["ha_color"]=="green")
    short_ha=(prev["ha_color"]=="green" and curr["ha_color"]=="red")
    if below and long_ha:
        atr=curr["atr"]; price=curr["close"]
        return {"signal":"LONG","price":price,"stop":price-ATR_MULT*atr,
                "atr":atr,"nw_upper":curr["nw_upper"],"nw_lower":curr["nw_lower"],
                "nw_mid":curr["nw_mid"]}
    if above and short_ha:
        atr=curr["atr"]; price=curr["close"]
        return {"signal":"SHORT","price":price,"stop":price+ATR_MULT*atr,
                "atr":atr,"nw_upper":curr["nw_upper"],"nw_lower":curr["nw_lower"],
                "nw_mid":curr["nw_mid"]}
    return None

def check_exit(df, pos):
    curr=df.iloc[-2]; prev=df.iloc[-3]
    side=pos["side"]; price=curr["close"]; nw_mid=curr["nw_mid"]
    if pd.isna(nw_mid): return None
    if side=="LONG"  and price<=pos["stop"]: return "STOP"
    if side=="SHORT" and price>=pos["stop"]: return "STOP"
    flip_down=(prev["ha_color"]=="green" and curr["ha_color"]=="red")
    flip_up  =(prev["ha_color"]=="red"   and curr["ha_color"]=="green")
    if side=="LONG"  and flip_down and price<nw_mid: return "HA_MID"
    if side=="SHORT" and flip_up   and price>nw_mid: return "HA_MID"
    return None

# ══════════════════════════════════════════════════
#  🔍  TEK SEMBOL TARA
# ══════════════════════════════════════════════════
def scan(symbol, positions):
    df=get_klines(symbol)
    if df is None or len(df) < NW_LOOKBACK+5:
        logger.debug(f"[{symbol}] Yetersiz veri")
        return {"action":"NONE","symbol":symbol}
    try:
        df=compute(df)
    except Exception as e:
        logger.debug(f"[{symbol}] Hesap hatası: {e}")
        return {"action":"NONE","symbol":symbol}

    curr=df.iloc[-2]; prev=df.iloc[-3]
    above=(curr["close"]>curr["nw_upper"]) or (prev["close"]>prev["nw_upper"])
    below=(curr["close"]<curr["nw_lower"]) or (prev["close"]<prev["nw_lower"])
    dist_u=(curr["close"]-curr["nw_upper"])/curr["nw_upper"]*100
    dist_l=(curr["close"]-curr["nw_lower"])/curr["nw_lower"]*100
    durum="BANT USTU" if above else ("BANT ALTI" if below else "icinde")
    logger.info(f"[{symbol}] {durum} | fiyat={curr['close']:.4f} NW_U={curr['nw_upper']:.4f} NW_L={curr['nw_lower']:.4f} HA={curr['ha_color']} prev={prev['ha_color']} u={dist_u:+.1f}% l={dist_l:+.1f}%")

    if symbol in positions:
        reason=check_exit(df,positions[symbol])
        if reason:
            return {"action":"EXIT","symbol":symbol,"reason":reason,
                    "price":curr["close"],"nw_mid":curr["nw_mid"]}
    else:
        sig=check_entry(df)
        if sig:
            return {"action":"ENTRY","symbol":symbol,**sig}

    return {"action":"NONE","symbol":symbol}

# ══════════════════════════════════════════════════
#  🚀  ANA DÖNGÜ
# ══════════════════════════════════════════════════
def run():
    logger.info("="*55)
    logger.info("  60 COİN SİNYAL BOTU BAŞLADI")
    logger.info(f"  {len(SYMBOLS)} sabit sembol | {TIMEFRAME} | Saat başı tarama")
    logger.info("="*55)

    tg_send(
        f"🤖 <b>60 Coin Sinyal Botu Başladı</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔢 Tarama     : {len(SYMBOLS)} sabit coin\n"
        f"⏱️ Timeframe  : <b>{TIMEFRAME}</b>\n"
        f"📐 NW         : h={NW_H}, mult={NW_MULT}\n"
        f"🛑 Stop       : ATR×{ATR_MULT}\n"
        f"🔄 Tarama     : Saat başı (mum kapanışında)\n"
        f"⏰ {datetime.now(timezone.utc).strftime('%d %b %Y %H:%M')} UTC\n"
        f"⚠️ <i>Sadece sinyal gönderir, işlem açmaz!</i>"
    )

    positions   = {}
    tarama_no   = 0
    son_tarama  = 0   # son tam tarama zamanı (epoch)

    while True:
        try:
            simdi = time.time()

            # ── Saat başı tam tarama ─────────────────────────
            # Her saatin ilk dakikasında çalış (xx:01)
            simdi_dt = datetime.now(timezone.utc)
            saat_basi = simdi_dt.minute == 1 and simdi_dt.second < 60

            # İlk çalışmada da hemen tara
            ilk_calisma = tarama_no == 0

            if ilk_calisma or saat_basi:
                # Aynı dakikada tekrar tarama yapma
                if simdi - son_tarama < 50:
                    time.sleep(STOP_CHECK_SEC)
                    continue

                tarama_no += 1
                son_tarama = simdi
                baslangic  = time.time()
                logger.info(f"━━━ Tarama #{tarama_no} başlıyor: {len(SYMBOLS)} coin ━━━")

                results = []
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                    futures = {ex.submit(scan, s, positions): s for s in SYMBOLS}
                    for f in as_completed(futures):
                        try:
                            r = f.result()
                            results.append(r)
                        except Exception as e:
                            logger.debug(f"Tarama hatası: {e}")

                long_sig = 0; short_sig = 0
                for r in results:
                    sym = r["symbol"]
                    if r["action"] == "ENTRY":
                        sig = r["signal"]
                        tg_entry(sig, sym, r["price"], r["stop"], r["atr"],
                                 r["nw_upper"], r["nw_lower"], r["nw_mid"])
                        positions[sym] = {"side": sig, "entry": r["price"],
                                          "stop": r["stop"],
                                          "time": datetime.now(timezone.utc)}
                        logger.info(f"SİNYAL: {sig} {sym} @ {r['price']:.4f} stop={r['stop']:.4f}")
                        if sig == "LONG": long_sig += 1
                        else: short_sig += 1
                    elif r["action"] == "EXIT":
                        pos = positions.pop(sym, {})
                        tg_exit(pos.get("side","?"), sym, pos.get("entry",0),
                                r["price"], r["nw_mid"], r["reason"])
                        logger.info(f"ÇIKIŞ: {pos.get('side','?')} {sym}")

                sure = time.time() - baslangic
                logger.info(f"━━━ Tarama #{tarama_no} tamamlandı | {sure:.1f}sn | Long:{long_sig} Short:{short_sig} ━━━")

                # Özet Telegram mesajı
                tg_ozet(tarama_no, long_sig, short_sig, positions, sure)

            # ── Dakikada bir stop kontrolü ────────────────────
            # Sadece açık pozisyon varsa
            elif positions:
                for sym in list(positions.keys()):
                    df = get_klines(sym)
                    if df is None: continue
                    try:
                        df = compute(df)
                        reason = check_exit(df, positions[sym])
                        if reason:
                            pos = positions.pop(sym)
                            tg_exit(pos["side"], sym, pos["entry"],
                                    df["close"].iloc[-2],
                                    df["nw_mid"].iloc[-2], reason)
                            logger.info(f"STOP KONTROL ÇIKIŞ: {pos['side']} {sym}")
                    except Exception as e:
                        logger.debug(f"Stop kontrol hatası [{sym}]: {e}")

        except Exception as e:
            logger.exception(f"Ana döngü hatası: {e}")
            tg_send(f"⚠️ <b>Bot Hatası</b>\n<code>{str(e)[:200]}</code>")

        time.sleep(STOP_CHECK_SEC)

if __name__ == "__main__":
    run()
    
