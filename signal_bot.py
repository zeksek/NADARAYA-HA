"""
signal_bot.py — 60 Coin Sinyal Botu (İşlem Açmaz)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Mantık:
  Saat başı mum kapanınca:
    → NW hesapla (o anki değeri hafızaya al)
    → Fiyat NW altında + HA yeşil  = LONG sinyali
    → Fiyat NW üstünde + HA kırmızı = SHORT sinyali
    → Çıkış: HA renk değişti + fiyat NW mid geçti
    → Stop: ATR × 2

NW: Pine Script repaint=False birebir (h=8, mult=3)
"""

import os, time, logging, requests
import numpy as np
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
from binance.um_futures import UMFutures

TR_TZ = ZoneInfo("Europe/Istanbul")

# ══════════════════════════════════════════════════
#  ⚙️  YAPILANDIRMA
# ══════════════════════════════════════════════════
TIMEFRAME    = "1h"
MAX_WORKERS  = 10
NW_H         = 8.0
NW_MULT      = 3.0
NW_LOOKBACK  = 500
ATR_PERIOD   = 14
ATR_MULT     = 2.0
KLINES_LIMIT = 600

TELEGRAM_TOKEN     = os.environ.get("TELEGRAM_TOKEN",     "8349458683:AAEi-AFSYxn0Skds7r4VQIaogVl3Fugftyw")
TELEGRAM_ID_GUNLUK = os.environ.get("TELEGRAM_ID_GUNLUK", "1484256652")
TELEGRAM_ID_KANAL  = os.environ.get("TELEGRAM_ID_KANAL",  "-1003792245773")

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

# ══════════════════════════════════════════════════
#  📋  LOGGING (TR saati)
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

class TRFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=TR_TZ)
        return dt.strftime(datefmt or "%Y-%m-%d %H:%M:%S")

tr_fmt = TRFormatter("%(asctime)s (TR) [%(levelname)s] %(message)s")
for h in logging.root.handlers:
    h.setFormatter(tr_fmt)

logger = logging.getLogger("BOT")

# ══════════════════════════════════════════════════
#  📨  TELEGRAM
# ══════════════════════════════════════════════════
_TG_URL     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
_TG_TARGETS = [TELEGRAM_ID_GUNLUK, TELEGRAM_ID_KANAL]

def tg_send(text: str):
    for chat_id in _TG_TARGETS:
        try:
            r = requests.post(_TG_URL,
                json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
                timeout=10)
            if r.status_code != 200:
                logger.warning(f"Telegram [{chat_id}]: {r.text[:80]}")
        except Exception as e:
            logger.error(f"Telegram hata: {e}")

def tg_entry(signal, symbol, close, ha_close, stop, atr, nw_upper, nw_lower, nw_mid):
    emoji  = "🟢" if signal == "LONG" else "🔴"
    action = "LONG AL 📈" if signal == "LONG" else "SHORT SAT 📉"
    risk   = abs(close - stop) / close * 100
    bant   = f"NW Alt: <b>{nw_lower:.6f}</b>" if signal == "LONG" else f"NW Üst: <b>{nw_upper:.6f}</b>"
    now_tr = datetime.now(TR_TZ).strftime("%d %b %Y %H:%M")
    msg = (
        f"{emoji} <b>{action} — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💵  Kapanış (Gerçek) : <b>{close:.6f}</b>\n"
        f"💵  Kapanış (HA)     : <b>{ha_close:.6f}</b>\n"
        f"🛑  ATR×{ATR_MULT} Stop     : <b>{stop:.6f}</b>  (%{risk:.2f})\n"
        f"🎯  Hedef (NW Mid)  : <b>{nw_mid:.6f}</b>\n"
        f"📐  {bant}\n"
        f"📊  ATR             : <b>{atr:.6f}</b>\n"
        f"⏱️  Timeframe        : <b>{TIMEFRAME}</b>\n"
        f"⏰  {now_tr} (TR)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <i>Sinyal botu — işlem açmaz</i>"
    )
    tg_send(msg)

def tg_exit(side, symbol, entry, curr_price, nw_mid, reason):
    pnl    = (curr_price-entry)/entry if side=="LONG" else (entry-curr_price)/entry
    emoji  = "✅" if pnl >= 0 else "❌"
    action = "LONG KAPAT" if side == "LONG" else "SHORT KAPAT"
    reasons = {"HA_MID":"HA + NW Mid", "STOP":"ATR Stop"}
    now_tr = datetime.now(TR_TZ).strftime("%d %b %Y %H:%M")
    msg = (
        f"{emoji} <b>{action} — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📌  Sebep   : <b>{reasons.get(reason,reason)}</b>\n"
        f"💵  Giriş  : <b>{entry:.6f}</b>\n"
        f"💵  Şimdi  : <b>{curr_price:.6f}</b>\n"
        f"🎯  NW Mid : <b>{nw_mid:.6f}</b>\n"
        f"💰  Tahmini: <b>{pnl*100:+.2f}%</b>\n"
        f"⏰  {now_tr} (TR)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <i>Sinyal botu — işlem açmaz</i>"
    )
    tg_send(msg)

def tg_ozet(no, longs, shorts, positions, sure):
    poz = ""
    if positions:
        for sym, pos in positions.items():
            sure_h = int((datetime.now(TR_TZ)-pos["time"]).total_seconds()/3600)
            poz += f"\n  • {pos['side']} {sym} ({sure_h}s önce)"
    else:
        poz = "\n  Açık pozisyon yok"
    now_tr = datetime.now(TR_TZ).strftime("%d %b %Y %H:%M")
    tg_send(
        f"📊 <b>Saatlik Tarama #{no}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔍  Taranan : <b>{len(SYMBOLS)} coin</b>\n"
        f"🟢  LONG    : <b>{longs}</b>\n"
        f"🔴  SHORT   : <b>{shorts}</b>\n"
        f"📂  Açık poz: <b>{len(positions)}</b>{poz}\n"
        f"⚡  Süre    : <b>{sure:.1f} sn</b>\n"
        f"⏰  {now_tr} (TR)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━"
    )

# ══════════════════════════════════════════════════
#  🌐  BİNANCE
# ══════════════════════════════════════════════════
client = UMFutures()

def get_klines(symbol: str):
    try:
        raw = client.klines(symbol=symbol, interval=TIMEFRAME, limit=KLINES_LIMIT)
        df  = pd.DataFrame(raw, columns=[
            "timestamp","open","high","low","close","volume",
            "close_time","quote_vol","trades","tbb","tbq","ignore"])
        for col in ["open","high","low","close"]:
            df[col] = df[col].astype(float)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        return df.set_index("timestamp")
    except Exception as e:
        logger.debug(f"Kline [{symbol}]: {e}")
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
    ha["ha_color"] = (ha["ha_close"] > ha["ha_open"]).map({True:"green",False:"red"})
    return ha

def calc_nw(close):
    """
    Pine Script repaint=TRUE birebir — TradingView varsayılanı
    Sadece son LB bar için tam hesap yapılır (verimli).
    
    Pine kodu özü (repaint=TRUE):
      for i = 0 to 499:
        sum=0, sumw=0
        for j = 0 to 499:
          w = gauss(i-j, h)
          sum += src[j] * w; sumw += w
        y[i] = sum/sumw
      sae = mean(|src - y|) * mult
    """
    LB     = NW_LOOKBACK
    n      = len(close)
    j_arr  = np.arange(LB)

    if n < LB:
        nan = np.full(n, np.nan)
        return nan, nan, nan

    # Son LB bar (en yeni → en eski)
    prices = close[-LB:][::-1]

    # Her i için NW değeri hesapla (Pine repaint=TRUE)
    nwe = np.zeros(LB)
    for i in range(LB):
        w_arr  = np.exp(-((i - j_arr)**2) / (NW_H**2 * 2))
        nwe[i] = np.dot(w_arr, prices) / w_arr.sum()

    # SAE = mean(|src - nwe|) * mult  (Pine: sae / min(499,n-1) * mult)
    sae = np.mean(np.abs(prices - nwe)) * NW_MULT

    # Son bar değerleri
    last_mid   = nwe[0]
    last_upper = nwe[0] + sae
    last_lower = nwe[0] - sae

    # Tüm dizi için sadece son değeri doldur (sinyal için yeterli)
    nw_mid_arr   = np.full(n, np.nan); nw_mid_arr[-1]   = last_mid
    nw_upper_arr = np.full(n, np.nan); nw_upper_arr[-1] = last_upper
    nw_lower_arr = np.full(n, np.nan); nw_lower_arr[-1] = last_lower

    # Önceki bar için de hesapla (renk karşılaştırması + çıkış kontrolü)
    prices2 = close[-LB-1:-1][::-1]
    nwe2    = np.zeros(LB)
    for i in range(LB):
        w_arr   = np.exp(-((i - j_arr)**2) / (NW_H**2 * 2))
        nwe2[i] = np.dot(w_arr, prices2) / w_arr.sum()
    sae2 = np.mean(np.abs(prices2 - nwe2)) * NW_MULT
    nw_mid_arr[-2]   = nwe2[0]
    nw_upper_arr[-2] = nwe2[0] + sae2
    nw_lower_arr[-2] = nwe2[0] - sae2

    return nw_mid_arr, nw_upper_arr, nw_lower_arr

def calc_atr(df):
    tr = pd.concat([
        df["high"]-df["low"],
        (df["high"]-df["close"].shift()).abs(),
        (df["low"] -df["close"].shift()).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(span=ATR_PERIOD, adjust=False).mean()

def compute(df):
    df = calc_ha(df)
    nw_mid, nw_upper, nw_lower = calc_nw(df["close"].values)
    df["nw_mid"]   = nw_mid
    df["nw_upper"] = nw_upper
    df["nw_lower"] = nw_lower
    df["atr"]      = calc_atr(df)
    return df

# ══════════════════════════════════════════════════
#  🎯  SİNYAL — SON KAPANMIŞ MUM
# ══════════════════════════════════════════════════
def check_candle(df):
    """
    Son kapanmış muma bak (iloc[-1] — tarama xx:02'de yapılır,
    o zaman Binance'de son mum kapanmış olur)
    Önceki mum için iloc[-2] kullan
    """
    last = df.iloc[-1]   # son kapanmış mum
    prev = df.iloc[-2]   # bir önceki

    if pd.isna(last["nw_upper"]) or pd.isna(prev["nw_upper"]):
        return None, last

    close     = last["close"]
    ha_close  = last["ha_close"]
    ha_color  = last["ha_color"]
    prev_color= prev["ha_color"]
    nw_upper  = last["nw_upper"]
    nw_lower  = last["nw_lower"]
    nw_mid    = last["nw_mid"]
    atr       = last["atr"]

    # Bant kontrolü (curr veya prev dışarıda)
    above = (close > nw_upper) or (prev["close"] > prev["nw_upper"])
    below = (close < nw_lower) or (prev["close"] < prev["nw_lower"])

    # HA renk dönüşü
    long_ha  = (prev_color == "red"   and ha_color == "green")
    short_ha = (prev_color == "green" and ha_color == "red")

    dist_u = (close - nw_upper) / nw_upper * 100
    dist_l = (close - nw_lower) / nw_lower * 100
    durum  = "BANT USTU" if above else ("BANT ALTI" if below else "icinde")

    logger.info(
        f"[{symbol}] [{df.index[-1].tz_localize('UTC').astimezone(TR_TZ).strftime('%H:%M')} TR] "
        f"close={close:.6f} ha={ha_color} prev={prev_color} "
        f"NW_U={nw_upper:.6f} NW_L={nw_lower:.6f} "
        f"u={dist_u:+.1f}% l={dist_l:+.1f}% → {durum}"
    )

    if below and long_ha:
        return {"signal":"LONG","close":close,"ha_close":ha_close,
                "stop":close-ATR_MULT*atr,"atr":atr,
                "nw_upper":nw_upper,"nw_lower":nw_lower,"nw_mid":nw_mid}, last
    if above and short_ha:
        return {"signal":"SHORT","close":close,"ha_close":ha_close,
                "stop":close+ATR_MULT*atr,"atr":atr,
                "nw_upper":nw_upper,"nw_lower":nw_lower,"nw_mid":nw_mid}, last

    return None, last

def check_exit(last, prev, pos):
    side      = pos["side"]
    close     = last["close"]
    nw_mid    = last["nw_mid"]
    ha_color  = last["ha_color"]
    prev_color= prev["ha_color"]

    if pd.isna(nw_mid): return None

    if side == "LONG"  and close <= pos["stop"]: return "STOP"
    if side == "SHORT" and close >= pos["stop"]: return "STOP"

    flip_down = (prev_color=="green" and ha_color=="red")
    flip_up   = (prev_color=="red"   and ha_color=="green")

    if side == "LONG"  and flip_down and close < nw_mid: return "HA_MID"
    if side == "SHORT" and flip_up   and close > nw_mid: return "HA_MID"
    return None

# ══════════════════════════════════════════════════
#  🔍  TEK SEMBOL TARA
# ══════════════════════════════════════════════════
def scan(symbol, positions):
    df = get_klines(symbol)
    if df is None or len(df) < NW_LOOKBACK+5:
        return {"action":"NONE","symbol":symbol}
    try:
        df = compute(df)
    except Exception as e:
        logger.debug(f"[{symbol}] hesap hatası: {e}")
        return {"action":"NONE","symbol":symbol}

    last = df.iloc[-1]
    prev = df.iloc[-2]

    if symbol in positions:
        reason = check_exit(last, prev, positions[symbol])
        if reason:
            return {"action":"EXIT","symbol":symbol,"reason":reason,
                    "close":last["close"],"nw_mid":last["nw_mid"]}
    else:
        sig, _ = check_candle(df)
        if sig:
            return {"action":"ENTRY","symbol":symbol,**sig}

    return {"action":"NONE","symbol":symbol}

# ══════════════════════════════════════════════════
#  🚀  ANA DÖNGÜ
# ══════════════════════════════════════════════════
def run():
    logger.info("="*55)
    logger.info("  60 COİN SİNYAL BOTU BAŞLADI")
    logger.info(f"  {len(SYMBOLS)} coin | {TIMEFRAME} | Saat başı (xx:02)")
    logger.info(f"  NW h={NW_H} mult={NW_MULT} lookback={NW_LOOKBACK}")
    logger.info(f"  Son kapanmış mum (iloc[-1]) kullanılır")
    logger.info("="*55)

    tg_send(
        f"🤖 <b>60 Coin Sinyal Botu Başladı</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔢 Tarama    : {len(SYMBOLS)} sabit coin\n"
        f"⏱️ Timeframe : <b>{TIMEFRAME}</b>\n"
        f"📐 NW        : h={NW_H}, mult={NW_MULT}\n"
        f"🛑 Stop      : ATR×{ATR_MULT}\n"
        f"🔄 Tarama    : Saat başı xx:02 (TR)\n"
        f"⏰ {datetime.now(TR_TZ).strftime('%d %b %Y %H:%M')} (TR)\n"
        f"⚠️ <i>Sadece sinyal gönderir, işlem açmaz!</i>"
    )

    positions  = {}
    tarama_no  = 0
    son_tarama = 0

    while True:
        try:
            simdi_dt = datetime.now(TR_TZ)
            # xx:02'de tarama (mum kapandıktan 2 dk sonra)
            saat_basi   = simdi_dt.minute == 2 and simdi_dt.second < 58
            ilk_calisma = tarama_no == 0

            if ilk_calisma or saat_basi:
                if time.time() - son_tarama < 50:
                    time.sleep(30)
                    continue

                tarama_no += 1
                son_tarama = time.time()
                t0 = time.time()
                logger.info(f"━━━ Tarama #{tarama_no} başlıyor ━━━")

                results = []
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                    futures = {ex.submit(scan, s, positions): s for s in SYMBOLS}
                    for f in as_completed(futures):
                        try:
                            results.append(f.result())
                        except Exception as e:
                            logger.debug(f"hata: {e}")

                longs = 0; shorts = 0
                for r in results:
                    sym = r["symbol"]
                    if r["action"] == "ENTRY":
                        sig = r["signal"]
                        tg_entry(sig, sym, r["close"], r["ha_close"],
                                 r["stop"], r["atr"],
                                 r["nw_upper"], r["nw_lower"], r["nw_mid"])
                        positions[sym] = {"side":sig,"entry":r["close"],
                                          "stop":r["stop"],
                                          "time":datetime.now(TR_TZ)}
                        logger.info(f"SİNYAL: {sig} {sym} @ {r['close']:.6f}")
                        if sig=="LONG": longs+=1
                        else: shorts+=1
                    elif r["action"] == "EXIT":
                        pos = positions.pop(sym, {})
                        tg_exit(pos.get("side","?"), sym, pos.get("entry",0),
                                r["close"], r["nw_mid"], r["reason"])
                        logger.info(f"ÇIKIŞ: {pos.get('side','?')} {sym} → {r['reason']}")

                sure = time.time()-t0
                logger.info(f"━━━ Tarama #{tarama_no} bitti | {sure:.1f}sn | L:{longs} S:{shorts} ━━━")
                tg_ozet(tarama_no, longs, shorts, positions, sure)

            elif positions:
                # Açık pozisyon varsa dakikada bir stop kontrol
                for sym in list(positions.keys()):
                    df = get_klines(sym)
                    if df is None: continue
                    try:
                        df = compute(df)
                        reason = check_exit(df.iloc[-1], df.iloc[-2], positions[sym])
                        if reason:
                            pos = positions.pop(sym)
                            tg_exit(pos["side"], sym, pos["entry"],
                                    df["close"].iloc[-1],
                                    df["nw_mid"].iloc[-1], reason)
                    except Exception as e:
                        logger.debug(f"stop kontrol [{sym}]: {e}")

        except Exception as e:
            logger.exception(f"Ana döngü: {e}")
            tg_send(f"⚠️ <b>Bot Hatası</b>\n<code>{str(e)[:200]}</code>")

        time.sleep(30)

if __name__ == "__main__":
    run()
