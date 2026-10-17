#!/usr/bin/env python3
"""Responsive Binance USDⓈ-M futures screener. Public market data only; no API key required."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
import json, time, re, threading, os

HOST, PORT = "0.0.0.0", int(os.environ.get("PORT", "8000"))
BASE = "https://fapi.binance.com"
CACHE = {}
CACHE_LOCK = threading.Lock()
SYMBOLS_CACHE = {"at": 0, "symbols": []}


def get_json(path, params=None, timeout=8):
    if params:
        from urllib.parse import urlencode
        path += "?" + urlencode(params)
    req = Request(BASE + path, headers={"User-Agent": "Mozilla/5.0 BinanceScreener/1.0"})
    with urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def fetch_symbols():
    now = time.time()
    if now - SYMBOLS_CACHE["at"] < 3600 and SYMBOLS_CACHE["symbols"]:
        return SYMBOLS_CACHE["symbols"]
    info = get_json("/fapi/v1/exchangeInfo")
    symbols = sorted(x["symbol"] for x in info.get("symbols", [])
                     if x.get("quoteAsset") == "USDT" and x.get("contractType") == "PERPETUAL" and x.get("status") == "TRADING")
    SYMBOLS_CACHE.update(at=now, symbols=symbols)
    return symbols


def ema(values, period=20):
    if not values: return None
    k = 2 / (period + 1)
    result = values[0]
    for x in values[1:]: result = x * k + result * (1 - k)
    return result


def candle_stats(candles):
    # Binance rows: open time, open, high, low, close, volume, ...
    if not candles or len(candles) < 3: return {}
    rows = [[float(x[i]) for i in (1,2,3,4,5)] for x in candles]
    closes = [x[3] for x in rows]
    highs, lows, vols = [x[1] for x in rows], [x[2] for x in rows], [x[4] for x in rows]
    tr = []
    for i in range(1, len(rows)):
        tr.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))
    atr = sum(tr[-14:]) / max(1, len(tr[-14:]))
    recent = sum(vols[-3:]) / 3
    baseline = sum(vols[-23:-3]) / max(1, len(vols[-23:-3]))
    vwap_num = sum(rows[i][3] * vols[i] for i in range(max(0, len(rows)-20), len(rows)))
    vwap_den = sum(vols[max(0, len(rows)-20):])
    return {"closes": closes, "last": closes[-1], "ema20": ema(closes[-20:], 20),
            "vwap": vwap_num / vwap_den if vwap_den else closes[-1], "atr": atr,
            "volume_mult": recent / baseline if baseline else 1,
            "change": (closes[-1] / closes[0] - 1) * 100,
            "series": closes[-72:]}


def scan_one(symbol):
    now = time.time()
    with CACHE_LOCK:
        cached = CACHE.get(symbol)
        if cached and now - cached[0] < 20: return cached[1]
    try:
        ticker, prem, oi, k5, k15, k1h, k4h = [None] * 7
        # Keep parallel subrequests per symbol, so watchlists refresh promptly.
        calls = {
            "ticker": ("/fapi/v1/ticker/24hr", {"symbol": symbol}),
            "premium": ("/fapi/v1/premiumIndex", {"symbol": symbol}),
            "oi": ("/fapi/v1/openInterest", {"symbol": symbol}),
            "k5": ("/fapi/v1/klines", {"symbol": symbol, "interval": "5m", "limit": 100}),
            "k15": ("/fapi/v1/klines", {"symbol": symbol, "interval": "15m", "limit": 100}),
            "k1h": ("/fapi/v1/klines", {"symbol": symbol, "interval": "1h", "limit": 100}),
            "k4h": ("/fapi/v1/klines", {"symbol": symbol, "interval": "4h", "limit": 100}),
            "oih": ("/futures/data/openInterestHist", {"symbol": symbol, "period": "5m", "limit": 13}),
        }
        result = {}
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = {pool.submit(get_json, path, params): key for key, (path, params) in calls.items()}
            for f in as_completed(futures):
                key = futures[f]
                try: result[key] = f.result()
                except Exception: result[key] = None
        ticker, prem, oi, k5, k15, k1h, k4h = (result.get(k) for k in ("ticker","premium","oi","k5","k15","k1h","k4h"))
        if not ticker or not k5 or not k1h: raise RuntimeError("Binance returned incomplete market data")
        s5, s15, s1h, s4h = (candle_stats(x) for x in (k5, k15 or k5, k1h, k4h or k1h))
        price = float(ticker["lastPrice"])
        # Current candle may still be forming; change percentages use recent completed-window values.
        changes = {}
        for key, period in (("5m", k5), ("15m", k15), ("1h", k1h), ("4h", k4h)):
            if period and len(period) > 1:
                changes[key] = (price / float(period[-2][4]) - 1) * 100
            else: changes[key] = 0
        oi_change = None
        hist = result.get("oih")
        if hist and len(hist) >= 2:
            old, new = float(hist[0].get("sumOpenInterest", 0)), float(hist[-1].get("sumOpenInterest", 0))
            if old: oi_change = (new / old - 1) * 100
        funding = float(prem.get("lastFundingRate", 0)) * 100 if prem else None
        ema1h = s1h.get("ema20") or price
        vwap = s15.get("vwap") or price
        atr = s1h.get("atr") or price * .01
        volume_mult = s5.get("volume_mult", 1)
        checks_long = [
            ("4h trend positive", changes["4h"] > 0),
            ("Price above 1h EMA20", price > ema1h),
            ("Price above 15m VWAP", price > vwap),
            ("5m volume above average", volume_mult >= 1.5),
            ("Open interest rising", oi_change is not None and oi_change > .5),
        ]
        checks_short = [
            ("4h trend negative", changes["4h"] < 0),
            ("Price below 1h EMA20", price < ema1h),
            ("Price below 15m VWAP", price < vwap),
            ("5m volume above average", volume_mult >= 1.5),
            ("Open interest rising", oi_change is not None and oi_change > .5),
        ]
        long_score = sum(bool(x[1]) for x in checks_long)
        short_score = sum(bool(x[1]) for x in checks_short)
        side = "LONG" if long_score >= 4 and long_score > short_score else "SHORT" if short_score >= 4 and short_score > long_score else "WAIT"
        if side == "LONG": stop, target = price - 1.5*atr, price + 2.5*atr
        elif side == "SHORT": stop, target = price + 1.5*atr, price - 2.5*atr
        else: stop, target = price - 1.5*atr, price + 2.5*atr
        data = {"symbol": symbol, "price": price, "change24": float(ticker.get("priceChangePercent",0)),
                "quoteVolume": float(ticker.get("quoteVolume",0)), "changes": changes,
                "funding": funding, "oi": float(oi.get("openInterest", 0)) if oi else None,
                "oiChange": oi_change, "ema1h": ema1h, "vwap15m": vwap, "atr": atr,
                "atrPct": atr / price * 100 if price else 0, "volumeMult": volume_mult,
                "longScore": long_score, "shortScore": short_score, "side": side,
                "checks": checks_long if side != "SHORT" else checks_short,
                "stop": stop, "target": target, "series": s1h.get("series", []), "updated": int(time.time()*1000)}
        with CACHE_LOCK: CACHE[symbol] = (time.time(), data)
        return data
    except Exception as e:
        return {"symbol": symbol, "error": str(e)}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args): pass
    def send_json(self, obj, status=200):
        raw = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(status); self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*"); self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(raw))); self.end_headers(); self.wfile.write(raw)
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/symbols":
            try: return self.send_json({"symbols": fetch_symbols()})
            except Exception as e: return self.send_json({"error": str(e)}, 502)
        if parsed.path == "/api/scan":
            symbols = parse_qs(parsed.query).get("symbols", [""])[0].split(",")
            symbols = list(dict.fromkeys(s.upper().strip() for s in symbols if re.fullmatch(r"[A-Z0-9]{3,25}", s.upper().strip())))[:20]
            try: allowed = set(fetch_symbols())
            except Exception as e: return self.send_json({"error": "Не удалось получить список Binance: " + str(e)}, 502)
            symbols = [s for s in symbols if s in allowed]
            with ThreadPoolExecutor(max_workers=5) as pool:
                data = list(pool.map(scan_one, symbols))
            return self.send_json({"items": data, "serverTime": int(time.time()*1000)})
        if parsed.path == "/" or parsed.path == "/index.html":
            try:
                with open(os.path.join(os.path.dirname(__file__), "index.html"), "rb") as f: raw = f.read()
                self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(raw))); self.end_headers(); self.wfile.write(raw)
            except OSError: self.send_error(404)
            return
        self.send_error(404)

if __name__ == "__main__":
    print(f"Binance screener listening on {HOST}:{PORT}", flush=True)
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
