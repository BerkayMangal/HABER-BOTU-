# ================================================================
# BERKAY FUNDAMENTALS V4 — Railway Edition
# Ayri bot token ile calisir, main.py'den bagimsiz
# ================================================================

import sys, importlib.util, os, re, math, asyncio, logging, datetime as dt
from html import escape as html_escape

import numpy as np
import pandas as pd
import yfinance as yf
from cachetools import TTLCache
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

BOT_VERSION = "V4"
APP_NAME = "BERKAY FUNDAMENTALS"
CONFIDENCE_MIN = 55

# ================================================================
# ENV VARS
# ================================================================
TOKEN = os.environ["FA_BOT_TOKEN"]
CHAT_ID = os.environ.get("CHAT_ID", "")
ALLOWED_CHAT_ID = int(CHAT_ID) if CHAT_ID else None

# ================================================================
# UNIVERSE
# ================================================================
UNIVERSE = [
    "ASELS","THYAO","BIMAS","KCHOL","SISE","EREGL","TUPRS","AKBNK","ISCTR","YKBNK",
    "GARAN","SAHOL","MGROS","FROTO","TOASO","TCELL","KRDMD","PETKM","ENKAI","TAVHL",
    "PGSUS","EKGYO","KOZAL","TTKOM","ARCLK","VESTL","DOHOL","AYGAZ","LOGO","SOKM",
    "TKFEN","KONTR","ODAS","GUBRF","SASA","ISMEN","OYAKC","CIMSA","MPARK","AKSEN",
]

# ================================================================
# CACHES + LOGGING
# ================================================================
RAW_CACHE = TTLCache(maxsize=5000, ttl=86400)
ANALYSIS_CACHE = TTLCache(maxsize=5000, ttl=86400)
TOP10_CACHE = {"asof": None, "items": []}

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("fa-bot")

# ================================================================
# HELPERS
# ================================================================
def normalize_symbol(ticker):
    t = (ticker or "").strip().upper().replace(" ", "")
    if t.endswith(".IS"): return t
    if "." in t: return t
    return f"{t}.IS"

def base_ticker(text):
    return (text or "").strip().upper().replace(".IS", "")

def is_allowed_chat(update):
    if ALLOWED_CHAT_ID is None: return True
    return bool(update.effective_chat and update.effective_chat.id == ALLOWED_CHAT_ID)

def safe_num(x):
    try:
        if x is None: return None
        x = float(x)
        if math.isnan(x) or math.isinf(x): return None
        return x
    except Exception: return None

def fmt_num(x, digits=2):
    x = safe_num(x)
    if x is None: return "N/A"
    if abs(x) >= 1e9: return f"{x/1e9:.2f}B"
    if abs(x) >= 1e6: return f"{x/1e6:.2f}M"
    if abs(x) >= 1e3: return f"{x:,.0f}"
    return f"{x:.{digits}f}"

def fmt_pct(x, digits=1):
    x = safe_num(x)
    if x is None: return "N/A"
    return f"{x*100:.{digits}f}%"

def pick_row_pair(df, names):
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return None, None
    for name in names:
        if name in df.index:
            try:
                s = df.loc[name]
                if isinstance(s, pd.DataFrame): s = s.iloc[:, 0]
                s = pd.to_numeric(s, errors="coerce").dropna()
                if s.empty: continue
                cur = safe_num(s.iloc[0])
                prev = safe_num(s.iloc[1]) if len(s) > 1 else None
                return cur, prev
            except Exception: continue
    return None, None

def growth(cur, prev):
    cur, prev = safe_num(cur), safe_num(prev)
    if cur is None or prev in (None, 0): return None
    return (cur - prev) / abs(prev)

def avg(values):
    vals = [safe_num(v) for v in values if safe_num(v) is not None]
    if not vals: return None
    return float(sum(vals) / len(vals))

def score_higher(x, bad, ok, good, great):
    x = safe_num(x)
    if x is None: return None
    if x <= bad: return 5.0
    if x >= great: return 100.0
    if x <= ok: return 5 + (x - bad) * (35 / max(ok - bad, 1e-9))
    if x <= good: return 40 + (x - ok) * (35 / max(good - ok, 1e-9))
    return 75 + (x - good) * (25 / max(great - good, 1e-9))

def score_lower(x, great, good, ok, bad):
    x = safe_num(x)
    if x is None: return None
    if x <= great: return 100.0
    if x >= bad: return 5.0
    if x <= good: return 100 - (x - great) * (25 / max(good - great, 1e-9))
    if x <= ok: return 75 - (x - good) * (35 / max(ok - good, 1e-9))
    return 40 - (x - ok) * (35 / max(bad - ok, 1e-9))

# ================================================================
# RAW FETCH (yfinance)
# ================================================================
def fetch_raw(symbol):
    if symbol in RAW_CACHE: return RAW_CACHE[symbol]
    tk = yf.Ticker(symbol)
    info = tk.get_info() or {}
    try: fast = getattr(tk, "fast_info", {}) or {}
    except Exception: fast = {}
    try: financials = tk.financials
    except Exception: financials = None
    try: balance = tk.balance_sheet
    except Exception: balance = None
    try: cashflow = tk.cashflow
    except Exception: cashflow = None
    raw = {"info": info, "fast": fast, "financials": financials, "balance": balance, "cashflow": cashflow}
    RAW_CACHE[symbol] = raw
    return raw

# ================================================================
# LEGENDARY METRICS
# ================================================================
def compute_piotroski(m):
    pts, used = 0, 0
    tests = [
        (m.get("roa", 0) > 0) if m.get("roa") is not None else None,
        (m.get("operating_cf", 0) > 0) if m.get("operating_cf") is not None else None,
        (m.get("roa", 0) > m.get("roa_prev", 0)) if (m.get("roa") is not None and m.get("roa_prev") is not None) else None,
        (m.get("operating_cf", 0) > m.get("net_income", 0)) if (m.get("operating_cf") is not None and m.get("net_income") is not None) else None,
        (m.get("current_ratio", 0) > m.get("current_ratio_prev", 0)) if (m.get("current_ratio") is not None and m.get("current_ratio_prev") is not None) else None,
        (m.get("share_change", 1) <= 0) if m.get("share_change") is not None else None,
        (m.get("gross_margin", 0) > m.get("gross_margin_prev", 0)) if (m.get("gross_margin") is not None and m.get("gross_margin_prev") is not None) else None,
        (m.get("asset_turnover", 0) > m.get("asset_turnover_prev", 0)) if (m.get("asset_turnover") is not None and m.get("asset_turnover_prev") is not None) else None,
    ]
    for t in tests:
        if t is None: continue
        used += 1; pts += int(t)
    return pts if used >= 4 else None

def compute_altman(m):
    wc = safe_num(m.get("working_capital"))
    ta = safe_num(m.get("total_assets"))
    re_ = safe_num(m.get("retained_earnings")) or 0.0
    ebit = safe_num(m.get("ebit"))
    tl = safe_num(m.get("total_liabilities"))
    sales = safe_num(m.get("revenue"))
    mve = safe_num(m.get("market_cap"))
    if None in (wc, ta, ebit, tl, sales, mve) or ta == 0 or tl == 0: return None
    return 1.2*(wc/ta) + 1.4*(re_/ta) + 3.3*(ebit/ta) + 0.6*(mve/tl) + 1.0*(sales/ta)

def compute_beneish(m):
    rec, rec_prev = m.get("receivables"), m.get("receivables_prev")
    sales, sales_prev = m.get("revenue"), m.get("revenue_prev")
    gp, gp_prev = m.get("gross_profit"), m.get("gross_profit_prev")
    ca, ca_prev = m.get("current_assets"), m.get("current_assets_prev")
    ppe, ppe_prev = m.get("ppe"), m.get("ppe_prev")
    dep, dep_prev = m.get("depreciation"), m.get("depreciation_prev")
    sga, sga_prev = m.get("sga"), m.get("sga_prev")
    debt, debt_prev = m.get("total_debt"), m.get("total_debt_prev")
    ta, ta_prev = m.get("total_assets"), m.get("total_assets_prev")
    ni, cfo = m.get("net_income"), m.get("operating_cf")
    if any(safe_num(x) in (None, 0) for x in [sales, sales_prev, ta, ta_prev]):
        return None
    try:
        dsri = ((rec or 0)/(sales or 1)) / max((rec_prev or 0)/(sales_prev or 1), 1e-9)
        gm = (gp or 0)/(sales or 1)
        gm_prev = (gp_prev or 0)/(sales_prev or 1)
        gmi = (gm_prev / max(gm, 1e-9)) if gm and gm_prev else 1.0
        aqi_num = 1 - ((ca or 0) + (ppe or 0)) / max(ta, 1e-9)
        aqi_den = 1 - ((ca_prev or 0) + (ppe_prev or 0)) / max(ta_prev, 1e-9)
        aqi = aqi_num / max(aqi_den, 1e-9)
        sgi = sales / max(sales_prev, 1e-9)
        dep_prev_rate = (dep_prev or 0) / max((dep_prev or 0) + (ppe_prev or 0), 1e-9)
        dep_cur_rate = (dep or 0) / max((dep or 0) + (ppe or 0), 1e-9)
        depi = dep_prev_rate / max(dep_cur_rate, 1e-9)
        sgai = ((sga or 0)/(sales or 1)) / max((sga_prev or 0)/(sales_prev or 1), 1e-9)
        lvgi = ((debt or 0)/max(ta, 1e-9)) / max((debt_prev or 0)/max(ta_prev, 1e-9), 1e-9)
        tata = ((ni or 0) - (cfo or 0)) / max(ta, 1e-9)
        return -4.84 + 0.92*dsri + 0.528*gmi + 0.404*aqi + 0.892*sgi + 0.115*depi - 0.172*sgai + 4.679*tata - 0.327*lvgi
    except Exception: return None

# ================================================================
# METRIC BUILD
# ================================================================
def compute_metrics(symbol):
    raw = fetch_raw(symbol)
    info, fast = raw["info"], raw["fast"]
    fin, bal, cf = raw["financials"], raw["balance"], raw["cashflow"]

    revenue, revenue_prev = pick_row_pair(fin, ["Total Revenue", "Operating Revenue"])
    gross_profit, gross_profit_prev = pick_row_pair(fin, ["Gross Profit"])
    operating_income, _ = pick_row_pair(fin, ["Operating Income", "EBIT"])
    ebit, _ = pick_row_pair(fin, ["EBIT", "Operating Income"])
    ebitda, ebitda_prev = pick_row_pair(fin, ["EBITDA"])
    net_income, net_income_prev = pick_row_pair(fin, ["Net Income", "Net Income Common Stockholders"])
    interest_exp, _ = pick_row_pair(fin, ["Interest Expense", "Interest Expense Non Operating"])
    dil_shares, dil_shares_prev = pick_row_pair(fin, ["Diluted Average Shares", "Basic Average Shares"])
    eps_row, eps_row_prev = pick_row_pair(fin, ["Diluted EPS", "Basic EPS"])
    sga, sga_prev = pick_row_pair(fin, ["Selling General And Administration"])

    op_cf, _ = pick_row_pair(cf, ["Operating Cash Flow", "Cash Flow From Continuing Operating Activities"])
    capex, _ = pick_row_pair(cf, ["Capital Expenditure"])
    dep, dep_prev = pick_row_pair(cf, ["Depreciation", "Depreciation And Amortization"])

    total_assets, total_assets_prev = pick_row_pair(bal, ["Total Assets"])
    total_liab, _ = pick_row_pair(bal, ["Total Liabilities Net Minority Interest", "Total Liabilities"])
    total_debt, total_debt_prev = pick_row_pair(bal, ["Total Debt"])
    cash, _ = pick_row_pair(bal, ["Cash Cash Equivalents And Short Term Investments", "Cash And Cash Equivalents"])
    cur_assets, cur_assets_prev = pick_row_pair(bal, ["Current Assets", "Total Current Assets"])
    cur_liab, cur_liab_prev = pick_row_pair(bal, ["Current Liabilities", "Total Current Liabilities"])
    ret_earn, _ = pick_row_pair(bal, ["Retained Earnings"])
    equity, _ = pick_row_pair(bal, ["Stockholders Equity", "Total Stockholder Equity"])
    receivables, rec_prev = pick_row_pair(bal, ["Accounts Receivable", "Receivables"])
    ppe, ppe_prev = pick_row_pair(bal, ["Net PPE", "Property Plant Equipment Net"])

    price = safe_num(fast.get("last_price")) or safe_num(info.get("currentPrice"))
    market_cap = safe_num(fast.get("market_cap")) or safe_num(info.get("marketCap"))
    pe = safe_num(info.get("trailingPE")) or safe_num(info.get("forwardPE"))
    pb = safe_num(info.get("priceToBook"))
    ev_ebitda = safe_num(info.get("enterpriseToEbitda"))
    div_yield = safe_num(info.get("dividendYield"))
    beta = safe_num(info.get("beta"))
    trailing_eps = safe_num(info.get("trailingEps")) or safe_num(eps_row)
    book_val_ps = safe_num(info.get("bookValue")) or ((equity/dil_shares) if equity and dil_shares else None)

    roe = safe_num(info.get("returnOnEquity")) or ((net_income/equity) if net_income and equity else None)
    roa = safe_num(info.get("returnOnAssets")) or ((net_income/total_assets) if net_income and total_assets else None)
    roa_prev = (net_income_prev/total_assets_prev) if net_income_prev and total_assets_prev else None
    gross_margin = (gross_profit/revenue) if gross_profit and revenue else None
    gross_margin_prev = (gross_profit_prev/revenue_prev) if gross_profit_prev and revenue_prev else None
    op_margin = safe_num(info.get("operatingMargins")) or ((operating_income/revenue) if operating_income and revenue else None)
    net_margin = safe_num(info.get("profitMargins")) or ((net_income/revenue) if net_income and revenue else None)
    cur_ratio = safe_num(info.get("currentRatio")) or ((cur_assets/cur_liab) if cur_assets and cur_liab else None)
    cur_ratio_prev = (cur_assets_prev/cur_liab_prev) if cur_assets_prev and cur_liab_prev else None
    debt_eq = safe_num(info.get("debtToEquity")) or ((total_debt/equity*100) if total_debt and equity else None)

    net_debt = (total_debt - cash) if total_debt is not None and cash is not None else None
    net_debt_ebit = (net_debt/ebitda) if net_debt is not None and ebitda not in (None, 0) else None
    _ebit_val = ebit if ebit is not None else operating_income
    int_cov = (_ebit_val/abs(interest_exp)) if _ebit_val is not None and interest_exp not in (None, 0) else None

    free_cf = ((op_cf + capex) if op_cf is not None and capex is not None else None) or safe_num(info.get("freeCashflow"))
    fcf_yield = (free_cf/market_cap) if free_cf is not None and market_cap not in (None, 0) else None
    fcf_margin = (free_cf/revenue) if free_cf is not None and revenue not in (None, 0) else None
    cfo_to_ni = (op_cf/net_income) if op_cf is not None and net_income not in (None, 0) else None

    rev_growth = safe_num(info.get("revenueGrowth")) or growth(revenue, revenue_prev)
    eps_growth = safe_num(info.get("earningsGrowth")) or growth(eps_row, eps_row_prev) or growth(net_income, net_income_prev)
    ebit_growth = growth(ebitda, ebitda_prev)

    wc = (cur_assets - cur_liab) if cur_assets is not None and cur_liab is not None else None
    tax_rate = safe_num(info.get("effectiveTaxRate")) or 0.20
    inv_cap = (total_debt + equity - cash) if total_debt is not None and equity is not None and cash is not None else None
    _ebit_nopat = ebit if ebit is not None else operating_income
    nopat = (_ebit_nopat * (1 - min(max(tax_rate, 0), 0.35))) if _ebit_nopat is not None else None
    roic = (nopat/inv_cap) if nopat is not None and inv_cap not in (None, 0) else None

    peg = (pe/max(eps_growth*100, 1e-9)) if pe not in (None, 0) and eps_growth is not None and eps_growth > 0 else None
    graham_fv = ((22.5*trailing_eps*book_val_ps)**0.5) if trailing_eps not in (None, 0) and book_val_ps not in (None, 0) and trailing_eps > 0 and book_val_ps > 0 else None
    mos = ((graham_fv - price)/graham_fv) if graham_fv not in (None, 0) and price is not None else None
    share_ch = growth(dil_shares, dil_shares_prev)
    asset_to = (revenue/total_assets) if revenue is not None and total_assets not in (None, 0) else None
    asset_to_p = (revenue_prev/total_assets_prev) if revenue_prev is not None and total_assets_prev not in (None, 0) else None

    m = {
        "symbol": symbol, "ticker": base_ticker(symbol),
        "name": str(info.get("shortName") or info.get("longName") or symbol),
        "currency": str(info.get("currency") or ""),
        "price": price, "market_cap": market_cap,
        "pe": pe, "pb": pb, "ev_ebitda": ev_ebitda, "dividend_yield": div_yield, "beta": beta,
        "revenue": revenue, "revenue_prev": revenue_prev,
        "gross_profit": gross_profit, "gross_profit_prev": gross_profit_prev,
        "operating_income": operating_income, "ebit": ebit or operating_income,
        "ebitda": ebitda, "ebitda_prev": ebitda_prev,
        "net_income": net_income, "net_income_prev": net_income_prev,
        "operating_cf": op_cf, "free_cf": free_cf,
        "total_assets": total_assets, "total_assets_prev": total_assets_prev,
        "total_liabilities": total_liab, "total_debt": total_debt, "total_debt_prev": total_debt_prev,
        "cash": cash, "current_assets": cur_assets, "current_assets_prev": cur_assets_prev,
        "current_liabilities": cur_liab, "current_liabilities_prev": cur_liab_prev,
        "working_capital": wc, "retained_earnings": ret_earn, "equity": equity,
        "receivables": receivables, "receivables_prev": rec_prev,
        "ppe": ppe, "ppe_prev": ppe_prev,
        "depreciation": dep, "depreciation_prev": dep_prev,
        "sga": sga, "sga_prev": sga_prev,
        "trailing_eps": trailing_eps, "book_value_ps": book_val_ps,
        "roe": roe, "roa": roa, "roa_prev": roa_prev, "roic": roic,
        "gross_margin": gross_margin, "gross_margin_prev": gross_margin_prev,
        "operating_margin": op_margin, "net_margin": net_margin,
        "current_ratio": cur_ratio, "current_ratio_prev": cur_ratio_prev,
        "debt_equity": debt_eq, "net_debt_ebitda": net_debt_ebit,
        "interest_coverage": int_cov,
        "fcf_yield": fcf_yield, "fcf_margin": fcf_margin, "cfo_to_ni": cfo_to_ni,
        "revenue_growth": rev_growth, "eps_growth": eps_growth, "ebitda_growth": ebit_growth,
        "peg": peg, "graham_fv": graham_fv, "margin_safety": mos,
        "share_change": share_ch, "asset_turnover": asset_to, "asset_turnover_prev": asset_to_p,
    }
    m["piotroski_f"] = compute_piotroski(m)
    m["altman_z"] = compute_altman(m)
    m["beneish_m"] = compute_beneish(m)
    return m

# ================================================================
# SCORING
# ================================================================
def score_value(m):
    return avg([
        score_lower(m.get("pe"), 8, 12, 18, 30) if (m.get("pe") or 0) > 0 else None,
        score_lower(m.get("pb"), 1, 1.8, 3, 5) if (m.get("pb") or 0) > 0 else None,
        score_lower(m.get("ev_ebitda"), 5, 8, 12, 18) if (m.get("ev_ebitda") or 0) > 0 else None,
        score_higher(m.get("fcf_yield"), 0, 0.02, 0.05, 0.08),
        score_higher(m.get("margin_safety"), -0.2, 0, 0.15, 0.30),
    ])

def score_quality(m):
    return avg([
        score_higher(m.get("roe"), 0.02, 0.08, 0.15, 0.25),
        score_higher(m.get("roic"), 0.02, 0.08, 0.12, 0.20),
        score_higher(m.get("gross_margin"), 0.10, 0.20, 0.30, 0.45),
        score_higher(m.get("operating_margin"), 0.03, 0.08, 0.15, 0.25),
        score_higher(m.get("net_margin"), 0.01, 0.05, 0.10, 0.18),
    ])

def score_growth(m):
    return avg([
        score_higher(m.get("revenue_growth"), -0.05, 0.03, 0.10, 0.20),
        score_higher(m.get("eps_growth"), -0.10, 0.03, 0.10, 0.20),
        score_higher(m.get("ebitda_growth"), -0.05, 0.03, 0.10, 0.18),
        score_lower(m.get("peg"), 0.6, 1.0, 1.8, 3.0) if (m.get("peg") or 0) > 0 else None,
    ])

def score_balance(m):
    nde = m.get("net_debt_ebitda")
    nde_s = 100.0 if nde is not None and nde < 0 else score_lower(nde, 0.5, 1.5, 2.5, 4.0)
    return avg([nde_s,
        score_lower(m.get("debt_equity"), 20, 60, 120, 250),
        score_higher(m.get("current_ratio"), 0.7, 1.0, 1.5, 2.2),
        score_higher(m.get("interest_coverage"), 1.0, 2.0, 5.0, 10.0),
        score_higher(m.get("altman_z"), 1.2, 1.8, 3.0, 4.5),
    ])

def score_earnings(m):
    bm = m.get("beneish_m")
    bm_s = None
    if bm is not None:
        bm_s = 90 if bm < -2.22 else (65 if bm < -1.78 else 25)
    return avg([
        score_higher(m.get("cfo_to_ni"), 0.2, 0.6, 0.9, 1.2),
        score_higher(m.get("fcf_margin"), -0.02, 0, 0.05, 0.12),
        bm_s,
    ])

def score_moat(m):
    stab = None
    if m.get("gross_margin") is not None and m.get("gross_margin_prev") is not None:
        stab = score_lower(abs(m["gross_margin"] - m["gross_margin_prev"]), 0, 0.03, 0.07, 0.15)
    return avg([
        score_higher(m.get("gross_margin"), 0.10, 0.20, 0.30, 0.45),
        score_higher(m.get("operating_margin"), 0.03, 0.08, 0.15, 0.25),
        score_higher(m.get("roic"), 0.02, 0.08, 0.12, 0.20),
        stab,
    ])

def score_capital(m):
    dil = None
    sc = m.get("share_change")
    if sc is not None:
        dil = 100 if sc <= 0 else score_lower(sc, 0, 0.03, 0.08, 0.20)
    return avg([
        score_higher(m.get("dividend_yield"), 0, 0.01, 0.03, 0.06),
        score_higher(m.get("fcf_yield"), 0, 0.02, 0.05, 0.08),
        score_higher(m.get("roic"), 0.02, 0.08, 0.12, 0.20),
        dil,
    ])

def confidence_score(m):
    keys = ["pe","pb","fcf_yield","roe","roic","operating_margin","revenue_growth","eps_growth",
            "net_debt_ebitda","interest_coverage","cfo_to_ni","piotroski_f","altman_z","peg","margin_safety"]
    have = sum(1 for k in keys if safe_num(m.get(k)) is not None)
    return round(100 * have / len(keys), 1)

def style_label(scores):
    v, q, g, moat = scores["value"], scores["quality"], scores["growth"], scores["moat"]
    if q >= 80 and g >= 65 and v >= 45 and moat >= 65: return "Quality Compounder"
    if q >= 78 and moat >= 72 and v < 45: return "Premium Compounder"
    if v >= 78 and scores["balance"] >= 60: return "Deep Value"
    if g >= 72 and v >= 50: return "GARP"
    if scores["balance"] < 45 and g >= 55: return "High-Risk Turnaround"
    return "Balanced"

def legendary_labels(m, scores):
    pf, az, bm, peg_v, mos = m.get("piotroski_f"), m.get("altman_z"), m.get("beneish_m"), m.get("peg"), m.get("margin_safety")
    pf_l = "N/A" if pf is None else (f"{int(pf)}/9 (Strong)" if pf >= 7 else f"{int(pf)}/9 (Okay)" if pf >= 5 else f"{int(pf)}/9 (Weak)")
    az_l = "N/A" if az is None else (f"{az:.2f} (Safe)" if az >= 3 else f"{az:.2f} (Grey)" if az >= 1.8 else f"{az:.2f} (Risk)")
    bm_l = "N/A" if bm is None else (f"{bm:.2f} (Low risk)" if bm < -2.22 else f"{bm:.2f} (Watch)" if bm < -1.78 else f"{bm:.2f} (Higher risk)")
    peg_l = "N/A" if peg_v is None else (f"{peg_v:.2f} (Cheap)" if peg_v < 1 else f"{peg_v:.2f} (Fair)" if peg_v <= 2 else f"{peg_v:.2f} (Rich)")
    mos_l = "N/A" if mos is None else ("High" if mos >= 0.20 else "Medium" if mos >= 0 else "Low")
    buffett = "Pass" if (scores["quality"] >= 75 and scores["moat"] >= 65 and scores["balance"] >= 60 and scores["capital"] >= 55) else ("Borderline" if scores["quality"] >= 60 and scores["moat"] >= 50 else "Fail")
    graham = "Pass" if (scores["value"] >= 70 and scores["balance"] >= 60 and (mos or -1) >= 0) else ("Borderline" if scores["value"] >= 55 else "Fail")
    return {"piotroski": pf_l, "altman": az_l, "beneish": bm_l, "peg": peg_l, "graham_mos": mos_l, "buffett_filter": buffett, "graham_filter": graham}

def drivers(scores, confidence):
    pos, neg = [], []
    if scores["quality"] >= 75: pos.append("Good business quality (ROIC / margins strong).")
    if scores["earnings"] >= 70: pos.append("Cash flow supports earnings.")
    if scores["balance"] >= 75: pos.append("Balance sheet solid.")
    if scores["value"] >= 75: pos.append("Looks cheap vs fundamentals.")
    if scores["moat"] >= 72: pos.append("Signs of pricing power.")
    if scores["capital"] >= 70: pos.append("Shareholder-friendly capital allocation.")
    if not pos: pos.append("Balanced profile, no single elite category.")
    if scores["value"] < 45: neg.append("Valuation looks expensive.")
    if scores["growth"] < 45: neg.append("Growth weak or inconsistent.")
    if scores["balance"] < 45: neg.append("Debt / liquidity needs watch.")
    if scores["earnings"] < 45: neg.append("Cash flow trails accounting profits.")
    if confidence < 70: neg.append("Some metrics missing; treat with caution.")
    if not neg: neg.append("No major red flag right now.")
    return pos[:3], neg[:3]

def analyze_symbol(symbol):
    if symbol in ANALYSIS_CACHE: return ANALYSIS_CACHE[symbol]
    m = compute_metrics(symbol)
    scores = {k: round((f(m) or 50), 1) for k, f in [
        ("value", score_value), ("quality", score_quality), ("growth", score_growth),
        ("balance", score_balance), ("earnings", score_earnings), ("moat", score_moat), ("capital", score_capital),
    ]}
    overall = 0.22*scores["value"] + 0.22*scores["quality"] + 0.16*scores["growth"] + 0.16*scores["balance"] + 0.10*scores["earnings"] + 0.09*scores["moat"] + 0.05*scores["capital"]
    if m.get("equity") is not None and m["equity"] < 0: overall -= 12
    if m.get("net_income") is not None and m["net_income"] < 0: overall -= 8
    if m.get("operating_cf") is not None and m["operating_cf"] < 0: overall -= 8
    if m.get("interest_coverage") is not None and m["interest_coverage"] < 1.5: overall -= 5
    if m.get("beneish_m") is not None and m["beneish_m"] > -1.78: overall -= 5
    overall = round(max(1, min(99, overall)), 1)
    confidence = confidence_score(m)
    style = style_label(scores)
    legends = legendary_labels(m, scores)
    pos, neg = drivers(scores, confidence)
    r = {
        "symbol": symbol, "ticker": base_ticker(symbol), "name": m["name"], "currency": m["currency"],
        "metrics": m, "scores": scores, "overall": overall, "confidence": confidence,
        "style": style, "legendary": legends, "positives": pos, "negatives": neg,
    }
    ANALYSIS_CACHE[symbol] = r
    return r

# ================================================================
# RENDERERS
# ================================================================
def header_line(r):
    return f"<b>{APP_NAME} [{BOT_VERSION}]</b>\n<b>{html_escape(r['ticker'])} — Score: {r['overall']}/100</b>"

def overview_text(r):
    s, m, L = r["scores"], r["metrics"], r["legendary"]
    lines = [
        header_line(r), html_escape(r["name"]),
        f"Style: {html_escape(r['style'])}  |  Confidence: {r['confidence']}/100",
        f"Buffett: {html_escape(L['buffett_filter'])}  |  Graham: {html_escape(L['graham_filter'])}",
    ]
    if m.get("price") is not None:
        lines.append(f"Price: {fmt_num(m['price'])} {html_escape(r['currency'])}")
    lines += ["", "<b>Scorecard</b>",
        f"Value: {s['value']} | Quality: {s['quality']} | Growth: {s['growth']}",
        f"Balance: {s['balance']} | Earnings: {s['earnings']} | Moat: {s['moat']}",
        "", "<b>Positives</b>", *[f"+ {html_escape(x)}" for x in r["positives"]],
        "", "<b>Negatives</b>", *[f"- {html_escape(x)}" for x in r["negatives"]],
        "", "Tap buttons for details.",
    ]
    return "\n".join(lines)

def value_text(r):
    m = r["metrics"]
    return "\n".join([f"<b>Value [{BOT_VERSION}] — {html_escape(r['ticker'])}</b>",
        f"P/E: {fmt_num(m.get('pe'))} | P/B: {fmt_num(m.get('pb'))} | EV/EBITDA: {fmt_num(m.get('ev_ebitda'))}",
        f"FCF Yield: {fmt_pct(m.get('fcf_yield'))} | PEG: {fmt_num(m.get('peg'))}",
        f"Graham FV: {fmt_num(m.get('graham_fv'))} | MoS: {fmt_pct(m.get('margin_safety'))}",
        f"<b>Value Score: {r['scores']['value']}/100</b>"])

def quality_text(r):
    m = r["metrics"]
    return "\n".join([f"<b>Quality [{BOT_VERSION}] — {html_escape(r['ticker'])}</b>",
        f"ROE: {fmt_pct(m.get('roe'))} | ROIC: {fmt_pct(m.get('roic'))}",
        f"Gross: {fmt_pct(m.get('gross_margin'))} | Op: {fmt_pct(m.get('operating_margin'))} | Net: {fmt_pct(m.get('net_margin'))}",
        f"<b>Quality Score: {r['scores']['quality']}/100</b>"])

def growth_text(r):
    m = r["metrics"]
    return "\n".join([f"<b>Growth [{BOT_VERSION}] — {html_escape(r['ticker'])}</b>",
        f"Revenue: {fmt_pct(m.get('revenue_growth'))} | EPS: {fmt_pct(m.get('eps_growth'))} | EBITDA: {fmt_pct(m.get('ebitda_growth'))}",
        f"PEG: {html_escape(r['legendary']['peg'])}",
        f"<b>Growth Score: {r['scores']['growth']}/100</b>"])

def balance_text(r):
    m = r["metrics"]
    return "\n".join([f"<b>Balance [{BOT_VERSION}] — {html_escape(r['ticker'])}</b>",
        f"Net Debt/EBITDA: {fmt_num(m.get('net_debt_ebitda'))} | D/E: {fmt_num(m.get('debt_equity'))}",
        f"Current: {fmt_num(m.get('current_ratio'))} | Int Cov: {fmt_num(m.get('interest_coverage'))}",
        f"Altman Z: {html_escape(r['legendary']['altman'])}",
        f"<b>Balance Score: {r['scores']['balance']}/100</b>"])

def earnings_text(r):
    m = r["metrics"]
    return "\n".join([f"<b>Earnings [{BOT_VERSION}] — {html_escape(r['ticker'])}</b>",
        f"CFO/NI: {fmt_num(m.get('cfo_to_ni'))} | FCF Margin: {fmt_pct(m.get('fcf_margin'))}",
        f"Beneish M: {html_escape(r['legendary']['beneish'])}",
        f"<b>Earnings Score: {r['scores']['earnings']}/100</b>"])

def legendary_text(r):
    L = r["legendary"]
    return "\n".join([f"<b>Legendary [{BOT_VERSION}] — {html_escape(r['ticker'])}</b>",
        f"Piotroski F: {html_escape(L['piotroski'])} | Altman Z: {html_escape(L['altman'])}",
        f"Beneish M: {html_escape(L['beneish'])} | PEG: {html_escape(L['peg'])}",
        f"MoS: {html_escape(L['graham_mos'])} | Buffett: {html_escape(L['buffett_filter'])} | Graham: {html_escape(L['graham_filter'])}"])

def risks_text(r):
    return "\n".join([f"<b>Risks [{BOT_VERSION}] — {html_escape(r['ticker'])}</b>",
        *[f"- {html_escape(x)}" for x in r["negatives"]]])

VIEW_RENDERERS = {"overview": overview_text, "value": value_text, "quality": quality_text,
    "growth": growth_text, "balance": balance_text, "earnings": earnings_text,
    "legendary": legendary_text, "risks": risks_text}

# ================================================================
# BUTTONS
# ================================================================
def stock_keyboard(ticker):
    t = base_ticker(ticker)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Overview", callback_data=f"view|{t}|overview"),
         InlineKeyboardButton("Value", callback_data=f"view|{t}|value"),
         InlineKeyboardButton("Quality", callback_data=f"view|{t}|quality")],
        [InlineKeyboardButton("Growth", callback_data=f"view|{t}|growth"),
         InlineKeyboardButton("Balance", callback_data=f"view|{t}|balance"),
         InlineKeyboardButton("Earnings", callback_data=f"view|{t}|earnings")],
        [InlineKeyboardButton("Legendary", callback_data=f"view|{t}|legendary"),
         InlineKeyboardButton("Risks", callback_data=f"view|{t}|risks"),
         InlineKeyboardButton("Top 10", callback_data="top10")],
    ])

def top10_keyboard(items):
    rows, row = [], []
    for item in items[:10]:
        row.append(InlineKeyboardButton(item["ticker"], callback_data=f"view|{item['ticker']}|overview"))
        if len(row) == 2: rows.append(row); row = []
    if row: rows.append(row)
    rows.append([InlineKeyboardButton("Refresh", callback_data="refresh_top10")])
    return InlineKeyboardMarkup(rows)

def top10_text_render():
    items = TOP10_CACHE["items"]
    if not items: return f"<b>Top 10 [{BOT_VERSION}]</b>\nHenuz taranmadi. /top10 yaz."
    stamp = TOP10_CACHE["asof"].strftime("%d.%m.%Y %H:%M")
    lines = [f"<b>BIST Top 10 [{BOT_VERSION}] — {stamp}</b>", ""]
    for i, item in enumerate(items[:10], 1):
        lines.append(f"{i}. <b>{item['ticker']}</b> — {item['overall']}/100 ({html_escape(item['style'])})")
    lines.append("\nTicker'a tikla detay gor.")
    return "\n".join(lines)

# ================================================================
# SCAN
# ================================================================
def scan_universe_blocking():
    ranked = []
    for t in UNIVERSE:
        try:
            r = analyze_symbol(normalize_symbol(t))
            if r["confidence"] >= CONFIDENCE_MIN:
                ranked.append(r)
        except Exception as e:
            log.warning("scan skip %s: %s", t, e)
    ranked.sort(key=lambda x: (x["overall"], x["scores"]["quality"]), reverse=True)
    TOP10_CACHE["asof"] = dt.datetime.now(dt.timezone.utc)
    TOP10_CACHE["items"] = ranked[:10]
    return TOP10_CACHE["items"]

# ================================================================
# BOT HANDLERS
# ================================================================
async def show_ticker(target, ticker, view="overview", try_edit=False):
    symbol = normalize_symbol(ticker)
    try:
        r = await asyncio.to_thread(analyze_symbol, symbol)
        m = r["metrics"]
        if m.get("price") is None and m.get("market_cap") is None and m.get("pe") is None:
            raise ValueError("No data")
        text = VIEW_RENDERERS.get(view, overview_text)(r)
        markup = stock_keyboard(r["ticker"])
        if try_edit:
            try:
                await target.edit_message_text(text=text, parse_mode=ParseMode.HTML, reply_markup=markup)
                return
            except Exception: pass
        if hasattr(target, "message") and target.message:
            await target.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
        else:
            raise RuntimeError("No target")
    except Exception:
        log.exception("show_ticker: %s", symbol)
        msg = f"Veri alinamadi: <b>{html_escape(base_ticker(ticker))}</b>"
        if hasattr(target, "message") and target.message:
            await target.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed_chat(update): return
    await update.message.reply_text(
        f"<b>{APP_NAME} [{BOT_VERSION}]</b>\n\n"
        "Ticker yaz: <code>ASELS</code> veya <code>TKFEN</code>\n"
        "/top10 — En iyi 10 hisse\n"
        "/ping — Bot canli mi\n"
        "/help — Yardim",
        parse_mode=ParseMode.HTML)

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed_chat(update): return
    await start_cmd(update, context)

async def ping_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed_chat(update): return
    await update.message.reply_text(f"[{BOT_VERSION}] Bot canli.")

async def top10_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed_chat(update): return
    if not TOP10_CACHE["items"]:
        wait = await update.message.reply_text("Top 10 taraniyor...")
        await asyncio.to_thread(scan_universe_blocking)
        await wait.edit_text(top10_text_render(), parse_mode=ParseMode.HTML, reply_markup=top10_keyboard(TOP10_CACHE["items"]))
    else:
        await update.message.reply_text(top10_text_render(), parse_mode=ParseMode.HTML, reply_markup=top10_keyboard(TOP10_CACHE["items"]))

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed_chat(update): return
    text = (update.message.text or "").strip()
    if not text: return
    token = base_ticker(text.split()[0])
    if not re.fullmatch(r"[A-Z0-9]{3,8}", token): return
    await show_ticker(update, token)

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query: return
    data = query.data or ""
    await query.answer()
    if data == "top10":
        if not TOP10_CACHE["items"]:
            await asyncio.to_thread(scan_universe_blocking)
        try:
            await query.edit_message_text(top10_text_render(), parse_mode=ParseMode.HTML, reply_markup=top10_keyboard(TOP10_CACHE["items"]))
        except Exception:
            if query.message:
                await query.message.reply_text(top10_text_render(), parse_mode=ParseMode.HTML, reply_markup=top10_keyboard(TOP10_CACHE["items"]))
        return
    if data == "refresh_top10":
        try: await query.edit_message_text("Yenileniyor...", parse_mode=ParseMode.HTML)
        except Exception: pass
        await asyncio.to_thread(scan_universe_blocking)
        try:
            await query.edit_message_text(top10_text_render(), parse_mode=ParseMode.HTML, reply_markup=top10_keyboard(TOP10_CACHE["items"]))
        except Exception: pass
        return
    if data.startswith("view|"):
        try:
            _, ticker, view = data.split("|", 2)
            await show_ticker(query, ticker, view=view, try_edit=True)
        except Exception:
            log.exception("callback: %s", data)

# ================================================================
# MAIN
# ================================================================
async def main():
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("ping", ping_cmd))
    app.add_handler(CommandHandler("top10", top10_cmd))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True, allowed_updates=["message", "callback_query"])

    log.info(f"BERKAY FUNDAMENTALS [{BOT_VERSION}] BASLADI! Universe: {len(UNIVERSE)} hisse")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
