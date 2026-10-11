import os, re, json, threading, time, urllib.request, urllib.parse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_HALF_UP

import requests
from flask import Flask, request, jsonify

IST = timezone(timedelta(hours=5, minutes=30))
ROOT_URL = os.environ.get("LEDGER_URL", "").strip()          # the root URL you were given (path=/)
LLM_BASE = os.environ.get("LLM_BASE_URL", "https://api.openai.com/v1").rstrip("/")
LLM_KEY = os.environ.get("LLM_API_KEY", "")
LLM_MODEL = os.environ.get("LLM_MODEL", "gpt-4o-mini")

app = Flask(__name__)
STATE = {"orders": [], "rates": {}, "ready": False, "error": None, "meta": {}}

# ---------------------------------------------------------------- loading
def http_json(url, tries=4):
    last = None
    for i in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=25) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            last = e
            time.sleep(0.5 * (i + 1))
    raise last

def parse_dt(s):
    s = str(s).strip().replace("Z", "+00:00")
    d = datetime.fromisoformat(s)
    if d.tzinfo is None:
        d = d.replace(tzinfo=IST)
    return d

def set_page(url, n):
    p = urllib.parse.urlparse(url)
    q = urllib.parse.parse_qsl(p.query, keep_blank_values=True)
    q = [(k, v) for k, v in q if k != "page"] + [("page", str(n))]
    return urllib.parse.urlunparse(p._replace(query=urllib.parse.urlencode(q)))

def build(raw_rows, rates):
    """dedupe by id (latest updated_at wins) and precompute fields"""
    best = {}
    for r in raw_rows:
        try:
            u = parse_dt(r["updated_at"])
        except Exception:
            u = datetime.min.replace(tzinfo=timezone.utc)
        cur = best.get(r["id"])
        if cur is None or u >= cur[0]:
            best[r["id"]] = (u, r)
    out = []
    for _, r in best.values():
        cur_code = str(r.get("currency", "USD")).strip().upper()
        rate = Decimal(str(rates.get(cur_code, 1)))
        amt = r.get("amount")
        if amt is None:
            amt = Decimal(str(r["qty"])) * Decimal(str(r["unit_price"]))
        usd = Decimal(str(amt)) * rate
        d = parse_dt(r["created_at"]).astimezone(IST)
        out.append({
            "id": r["id"], "customer": str(r.get("customer", "")).strip(),
            "region": str(r.get("region", "")).strip(), "product": str(r.get("product", "")).strip(),
            "qty": int(r.get("qty", 0) or 0), "usd": usd,
            "status": str(r.get("status", "")).strip().lower(),
            "date": d.date(),
        })
    return out

def http_text(url, tries=4):
    last = None
    for i in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                return r.read().decode()
        except Exception as e:
            last = e
            time.sleep(0.5 * (i + 1))
    raise last

def load_rows(links):
    """Prefer the one-shot NDJSON export; fall back to paging /orders."""
    try:
        txt = http_text(links["export"])
        rows = [json.loads(l) for l in txt.splitlines() if l.strip()]
        if rows:
            return rows, "export"
    except Exception:
        pass
    first = http_json(links["orders"])
    pages = int(first.get("pages", 1))
    rows = list(first["orders"])
    def get(n):
        return http_json(set_page(links["orders"], n))["orders"]
    with ThreadPoolExecutor(12) as ex:
        for part in ex.map(get, range(2, pages + 1)):
            rows.extend(part)
    return rows, "orders-pages"

def load():
    try:
        root = http_json(ROOT_URL)
        links = root["links"]
        rates = http_json(links["rates"])["usd_per_unit"]
        rows, src = load_rows(links)
        orders = build(rows, rates)
        STATE.update(orders=orders, rates=rates, ready=True, error=None,
                     meta={"raw_rows": len(rows), "unique_orders": len(orders), "source": src})
    except Exception as e:
        STATE["error"] = repr(e)

def ensure_loaded(wait=12):
    t0 = time.time()
    while not STATE["ready"] and time.time() - t0 < wait:
        time.sleep(0.2)
    return STATE["ready"]

if ROOT_URL:
    threading.Thread(target=load, daemon=True).start()
else:
    STATE["error"] = "LEDGER_URL environment variable is not set"

# ---------------------------------------------------------------- query engine
def norm(s):
    s = re.sub(r"[^a-z0-9]", "", str(s).lower())
    return s[:-1] if s.endswith("s") and len(s) > 3 else s

def canon_map(values):
    m = {}
    for v in values:
        m[norm(v)] = v
    return m

def as_list(x):
    if x is None or x == "":
        return None
    return x if isinstance(x, list) else [x]

def money(d):
    return float(Decimal(d).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

def run_spec(spec):
    orders = STATE["orders"]
    regions = canon_map({o["region"] for o in orders})
    products = canon_map({o["product"] for o in orders})
    metric = spec.get("metric") or "amount_usd"
    status = (spec.get("status") or "paid").lower()
    flt = {}
    for field, cmap in (("region", regions), ("product", products)):
        vals = as_list(spec.get(field))
        if vals:
            flt[field] = {cmap.get(norm(v), v) for v in vals}
    custs = as_list(spec.get("customer"))
    if custs:
        flt["customer"] = {str(c).strip().upper() for c in custs}
    start = datetime.strptime(spec["start"], "%Y-%m-%d").date() if spec.get("start") else None
    end = datetime.strptime(spec["end"], "%Y-%m-%d").date() if spec.get("end") else None

    rows = []
    for o in orders:
        if status != "any" and o["status"] != status:
            continue
        if "region" in flt and o["region"] not in flt["region"]: continue
        if "product" in flt and o["product"] not in flt["product"]: continue
        if "customer" in flt and o["customer"].upper() not in flt["customer"]: continue
        if start and o["date"] < start: continue
        if end and o["date"] > end: continue
        rows.append(o)

    def measure(rs):
        if metric == "amount_usd":   return sum((r["usd"] for r in rs), Decimal(0))
        if metric == "order_count":  return len(rs)
        if metric == "unit_count":   return sum(r["qty"] for r in rs)
        if metric == "customer_count": return len({r["customer"] for r in rs})
        if metric == "avg_order_usd":
            return (sum((r["usd"] for r in rs), Decimal(0)) / len(rs)) if rs else Decimal(0)
        raise ValueError("bad metric " + metric)

    def fmt(v):
        return money(v) if isinstance(v, Decimal) else v

    gb = spec.get("group_by")
    if not gb:
        return fmt(measure(rows))
    keyf = {"region": lambda r: r["region"], "product": lambda r: r["product"],
            "customer": lambda r: r["customer"], "month": lambda r: r["date"].strftime("%Y-%m"),
            "status": lambda r: r["status"], "date": lambda r: r["date"].isoformat()}[gb]
    groups = defaultdict(list)
    for r in rows:
        groups[keyf(r)].append(r)
    pairs = [(k, measure(v)) for k, v in groups.items()]
    reverse = (spec.get("sort") or "desc") != "asc"
    pairs.sort(key=lambda kv: (-kv[1] if reverse else kv[1], kv[0]) if not isinstance(kv[1], Decimal)
               else (-kv[1] if reverse else kv[1], kv[0]))
    limit = spec.get("limit")
    if limit: pairs = pairs[: int(limit)]
    out = spec.get("output") or "key"
    if out == "key":
        keys = [k for k, _ in pairs]
        return keys[0] if len(keys) == 1 else keys
    if out == "pairs":
        return {k: fmt(v) for k, v in pairs}
    return fmt(pairs[0][1]) if pairs else 0

# ---------------------------------------------------------------- LLM translation
SYSTEM = """You translate questions about an order ledger into a JSON query. Reply with ONLY a JSON object, no prose.
Fields (omit or use null when not needed):
- metric: "amount_usd" (money, in USD) | "order_count" | "unit_count" (sum of quantity) | "customer_count" (distinct customers) | "avg_order_usd"
- status: "paid" (DEFAULT; use for revenue/sales/orders/units/customers) | "refunded" (use for anything about refunds / refunded orders / refund amounts) | "void" | "any"
- region, product: exact name from the allowed lists (string or list of strings)
- customer: customer id such as "C1246" (string or list)
- start, end: inclusive dates YYYY-MM-DD (business dates in India). "March 2026" -> start 2026-03-01, end 2026-03-31. "Q1 2026" -> 2026-01-01..2026-03-31. A single year -> Jan 1..Dec 31.
- group_by: "region" | "product" | "customer" | "month" | null  (use when the question asks which/top/best/highest/lowest/each)
- sort: "desc" (default) or "asc" (lowest/least/fewest)
- limit: integer number of groups to keep (e.g. 1 for "which product ...", 3 for "top 3")
- output: "key" (the name of the group(s), e.g. "which product") | "value" (the metric value of the top group) | "pairs" (name -> value for each group kept)
Rules: revenue/sales/total = metric amount_usd with status paid. Refund amount = amount_usd with status refunded. Number of refunds = order_count with status refunded.
Allowed regions: {regions}
Allowed products: {products}
Today (IST): {today}."""

def to_spec(question, deadline=None):
    regions = sorted({o["region"] for o in STATE["orders"]})
    products = sorted({o["product"] for o in STATE["orders"]})
    sys_prompt = SYSTEM.format(regions=regions, products=products,
                               today=datetime.now(IST).date().isoformat())
    body = {"model": LLM_MODEL, "temperature": 0,
            "messages": [{"role": "system", "content": sys_prompt},
                         {"role": "user", "content": question}]}
    last = None
    for attempt in range(2):
        left = (deadline - time.time()) if deadline else 9
        if left < 2: break
        try:
            r = requests.post(LLM_BASE + "/chat/completions", json=body, timeout=min(8, left),
                              headers={"Authorization": "Bearer " + LLM_KEY})
            r.raise_for_status()
            txt = r.json()["choices"][0]["message"]["content"]
            m = re.search(r"\{.*\}", txt, re.S)
            return json.loads(m.group(0))
        except Exception as e:
            last = e
    raise last


# ---------------------------------------------------------------- rule-based fallback (no LLM)
import calendar
MONTHS = {m.lower(): i for i, m in enumerate(calendar.month_name) if m}
MONTHS.update({m.lower(): i for i, m in enumerate(calendar.month_abbr) if m})

def rule_spec(q):
    ql = q.lower()
    spec = {}
    if re.search(r"refund", ql): spec["status"] = "refunded"
    elif re.search(r"\b(void|voided|cancel)", ql): spec["status"] = "void"
    else: spec["status"] = "paid"
    if re.search(r"distinct|unique|different", ql) and "customer" in ql: spec["metric"] = "customer_count"
    elif re.search(r"how many|count|number of", ql) and not re.search(r"units|items|quantity|pieces", ql): spec["metric"] = "order_count"
    elif re.search(r"units|items sold|quantity|pieces", ql): spec["metric"] = "unit_count"
    elif re.search(r"average|avg|mean", ql): spec["metric"] = "avg_order_usd"
    else: spec["metric"] = "amount_usd"
    regions = {o["region"] for o in STATE["orders"]}
    products = {o["product"] for o in STATE["orders"]}
    qn = norm(ql)
    spec["region"] = [r for r in regions if re.search(r"\b" + re.escape(r.lower()) + r"\b", ql)] or None
    spec["product"] = [p for p in products if norm(p) in qn or norm(p + "s") in qn] or None
    c = re.findall(r"\bc\d{3,5}\b", ql)
    spec["customer"] = [x.upper() for x in c] or None
    year = re.search(r"\b(20\d\d)\b", ql)
    yr = int(year.group(1)) if year else 2026
    mm = [MONTHS[m] for m in re.findall(r"\b(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)\b", ql) if m in MONTHS]
    qtr = re.search(r"\bq([1-4])\b", ql)
    if qtr:
        k = int(qtr.group(1)); a, b = 3 * k - 2, 3 * k
        spec["start"] = f"{yr}-{a:02d}-01"; spec["end"] = f"{yr}-{b:02d}-{calendar.monthrange(yr, b)[1]:02d}"
    elif mm:
        a, b = min(mm), max(mm)
        spec["start"] = f"{yr}-{a:02d}-01"; spec["end"] = f"{yr}-{b:02d}-{calendar.monthrange(yr, b)[1]:02d}"
    elif year:
        spec["start"] = f"{yr}-01-01"; spec["end"] = f"{yr}-12-31"
    if re.search(r"\b(which|top|best|highest|most|biggest|largest|lowest|least|fewest)\b", ql):
        for key in ("product", "region", "customer"):
            if re.search(r"which\s+" + key + r"|top\s+\d*\s*" + key + r"|" + key + r"\s+(with|had|has|generated|earned)", ql) or ("which " + key) in ql:
                spec["group_by"] = key; break
        if spec.get("group_by"):
            spec["sort"] = "asc" if re.search(r"lowest|least|fewest|worst", ql) else "desc"
            n = re.search(r"top\s+(\d+)", ql)
            spec["limit"] = int(n.group(1)) if n else 1
            spec["output"] = "key"
            spec[spec["group_by"]] = None
    return spec

def answer_question(q):
    deadline = time.time() + 11
    try:
        return run_spec(to_spec(q, deadline))
    except Exception as e:
        STATE["last_llm_error"] = repr(e)
        return run_spec(rule_spec(q))

# ---------------------------------------------------------------- routes
@app.route("/", methods=["POST", "OPTIONS"])
@app.route("/ask", methods=["POST", "OPTIONS"])
def ask():
    if request.method == "OPTIONS":
        r = jsonify({}); r.headers["Access-Control-Allow-Origin"] = "*"
        r.headers["Access-Control-Allow-Headers"] = "*"; return r
    q = (request.get_json(force=True, silent=True) or {}).get("question", "")
    if not ensure_loaded():
        resp = jsonify({"answer": None, "error": STATE["error"] or "data still loading"})
    else:
        try:
            resp = jsonify({"answer": answer_question(q)})
        except Exception as e:
            resp = jsonify({"answer": None, "error": repr(e)})
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp

@app.route("/health")
def health():
    return jsonify({"ready": STATE["ready"], "error": STATE["error"], "last_llm_error": STATE.get("last_llm_error"), "ledger_url_set": bool(ROOT_URL), **STATE["meta"]})

@app.route("/debug")
def debug():
    o = STATE["orders"]
    c = lambda f: sorted({x[f] for x in o})
    return jsonify({"meta": STATE["meta"], "rates": STATE["rates"],
                    "statuses": c("status"), "regions": c("region"), "products": c("product"),
                    "n_customers": len(c("customer")),
                    "date_min": str(min(x["date"] for x in o)) if o else None,
                    "date_max": str(max(x["date"] for x in o)) if o else None,
                    "paid_total_usd": money(sum((x["usd"] for x in o if x["status"] == "paid"), Decimal(0)))})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
