# app/tools/report_to_discord.py
from __future__ import annotations

import os, asyncio, aiohttp, math
from collections import defaultdict
from typing import Any, Dict, Iterable, Tuple, List

# ---- Required ENV ----
SUPABASE_URL  = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_KEY  = os.environ["SUPABASE_KEY"]

# Prefer the new name; fallback to the old one if present
DISCORD_WEBHOOK = (
    os.environ.get("DISCORD_WEBHOOK_PERFORMANCE", "").strip()
    or os.environ.get("DISCORD_PERF_WEBHOOK", "").strip()
)

# ---- Optional filters / knobs ----
REPORT_SYMBOL  = os.environ.get("REPORT_SYMBOL", "").strip()   # e.g. "BTCUSDT"
REPORT_MIN_N   = int(os.environ.get("REPORT_MIN_N", "1"))      # min sample size to show a row
REPORT_TOP     = int(os.environ.get("REPORT_TOP", "25"))       # max rows per table
REPORT_HORIZON = os.environ.get("REPORT_HORIZON", "").strip()  # e.g. "15,30,60" or ""

REST_BASE = f"{SUPABASE_URL}/rest/v1"
HEADERS  = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}

# ---------- Supabase REST helpers ----------
async def fetch_all(table: str, select: str, where: Dict[str, str] | None = None, page: int = 10000) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    start = 0
    async with aiohttp.ClientSession() as sess:
        while True:
            params = {"select": select}
            if where: params.update(where)
            headers = dict(HEADERS)
            headers["Range-Unit"] = "items"
            headers["Range"] = f"{start}-{start+page-1}"
            async with sess.get(f"{REST_BASE}/{table}", headers=headers, params=params) as r:
                if r.status >= 400:
                    raise RuntimeError(f"GET {table} failed {r.status}: {await r.text()}")
                chunk = await r.json()
                if not chunk:
                    break
                rows.extend(chunk)
                if len(chunk) < page:
                    break
                start += page
    return rows

async def post_discord(text: str):
    if not DISCORD_WEBHOOK:
        print(text)  # fallback to stdout
        return
    async with aiohttp.ClientSession() as sess:
        async with sess.post(DISCORD_WEBHOOK, json={"content": text[:1990]}) as r:
            if r.status >= 300:
                raise RuntimeError(f"Discord post failed {r.status}: {await r.text()}")

# ---------- Aggregation ----------
def _h_filter(h: int) -> bool:
    if not REPORT_HORIZON:
        return True
    try:
        wanted = {int(x.strip()) for x in REPORT_HORIZON.split(",") if x.strip()}
    except Exception:
        return True
    return h in wanted

def summarize(rows: List[Dict[str, Any]]):
    # rows contain: symbol,horizon_m,score,ret,max_fav,max_adv,triggers(json)
    by_sym_h: dict[Tuple[str,int], list]   = defaultdict(list)
    by_trig_h: dict[Tuple[str,int], list]  = defaultdict(list)
    by_score_h: dict[Tuple[float,int], list] = defaultdict(list)

    for r in rows:
        h  = int(r.get("horizon_m") or 0)
        if not _h_filter(h): continue
        sym = str(r.get("symbol") or "")
        ret = float(r.get("ret") or 0.0)

        by_sym_h[(sym, h)].append(r)

        # triggers is an array; ensure iterable
        for t in (r.get("triggers") or []):
            by_trig_h[(str(t), h)].append(r)

        b = round(float(r.get("score") or 0.0), 1)
        by_score_h[(b, h)].append(r)

    def agg(group: dict[Tuple[Any,int], list]) -> List[Tuple[Tuple[Any,int], int, float, float, float, float]]:
        out = []
        for key, lst in group.items():
            n   = len(lst)
            win = sum(1 for x in lst if float(x.get("ret") or 0.0) > 0)
            exp = sum(float(x.get("ret") or 0.0)        for x in lst) / max(1, n)
            mfe = sum(float(x.get("max_fav") or 0.0)    for x in lst) / max(1, n)
            mae = sum(float(x.get("max_adv") or 0.0)    for x in lst) / max(1, n)
            if n >= REPORT_MIN_N:
                out.append((key, n, win / n, exp, mfe, mae))
        # sort by expectancy desc, then n desc
        out.sort(key=lambda t: (t[3], t[1]), reverse=True)
        return out
    return agg(by_sym_h), agg(by_trig_h), agg(by_score_h)

def fmt_table(title: str, rows: Iterable[Tuple[Tuple[Any,int], int, float, float, float, float]], key_hdr: str, top: int) -> str:
    rows = list(rows)[:max(1, top)]
    lines = [f"**{title}**"]
    if not rows:
        lines.append("_no rows_")
        return "\n".join(lines)
    lines.append(f"`{key_hdr:26s}  n    win   exp       mfe       mae`")
    for (key, n, win, exp, mfe, mae) in rows:
        label = " | ".join(map(str, key)) if isinstance(key, tuple) else str(key)
        lines.append(f"`{label[:26]:26s}  {n:4d}  {win:0.2f}  {exp:0.6f}  {mfe:0.6f}  {mae:0.6f}`")
    return "\n".join(lines)

# ---------- Main ----------
async def main():
    where = {}
    if REPORT_SYMBOL:
        where["symbol"] = f"eq.{REPORT_SYMBOL}"

    rows = await fetch_all(
        "v_signal_perf",
        select="symbol,horizon_m,score,ret,max_fav,max_adv,triggers",
        where=where,
    )
    if not rows:
        await post_discord("📊 Report: no rows found in `v_signal_perf` with current filters.")
        return

    sym_rows, trig_rows, bucket_rows = summarize(rows)

    header = "📊 **Sniper Performance**"
    if REPORT_SYMBOL:
        header += f" — `{REPORT_SYMBOL}`"
    if REPORT_HORIZON:
        header += f" — horizons `{REPORT_HORIZON}`"
    header += f" — min n={REPORT_MIN_N}"

    parts = [header]
    parts.append(fmt_table("By Symbol × Horizon", sym_rows, "symbol | horizon", REPORT_TOP))
    parts.append(fmt_table("By Trigger × Horizon", trig_rows, "trigger | horizon", REPORT_TOP))
    parts.append(fmt_table("By Score Bucket × Horizon", bucket_rows, "score~ | horizon", REPORT_TOP))

    # Discord 2k char limit → chunk posts
    buf = ""
    for block in parts:
        block = block.strip()
        if not block: continue
        if len(buf) + len(block) + 2 > 1900:
            await post_discord(buf)
            buf = block
        else:
            buf = f"{buf}\n\n{block}" if buf else block
    if buf:
        await post_discord(buf)

if __name__ == "__main__":
    asyncio.run(main())
