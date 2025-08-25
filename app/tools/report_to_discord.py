# app/tools/report_to_discord.py
from __future__ import annotations

import os, asyncio, aiohttp
from collections import defaultdict
from typing import Any, Dict, Iterable, Tuple, List

SUPABASE_URL  = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_KEY  = os.environ["SUPABASE_KEY"]
DISCORD_WEBHOOK = (
    os.environ.get("DISCORD_WEBHOOK_PERFORMANCE", "").strip()
    or os.environ.get("DISCORD_PERF_WEBHOOK", "").strip()
)

REPORT_SYMBOL  = os.environ.get("REPORT_SYMBOL", "").strip()
REPORT_MIN_N   = int(os.environ.get("REPORT_MIN_N", "1"))
REPORT_TOP     = int(os.environ.get("REPORT_TOP", "20"))
REPORT_HORIZON = os.environ.get("REPORT_HORIZON", "").strip()

REST_BASE = f"{SUPABASE_URL}/rest/v1"
HEADERS  = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}

# ---------- Supabase ----------
async def fetch_all(table: str, select: str, where: Dict[str, str] | None = None, page: int = 10000):
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
                    raise RuntimeError(f"GET {table} {r.status}: {await r.text()}")
                chunk = await r.json()
                if not chunk: break
                rows.extend(chunk)
                if len(chunk) < page: break
                start += page
    return rows

# ---------- Discord ----------
async def post_embed(title: str, fields: List[dict], footer: str = ""):
    if not DISCORD_WEBHOOK:
        print(f"\n== {title} ==")
        for f in fields: print(f"{f['name']}: {f['value']}")
        return
    embed = {"title": title, "color": 0x5865F2, "fields": fields[:25]}
    if footer: embed["footer"] = {"text": footer}
    async with aiohttp.ClientSession() as sess:
        async with sess.post(DISCORD_WEBHOOK, json={"embeds": [embed]}) as r:
            if r.status >= 300:
                raise RuntimeError(f"Discord post failed {r.status}: {await r.text()}")

# ---------- Aggregation ----------
def _h_filter(h: int) -> bool:
    if not REPORT_HORIZON: return True
    wanted = {int(x.strip()) for x in REPORT_HORIZON.split(",") if x.strip()}
    return h in wanted

def summarize(rows: List[Dict[str, Any]]):
    by_sym_h, by_trig_h, by_score_h = defaultdict(list), defaultdict(list), defaultdict(list)
    for r in rows:
        h = int(r.get("horizon_m") or 0)
        if not _h_filter(h): continue
        sym = str(r.get("symbol") or "")
        by_sym_h[(sym, h)].append(r)
        for t in (r.get("triggers") or []):
            by_trig_h[(str(t), h)].append(r)
        b = round(float(r.get("score") or 0.0), 1)
        by_score_h[(b, h)].append(r)

    def agg(group):
        out=[]
        for key,lst in group.items():
            n=len(lst)
            if n<REPORT_MIN_N: continue
            win=sum(1 for x in lst if float(x.get("ret") or 0.0)>0)/n
            exp=sum(float(x.get("ret") or 0.0) for x in lst)/n
            mfe=sum(float(x.get("max_fav") or 0.0) for x in lst)/n
            mae=sum(float(x.get("max_adv") or 0.0) for x in lst)/n
            out.append((key,n,win,exp,mfe,mae))
        out.sort(key=lambda t:(t[3],t[1]), reverse=True)
        return out[:REPORT_TOP]
    return agg(by_sym_h), agg(by_trig_h), agg(by_score_h)

def pack_fields(rows: Iterable[Tuple[Tuple[Any,int], int, float, float, float, float]], key_hdr: str):
    name_col=["Key","—"]; n_col=["n","—"]; win_col=["win","—"]; exp_col=["exp","—"]; mfe_col=["mfe","—"]; mae_col=["mae","—"]
    for (key,n,win,exp,mfe,mae) in rows:
        label = " | ".join(map(str,key)) if isinstance(key,tuple) else str(key)
        name_col.append(label[:42]); n_col.append(str(n)); win_col.append(f"{win:0.2f}")
        exp_col.append(f"{exp:0.6f}"); mfe_col.append(f"{mfe:0.6f}"); mae_col.append(f"{mae:0.6f}")
    def block(lines): return "```\n" + "\n".join(lines) + "\n```"
    return [
        {"name":"Key", "value":block(name_col), "inline":True},
        {"name":"n",   "value":block(n_col),   "inline":True},
        {"name":"win", "value":block(win_col), "inline":True},
        {"name":"exp", "value":block(exp_col), "inline":True},
        {"name":"mfe", "value":block(mfe_col), "inline":True},
        {"name":"mae", "value":block(mae_col), "inline":True},
    ]

# ---------- Main ----------
async def main():
    where = {"symbol": f"eq.{REPORT_SYMBOL}"} if REPORT_SYMBOL else None
    rows = await fetch_all("v_signal_perf", select="symbol,horizon_m,score,ret,max_fav,max_adv,triggers", where=where)
    if not rows:
        await post_embed("Sniper Performance", [{"name":"Info","value":"_no rows in v_signal_perf_"}]); return

    sym_rows, trig_rows, bucket_rows = summarize(rows)
    footer = " • ".join(filter(None, [
        f"symbol={REPORT_SYMBOL}" if REPORT_SYMBOL else "",
        f"horizons={REPORT_HORIZON}" if REPORT_HORIZON else "",
        f"min_n={REPORT_MIN_N}"
    ]))

    await post_embed("Sniper Performance • By Symbol × Horizon",      pack_fields(sym_rows,   "symbol | horizon"), footer)
    await post_embed("Sniper Performance • By Trigger × Horizon",     pack_fields(trig_rows,  "trigger | horizon"), footer)
    await post_embed("Sniper Performance • By Score Bucket × Horizon",pack_fields(bucket_rows,"score~ | horizon"), footer)

if __name__ == "__main__":
    asyncio.run(main())
