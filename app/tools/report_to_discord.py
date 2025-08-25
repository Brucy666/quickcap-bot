# app/tools/report_to_discord.py
import os, asyncio, aiohttp, json, math
from collections import defaultdict

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
DISCORD_WEBHOOK = os.environ.get("DISCORD_PERF_WEBHOOK", "").strip()

REST_BASE = f"{SUPABASE_URL}/rest/v1"
HEAD = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}

async def fetch_all(table: str, select: str = "*", limit: int = 10000, where: dict | None = None):
    out, start = [], 0
    async with aiohttp.ClientSession() as s:
        while True:
            params = {"select": select}
            if where:
                params.update(where)
            headers = dict(HEAD)
            headers["Range-Unit"] = "items"
            headers["Range"] = f"{start}-{start+limit-1}"
            async with s.get(f"{REST_BASE}/{table}", headers=headers, params=params) as r:
                if r.status >= 400:
                    raise RuntimeError(f"GET {table} {r.status}: {await r.text()}")
                rows = await r.json()
                if not rows:
                    break
                out.extend(rows)
                if len(rows) < limit:
                    break
                start += limit
    return out

def agg_perf(rows):
    # rows from v_signal_perf
    by_sym_h = defaultdict(list)
    by_trig_h = defaultdict(list)
    by_score_h = defaultdict(list)

    for r in rows:
        sym = r["symbol"]
        h   = r["horizon_m"]
        ret = r.get("ret") or 0.0
        by_sym_h[(sym, h)].append(r)

        # triggers is JSON array
        for t in (r.get("triggers") or []):
            by_trig_h[(t, h)].append(r)

        # score bucket (0.1 precision)
        b = round((r.get("score") or 0.0), 1)
        by_score_h[(b, h)].append(r)

    def summarize(group):
        res = []
        for key, lst in sorted(group.items()):
            n = len(lst)
            win = sum(1 for x in lst if (x.get("ret") or 0) > 0)
            exp = sum((x.get("ret") or 0.0) for x in lst) / max(1, n)
            mfe = sum((x.get("max_fav") or 0.0) for x in lst) / max(1, n)
            mae = sum((x.get("max_adv") or 0.0) for x in lst) / max(1, n)
            res.append((key, n, win / max(1, n), exp, mfe, mae))
        return res

    return summarize(by_sym_h), summarize(by_trig_h), summarize(by_score_h)

def fmt_table(title, rows, key_labels):
    lines = [f"**{title}**"]
    if not rows:
        lines.append("_no rows_")
        return "\n".join(lines)
    head = f"`{key_labels:26s}  n    win   exp       mfe       mae`"
    lines.append(head)
    for (key, n, win, exp, mfe, mae) in rows[:25]:
        lbl = " | ".join(map(str, key)) if isinstance(key, tuple) else str(key)
        lines.append(f"`{lbl[:26]:26s}  {n:4d}  {win:0.2f}  {exp:0.6f}  {mfe:0.6f}  {mae:0.6f}`")
    return "\n".join(lines)

async def post_discord(content: str):
    if not DISCORD_WEBHOOK:
        print("[REPORT] DISCORD_PERF_WEBHOOK not set; printing to stdout.\n")
        print(content)
        return
    async with aiohttp.ClientSession() as s:
        async with s.post(DISCORD_WEBHOOK, json={"content": content[:1990]}) as r:
            if r.status >= 300:
                raise RuntimeError(f"Discord post failed {r.status}: {await r.text()}")

async def main():
    # Optional filters via env
    only_symbol = os.environ.get("REPORT_SYMBOL")   # e.g. "BTCUSDT"
    where = {}
    if only_symbol:
        # RPC filter via rest/v1: select=...&symbol=eq.BTCUSDT
        where["symbol"] = f"eq.{only_symbol}"

    rows = await fetch_all("v_signal_perf", select="symbol,horizon_m,score,ret,max_fav,max_adv,triggers", where=where)
    if not rows:
        await post_discord("📊 Report: no rows in `v_signal_perf` yet.")
        return

    sym_rows, trig_rows, score_rows = agg_perf(rows)

    msg = []
    msg.append("📊 **Sniper Performance** (from Supabase)")

    msg.append(fmt_table("By Symbol × Horizon", sym_rows, "symbol | horizon"))
    msg.append(fmt_table("By Trigger × Horizon", trig_rows, "trigger | horizon"))
    msg.append(fmt_table("By Score Bucket × Horizon", score_rows, "score~ | horizon"))

    await post_discord("\n\n".join(msg))

if __name__ == "__main__":
    asyncio.run(main())
