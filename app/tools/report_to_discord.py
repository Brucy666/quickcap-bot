# app/tools/report_to_discord.py
import os, asyncio, aiohttp, json
from collections import defaultdict

BASE = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1"
KEY  = os.environ["SUPABASE_KEY"]
HEAD = {"apikey": KEY, "Authorization": f"Bearer {KEY}"}
DISCORD_PERF_WEBHOOK = os.environ.get("DISCORD_PERF_WEBHOOK","").strip()
REPORT_SYMBOL = os.environ.get("REPORT_SYMBOL","").strip()

async def fetch_all(table, select="*", where: dict | None = None, limit=10000):
    rows, start = [], 0
    async with aiohttp.ClientSession() as s:
        while True:
            params = {"select": select}
            if where: params.update(where)
            headers = dict(HEAD); headers["Range-Unit"]="items"; headers["Range"]=f"{start}-{start+limit-1}"
            async with s.get(f"{BASE}/{table}", headers=headers, params=params) as r:
                if r.status >= 400: raise RuntimeError(f"GET {table} {r.status}: {await r.text()}")
                page = await r.json()
                if not page: break
                rows.extend(page)
                if len(page) < limit: break
                start += limit
    return rows

def summarize(rows):
    by_sym_h = defaultdict(list); by_trig_h = defaultdict(list); by_score_h = defaultdict(list)
    for r in rows:
        sym, h, ret = r["symbol"], r["horizon_m"], r.get("ret") or 0.0
        by_sym_h[(sym,h)].append(r)
        for t in (r.get("triggers") or []): by_trig_h[(t,h)].append(r)
        b = round((r.get("score") or 0.0),1); by_score_h[(b,h)].append(r)

    def agg(group):
        out=[]
        for k,lst in sorted(group.items()):
            n=len(lst); win=sum(1 for x in lst if (x.get("ret") or 0)>0)
            exp=sum((x.get("ret") or 0.0) for x in lst)/max(1,n)
            mfe=sum((x.get("max_fav") or 0.0) for x in lst)/max(1,n)
            mae=sum((x.get("max_adv") or 0.0) for x in lst)/max(1,n)
            out.append((k,n,win/max(1,n),exp,mfe,mae))
        return out
    return agg(by_sym_h), agg(by_trig_h), agg(by_score_h)

def fmt_table(title, rows, keylbl):
    lines=[f"**{title}**"]
    if not rows: lines.append("_no rows_"); return "\n".join(lines)
    lines.append(f"`{keylbl:26s}  n    win   exp       mfe       mae`")
    for (k,n,win,exp,mfe,mae) in rows[:25]:
        lbl = " | ".join(map(str,k)) if isinstance(k,tuple) else str(k)
        lines.append(f"`{lbl[:26]:26s}  {n:4d}  {win:0.2f}  {exp:0.6f}  {mfe:0.6f}  {mae:0.6f}`")
    return "\n".join(lines)

async def post_discord(content: str):
    if not DISCORD_PERF_WEBHOOK:
        print(content); return
    async with aiohttp.ClientSession() as s:
        async with s.post(DISCORD_PERF_WEBHOOK, json={"content": content[:1990]}) as r:
            if r.status>=300: raise RuntimeError(f"Discord post failed {r.status}: {await r.text()}")

async def main():
    where = {"symbol": f"eq.{REPORT_SYMBOL}"} if REPORT_SYMBOL else None
    rows = await fetch_all("v_signal_perf", select="symbol,horizon_m,score,ret,max_fav,max_adv,triggers", where=where)
    if not rows:
        await post_discord("📊 Report: no rows in `v_signal_perf` yet."); return
    sym, trig, score = summarize(rows)
    msg = []
    msg.append("📊 **Sniper Performance**")
    msg.append(fmt_table("By Symbol × Horizon", sym, "symbol | horizon"))
    msg.append(fmt_table("By Trigger × Horizon", trig, "trigger | horizon"))
    msg.append(fmt_table("By Score Bucket × Horizon", score, "score~ | horizon"))
    await post_discord("\n\n".join(msg))

if __name__ == "__main__":
    asyncio.run(main())
