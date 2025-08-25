# app/tools/report.py
import os, asyncio, aiohttp, math

BASE = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1"
KEY  = os.environ["SUPABASE_KEY"]
HEAD = {"apikey": KEY, "Authorization": f"Bearer {KEY}"}

async def fetch_all(table, select="*", limit=10000):
    out = []; from_ = 0
    async with aiohttp.ClientSession() as s:
        while True:
            params = {"select": select}
            headers = dict(HEAD); headers["Range-Unit"]="items"
            headers["Range"]=f"{from_}-{from_+limit-1}"
            async with s.get(f"{BASE}/{table}", headers=headers, params=params) as r:
                rows = await r.json()
                if not rows: break
                out.extend(rows)
                if len(rows) < limit: break
                from_ += limit
    return out

def group(rows, key_fn):
    d = {}
    for r in rows:
        k = key_fn(r); d.setdefault(k, []).append(r)
    return d

async def main():
    rows = await fetch_all("v_signal_perf",
        select="symbol,horizon_m,score,ret,max_fav,max_adv")
    if not rows:
        print("No rows in v_signal_perf yet."); return

    # 1) Perf by symbol & horizon
    by_sh = group(rows, lambda r:(r["symbol"], r["horizon_m"]))
    print("\n=== Performance by Symbol & Horizon ===")
    for (sym,h), rs in sorted(by_sh.items()):
        n = len(rs)
        win = sum(1 for r in rs if (r["ret"] or 0) > 0)
        exp = sum(r["ret"] or 0 for r in rs)/n
        mfe = sum(r["max_fav"] or 0 for r in rs)/n
        mae = sum(r["max_adv"] or 0 for r in rs)/n
        print(f"{sym:10s} h={h:2d} | n={n:4d} winrate={win/n:0.3f} exp={exp:0.6f} mfe={mfe:0.6f} mae={mae:0.6f}")

    # 2) Score→expectancy
    by_bucket = group(rows, lambda r:(round((r["score"] or 0),1), r["horizon_m"]))
    print("\n=== Expectancy by Score Bucket ===")
    for (b,h), rs in sorted(by_bucket.items()):
        n = len(rs); exp = sum(r["ret"] or 0 for r in rs)/n
        print(f"score~{b:4.1f} h={h:2d} | n={n:4d} exp={exp:0.6f}")

if __name__ == "__main__":
    asyncio.run(main())
