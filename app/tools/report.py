# app/tools/report.py
import os, asyncio, aiohttp, json

BASE = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1"
KEY  = os.environ["SUPABASE_KEY"]
HEAD = {"apikey": KEY, "Authorization": f"Bearer {KEY}"}

async def query(sql: str):
    url = BASE.replace("/rest/v1", "") + "/rest/v1/rpc"
    headers = dict(HEAD)
    headers["Content-Type"] = "application/json"
    async with aiohttp.ClientSession() as sess:
        async with sess.post(url, headers=headers, data=json.dumps({"query": sql})) as r:
            text = await r.text()
            if r.status >= 400:
                raise RuntimeError(f"Supabase SQL error {r.status}: {text}")
            return json.loads(text)

async def main():
    # Performance by symbol & horizon
    sql = """
    select s.symbol, o.horizon_m,
           count(*) as n,
           round(avg(case when o.ret>0 then 1 else 0 end)::numeric,3) as winrate,
           round(avg(o.ret)::numeric,6) as expectancy,
           round(avg(o.max_fav)::numeric,6) as avg_mfe,
           round(avg(o.max_adv)::numeric,6) as avg_mae
    from signal_outcomes o
    join signals s on s.id=o.signal_id
    group by s.symbol, o.horizon_m
    order by s.symbol, o.horizon_m;
    """
    rows = await query(sql)
    print("\n=== Performance by Symbol & Horizon ===")
    for r in rows:
        print(r)

    # By trigger
    sql2 = """
    with x as (
      select s.symbol,o.horizon_m,o.ret,
             jsonb_array_elements_text(s.triggers) as trig
      from signal_outcomes o
      join signals s on s.id=o.signal_id
    )
    select trig,horizon_m,count(*) n,
           round(avg(case when ret>0 then 1 else 0 end)::numeric,3) as winrate,
           round(avg(ret)::numeric,6) as expectancy
    from x
    group by trig,horizon_m
    order by expectancy desc;
    """
    rows2 = await query(sql2)
    print("\n=== Performance by Trigger ===")
    for r in rows2:
        print(r)

    # By score bucket
    sql3 = """
    select round(s.score,1) as score_bucket, o.horizon_m,
           count(*) as n, round(avg(o.ret)::numeric,6) as expectancy
    from signal_outcomes o
    join signals s on s.id=o.signal_id
    group by score_bucket,o.horizon_m
    order by o.horizon_m, score_bucket;
    """
    rows3 = await query(sql3)
    print("\n=== Expectancy by Score Bucket ===")
    for r in rows3:
        print(r)

if __name__ == "__main__":
    asyncio.run(main())
