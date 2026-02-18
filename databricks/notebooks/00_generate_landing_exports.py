from datetime import date, timedelta
import pandas as pd
import random

CATALOG = "dbw_policy_lakehouse_dev_v1"     
SCHEMA  = "default"
VOLUME  = "vol_landing"

landing_base = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

states = ["TX","CA","FL","NY","IL"]
products = ["AUTO","HOME","RENTERS"]
pay_methods = ["ACH","CC","CHECK"]

start = date.today() - timedelta(days=7)   # keep it small/cheap (7 days)

def write_csv(df_spark, out_path):
    (df_spark
        .coalesce(1)
        .write.mode("overwrite")
        .option("header","true")
        .csv(out_path))

for i in range(7):
    d = start + timedelta(days=i)
    runDate = d.isoformat()

    # policies
    policies = []
    for idx in range(50):  # tiny
        policy_id = f"P{10000 + idx}"
        eff = runDate
        premium = round(random.uniform(200, 2500), 2)
        status = random.choice(["Active","Active","Active","Cancelled"])
        policies.append([policy_id, random.choice(products), random.choice(states), eff, premium, status, runDate])

    df_p = spark.createDataFrame(pd.DataFrame(
        policies,
        columns=["policy_id","product","state","effective_date","written_premium","status","runDate"]
    ))
    write_csv(df_p, f"{landing_base}/policies/runDate={runDate}/")

    # policyholders
    holders = []
    for h in range(20):
        holder_id = f"H{5000 + h}"
        policy_id = f"P{10000 + random.randint(0,49)}"
        name = f"Holder_{h}"
        addr = f"{random.randint(1,999)} Main St"
        city = random.choice(["Houston","Austin","Dallas","Miami","Orlando","LA","NYC"])
        st = random.choice(states)
        zipc = str(random.randint(70000, 99999))
        holders.append([holder_id, policy_id, name, addr, city, st, zipc, runDate])

    df_h = spark.createDataFrame(pd.DataFrame(
        holders,
        columns=["holder_id","policy_id","full_name","address","city","state","zip","runDate"]
    ))
    write_csv(df_h, f"{landing_base}/policyholders/runDate={runDate}/")

    # payments
    payments = []
    for p in range(60):
        payment_id = f"PMT{runDate.replace('-','')}{p:03d}"
        policy_id = f"P{10000 + random.randint(0,49)}"
        amt = round(random.uniform(25, 350), 2)
        pay_dt = runDate
        method = random.choice(pay_methods)
        payments.append([payment_id, policy_id, amt, pay_dt, method, runDate])

    df_pay = spark.createDataFrame(pd.DataFrame(
        payments,
        columns=["payment_id","policy_id","amount","payment_date","method","runDate"]
    ))
    write_csv(df_pay, f"{landing_base}/payments/runDate={runDate}/")

print("✅ Landing exports generated under:", landing_base)
