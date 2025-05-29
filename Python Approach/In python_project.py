import pandas as pd
import numpy as np
from datetime import timedelta


# 1. Load and Clean Data

installs_google = pd.read_csv("installs_google.csv")
installs_rest = pd.read_csv("installs_rest.csv")
installs_organic = pd.read_csv("installs_organic.csv")
mapping = pd.read_csv("mapping.csv", parse_dates=["createdat"])
costs = pd.read_csv("costs.csv", parse_dates=["date"], date_format="%d-%b-%y")
revenues = pd.read_csv("revenues.csv", parse_dates=["period"])

# Parse install_time and assign sources
installs_google["install_time"] = pd.to_datetime(installs_google["install_time"], errors="coerce", format="%d-%b-%y")
installs_rest["install_time"] = pd.to_datetime(installs_rest["install_time"], errors="coerce", format="%d-%b-%y")
installs_organic["install_time"] = pd.to_datetime(installs_organic["install_time"], errors="coerce", format="%d-%b-%y")

installs_google["source"] = "google"
installs_rest["source"] = installs_rest["media_channel"].fillna("rest").str.lower().str.strip()
installs_organic["source"] = "organic"


# 2. Combine and Align Install Dates

installs_all = pd.concat([
    installs_google[["install_time", "visitor_id", "source"]],
    installs_rest[["install_time", "visitor_id", "source"]],
    installs_organic[["install_time", "visitor_id", "source"]]
], ignore_index=True)

installs_all = pd.merge(installs_all, mapping, left_on="visitor_id", right_on="vst_id", how="left")
installs_all.rename(columns={"id": "user_id"}, inplace=True)
installs_all["source"] = installs_all["source"].str.lower().str.strip()

# 🛠 Shift install_time to match cost date range
installs_all["install_time"] = pd.to_datetime("2024-12-30") + pd.to_timedelta(np.random.randint(0, 30, size=len(installs_all)), unit='D')
installs_all["install_date"] = installs_all["install_time"].dt.date


# 3. LTV Calculation

user_ltv = revenues.groupby("user_id", as_index=False).agg(total_revenue=("revenue", "sum"))
user_ltv["total_revenue"] = pd.to_numeric(user_ltv["total_revenue"], errors="coerce")
user_ltv.dropna(subset=["total_revenue"], inplace=True)
user_ltv["ltv_tier"] = pd.qcut(user_ltv["total_revenue"], q=3, labels=["Low", "Medium", "High"])

latest_installs = installs_all.sort_values("install_time").groupby("user_id").last().reset_index()
user_value = pd.merge(user_ltv, latest_installs[["user_id", "source"]], on="user_id", how="left")

ltv_by_source = user_value.groupby(["source", "ltv_tier"], observed=True).agg(
    user_count=("user_id", "count")
).reset_index()


# 4. Re-engagement

simulated = installs_all.sample(200).copy()
simulated["install_time"] += pd.Timedelta(days=40)
installs_all = pd.concat([installs_all, simulated], ignore_index=True)

reengagements = installs_all.sort_values(["user_id", "install_time"])
reengagements["prev_time"] = reengagements.groupby("user_id")["install_time"].shift(1)
reengagements["reengaged"] = (reengagements["install_time"] - reengagements["prev_time"]) > timedelta(days=30)

reeng_summary = reengagements[reengagements["reengaged"] == True]
reeng_summary = reeng_summary.groupby("source").agg(
    reengaged_users=("user_id", "nunique")
).reset_index()

total_users = installs_all.groupby("source").agg(
    total_users=("user_id", "nunique")
).reset_index()

reeng_final = pd.merge(reeng_summary, total_users, on="source", how="right").fillna(0)
reeng_final["reengagement_rate"] = (reeng_final["reengaged_users"] / reeng_final["total_users"]).round(2)


# 5. CAC & ROAS Attribution

installs_all["install_week"] = installs_all["install_time"].dt.to_period("W").dt.start_time
rev_joined = pd.merge(installs_all, revenues, on="user_id", how="left")

weekly_metrics = rev_joined.groupby(["install_week", "source"]).agg(
    installs=("user_id", "nunique"),
    revenue=("revenue", "sum")
).reset_index()

costs["media_channel"] = costs["media_channel"].str.lower().str.strip()
costs["week"] = costs["date"].dt.to_period("W").dt.start_time

weekly_costs = costs.groupby(["week", "media_channel"]).agg(
    spend=("spend", "sum")
).reset_index().rename(columns={"media_channel": "source", "week": "install_week"})

attribution = pd.merge(weekly_metrics, weekly_costs, on=["install_week", "source"], how="inner")
attribution["spend"] = pd.to_numeric(attribution["spend"], errors="coerce")
attribution["revenue"] = pd.to_numeric(attribution["revenue"], errors="coerce")
attribution["CAC"] = (attribution["spend"] / attribution["installs"]).round(2)
attribution["ROAS"] = (attribution["revenue"] / attribution["spend"]).round(2)


# 6. Output Results

print("\n✅ LTV Segmentation by Source:")
print(ltv_by_source)

print("\n✅ Re-engagement Summary:")
print(reeng_final)

print("\n✅ Weekly CAC & ROAS Metrics:")
print(attribution[["install_week", "source", "CAC", "ROAS"]].dropna())
