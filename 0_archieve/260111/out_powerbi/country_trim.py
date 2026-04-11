import pandas as pd

p = pd.read_csv("/home/user/문서/workspace/python/out_powerbi/powerbi_master.csv", sep=None, engine="python", encoding="utf-8-sig")
u = pd.read_csv("/home/user/문서/workspace/python/out_powerbi/UNSD — Methodology.csv", sep=None, engine="python", encoding="utf-8-sig")

powerbi = set(p["country"].dropna().astype(str).str.strip())
unsd = set(u["Country or Area"].dropna().astype(str).str.strip())

missing = sorted(powerbi - unsd)

print("\n".join(missing))
print(f"\nmissing count = {len(missing)}")
