import glob
import pandas as pd

f = glob.glob("data/*RH*.xlsx")[0]
df = pd.read_excel(f)
print("COLUMNS:", list(df.columns))
print(df["Moyen de déplacement"].value_counts())
