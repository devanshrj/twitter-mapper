import glob
import pandas as pd

txtfiles = []
path = "/home/devanshjain/ssa_india_filters/*.csv"
for file in glob.glob(path):
    if 'Alias' in file:
        continue
    txtfiles.append(file)

for file in txtfiles:
    df = pd.read_csv(file)
    df['name'] = df['name'].str.lower()
    df.drop_duplicates(subset=['name'], inplace=True)
    mod_file = file.replace('ssa_india_filters', 'mod_ssa_india_filters')
    print("Modified file:", file)
    df.to_csv(mod_file, index=False)