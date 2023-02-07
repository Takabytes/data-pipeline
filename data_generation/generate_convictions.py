from datetime import datetime
import pandas as pd

df_ids = pd.read_csv('../local_data/identities.csv')

df_crimes = pd.read_json('../local_data/all_crimes.json')
df_crimes = df_crimes.T.reset_index(drop=True)
uniq = list(df_crimes['Name'].unique())


def get_age(date: str):
    birth = datetime.strptime(date.replace('-', '/'), '%Y/%m/%d').date()
    current = datetime.strptime("2023/02/03", '%Y/%m/%d').date()
    age = current.year - birth.year
    if current.month < birth.month:
        age -= 1
    return age

def get_condam(name: str):
    return 1 if name in uniq else 0

df_ids['Age'] = df_ids['Date-of-birth'].apply(get_age)
df_ids['is_sentenced'] = df_ids['Name'].apply(get_condam)

df_convictions = df_ids[['Name', 'Age', 'is_sentenced']]
#df_convictions.to_csv('../local_data/convictions.csv', index=False)
