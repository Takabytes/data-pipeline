from datetime import datetime
import pandas as pd

def get_age(date: str):
    birth = datetime.strptime(date.replace('-', '/'), '%Y/%m/%d').date()
    current = datetime.strptime("2023/02/03", '%Y/%m/%d').date()
    age = current.year - birth.year
    if current.month < birth.month:
        age -= 1
    return age

def generate_convictions():
    df_identities = pd.read_csv('../data/identities.csv')
    df_infractions = pd.read_json('../data/infractions.json')
    df_infractions = df_infractions.T.reset_index(drop=True)
    uniq = list(df_infractions['Nom'].unique())
    is_sentenced = lambda x: 1 if x in uniq else 0
    df_identities['Age'] = df_identities['Date_de_naissance'].apply(get_age)
    df_identities['Condamné'] = df_identities['Nom'].apply(is_sentenced)
    return df_identities[['Nom', 'Age', 'Condamné']]

df = generate_convictions()
df.to_csv('../data/convictions.csv', index=False)
