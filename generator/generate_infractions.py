from faker import Faker
import json
import random
from datetime import datetime, date
import pandas as pd

def infractions_from_file(filename):
    infractions_dict = {}
    content_l = open(filename).read().lower().split('\n')
    content_l = list(map(lambda x: x.capitalize(), content_l))
    delit_idx, th_idx = content_l.index('Délits'), content_l.index('Contraventions')
    crime_l = content_l[1:delit_idx]
    delit_l = content_l[(delit_idx + 1): th_idx]
    contra_l = content_l[(th_idx + 1):]
    infractions_dict = {'Crimes': crime_l, 'Délits': delit_l, 'Contraventions': contra_l}
    return infractions_dict

def select_infraction(infractions_dict):
    infrac_types = tuple(infractions_dict.keys())
    infrac_type = random.choice(infrac_types)
    infracs = infractions_dict.get(infrac_type)
    infrac = random.choice(infracs)
    return [infrac_type, infrac]

def generate_someone_records(name, age, infractions_dict):
    fake = Faker(locale='fr_FR')
    nbr, i = 0, 0
    names, ages, infractionst, infractions, infraction_dates = [], [], [], [], []
    if 50 < age < 70:
        nbr = random.randint(0, 8)
    elif 25 < age <= 50:
        nbr = random.randint(0, 10)
    elif 19 < age <= 25:
        nbr = random.randint(0, 5)
    elif age >= 70:
        nbr = random.randint(0, 3)
    while (i < nbr):
        infraction_l = select_infraction(infractions_dict)
        infraction = infraction_l[1]
        infraction_type = infraction_l[0]
        dateminyear = datetime(2022, 12, 31).year - age + 19
        datemin = date(year=dateminyear, month=1, day=1)
        infraction_date = fake.date_between_dates(date_start=datemin,
                                             date_end=datetime(2022, 12, 31))
        names.append(name), ages.append(age), infractions.append(infraction)
        infractionst.append(infraction_type), infraction_dates.append(infraction_date)
        i += 1
    df = pd.DataFrame({'Nom': names, 'Age': ages, 'Type_d_infraction': infractionst,
                       'Infraction': infractions, 'Date_d_infraction': infraction_dates})
    return df

def generate_infractions():
    infractions_dict = infractions_from_file('crimes.txt')
    df_identities = pd.read_csv('../data/identities.csv')
    shorter = lambda x: 2022 - int(x[:4])
    df_identities['Age'] = df_identities['Date_de_naissance'].apply(shorter)
    df = pd.DataFrame()
    for i, row in df_identities.iterrows():
        if row['Age'] > 18:
            df_one = generate_someone_records(row['Nom'], row['Age'], infractions_dict)
            if df.empty:
                df = df_one
            else:
                df = pd.concat([df, df_one])
    return df

def convert_to_json(csv_file):
    df = pd.read_csv(csv_file)
    df['Nom2'] = df['Nom'].copy()
    result = df.set_index('Nom2').T.to_dict()
    with open('../data/infractions.json', 'w') as f:
        json.dump(result, f, indent=2)

df = generate_infractions()
df.to_csv('../data/infractions.csv', index=False)
convert_to_json('../data/infractions.csv')
