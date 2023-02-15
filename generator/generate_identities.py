from faker import Faker
import random
import pandas as pd
from datetime import date, datetime

def generate_phone_number():
    operators = ['mtn', 'moov']
    mtn = ['50', '51', '52', '53', '54', '56', '57', '59'
           , '61', '62', '66', '67', '69', '90', '91', '96', '97']
    moov = ['64', '65', '68', '94', '95', '98', '99']
    operator = random.choice(operators)
    if operator == 'mtn':
        prefix = random.choice(mtn)
    else:
        prefix = random.choice(moov)
    num = '+229' + prefix
    l = [random.randint(0, 9) for i in range(1, 7)]
    arr = ''.join(str(elem) for elem in l)
    num = num + arr
    return num

def generate_square_number():
    l = [random.randint(0, 9) for i in range(1, 5)]
    arr = ''.join(str(elem) for elem in l)
    arr = ' carré ' + arr
    return arr

def generate_address(lines):
    return random.choice(lines) + generate_square_number()

def generate_identities(nsamples=1000):
    fake = Faker(locale='fr_FR')
    lines = open('adresses.csv').read().splitlines()
    df = pd.DataFrame()
    df['Nom'] = [fake.unique.name() for i in range(nsamples)]
    df['Date_de_naissance'] = [fake.date_between_dates(date_start=datetime(1930,1,1), date_end=datetime(2022,12,31))
                               for i in range(nsamples)]
    df['Genre'] = df['Nom'].apply(lambda x: 'Féminin' if x[-1] in 'aeiou' else 'Masculin')
    df['Adresse'] = [generate_address(lines).replace('\n', ', ')
                     for i in range(nsamples)]
    df['E-mail'] = df['Nom'].apply(lambda x: x.lower().replace(' ', '') + '@gmail.com')
    df['Numéro_de_téléphone'] = [generate_phone_number().replace('\n',', ') for i in range(nsamples)]
    df['Job'] = [fake.job() for i in range(nsamples)]
    df['Date'] = pd.to_datetime(df['Date_de_naissance'])
    df['Age'] = (df['Date']
                 .apply(lambda x: datetime(2022, 12, 31).year - x.year
                        - ((datetime(2022, 12, 31).month, datetime(2022, 12, 31).day) < (x.month, x.day))))
    df.loc[df['Age'] < 18, ['E-mail', 'Numéro_de_téléphone', 'Job']] = None
    df.drop(columns=['Date'], inplace=True)
    df.drop(columns=['Age'], inplace=True)
    return df

df = generate_identities()
df.to_csv('../data/identities.csv', index=False)
