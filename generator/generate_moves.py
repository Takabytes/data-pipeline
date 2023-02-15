from faker import Faker
import pandas as pd
import random
from datetime import datetime

def generate_someone_moves(name, n):
    i = 0
    fake = Faker(locale="fr_FR")
    frontieres = ['Burkina-Faso', 'Nigéria', 'Niger', 'Togo', 'Aéroport de Cotonou']
    move_types = ['Entrée', 'Sortie']
    documents = ['Passeport', 'Carte nationale dIdentité']
    docs_numbers = [[random.randint(0, 9) for i in range(1, 9)],
                    [random.randint(0, 9) for i in range(1, 9)]]
    df = pd.DataFrame()
    names, moves, typdocs, usedocs, numdocs, srcs, dests, dates = [], [], [], [], [], [], [], []
    while i < n:
        document = random.choice(documents)
        if document == 'Passeport':
            doc_num = docs_numbers[0]
        else:
            doc_num = docs_numbers[1]
        frontiere = random.choice(frontieres)
        move = random.choice(move_types)
        if move == 'Entrée':
            destination = 'Bénin'
        elif 'Aéroport' in frontiere and move != 'Entrée':
            destination = fake.country()
        elif 'Aéroport' not in frontiere and move != 'Entrée':
            destination = frontiere
        date = fake.date_between_dates(date_start=datetime(2012,1,1), date_end=datetime(2022,12,31))
        doc_num_int = int(''.join(str(elem) for elem in doc_num))
        names.append(name), moves.append(move), usedocs.append(document), numdocs.append(doc_num_int)
        srcs.append(frontiere), dests.append(destination), dates.append(date)
        i += 1
    df = pd.DataFrame({'Nom': names, 'Type': moves, 'Document_utilisé': usedocs,
                       'Numéro_du_document': numdocs, 'Provenance': srcs,
                       'Destination': dests, 'Date': dates})
    return df

def generate_moves():
    df_identities = pd.read_csv('../data/identities.csv')
    shorter = lambda x: 2022 - int(x[:4])
    df_identities['Age'] = df_identities['Date_de_naissance'].apply(shorter)
    df = pd.DataFrame()
    n = 0
    for i, row in df_identities.iterrows():
        age = row['Age']
        if age >= 70:
            n = random.randint(0, 50)
        elif 19 < age <= 25:
            n = random.randint(50, 100)
        elif 25 < age <= 50:
            n = random.randint(150, 200)
        elif 50 < age < 70:
            n = random.randint(100, 150)
        df_one = generate_someone_moves(row['Nom'], n)
        if df.empty:
            df = df_one
        else:
            df = pd.concat([df, df_one])
    df = df.sort_values(by=['Date'], ascending=True)
    return df

df = generate_moves()
df.to_csv('../data/moves.csv', index=False)
