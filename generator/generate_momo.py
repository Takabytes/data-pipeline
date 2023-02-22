from faker import Faker
from datetime import datetime
import pandas as pd
import random
import os

def get_good_df() -> pd.DataFrame:
    df = pd.read_csv('../data/identities.csv')
    df = df[df['Numéro_de_téléphone'] != None]
    mtn = ['50', '51', '52', '53', '54', '56', '57', '59'
           , '61', '62', '66', '67', '69', '90', '91', '96', '97']
    moov = ['64', '65', '68', '94', '95', '98', '99']
    indicator = lambda num: num[4:6]
    network = lambda ind: 'mtn' if ind in mtn else 'moov'
    df['indicator'] = df['Numéro_de_téléphone'].apply(str).apply(indicator)
    df['operator'] = df['indicator'].apply(network)
    df = df.drop(columns=['indicator'])
    return df

def create_proportion(df: pd.DataFrame) -> pd.DataFrame:
    assert len(df) == 100
    ndf = df.sample(frac=1).reset_index(drop=True)
    ltail, mid, rtail = ndf[:30].copy(), ndf[30:50].copy(), ndf[50:].copy()
    ltail['nbr_transac'] = [random.randint(0, 5) for i in range(30)]
    mid['nbr_transac'] = [random.randint(21, 30) for i in range(20)]
    rtail['nbr_transac'] = [random.randint(6, 20) for i in range(50)]
    new_df = pd.concat([ltail, mid, rtail])
    return new_df

def generate_momo(df: pd.DataFrame, operator: str) -> pd.DataFrame:
    fake = Faker(locale='fr_FR')
    new_sample = df.sample(n=100, random_state=1)
    transac_type = ['CASH_IN', 'CASH_OUT', 'TRANSFER']
    agences = [f'Agence_{operator}_{city}'
               for city in ['Abomey', 'Cotonou', 'Ouidah']]
    agences_map = dict(zip(agences, [300_000]*3))
    solde_range = [i for i in range(2000, 2_000_000, 100)]
    result_df = pd.DataFrame()

    for i in range(12):
        new_df = create_proportion(new_sample)
        report = dict(zip(new_df.Nom, new_df.nbr_transac))
        for key in list(report.keys()):
            report[key] = [report[key], random.choice(solde_range)]

        for idx, row in new_df.iterrows():
            if report[row['Nom']][0] == 0 or report[row['Nom']][1] == 0:
                continue
            name = row['Nom']
            dates = [fake.date_between_dates(date_start=datetime(2022, i+1, 1),
                                             date_end=datetime(2022, i+1, 28))
                     for k in range(row['nbr_transac'])]
            dates.sort()
            for d in dates:
                element = {'Type': [None], 'Montant': [None], 'Nom_src': [None],
                           'Ancien_solde_src': [None], 'Nouveau_solde_src': [None],
                           'Nom_dest': [None], 'Ancien_solde_dest': [None],
                           'Nouveau_solde_dest': None, 'Date': [None]}

                transac_nbrs = [int(report[key][0]) for key in report.keys() if key != name]
                transac_soldes = [int(report[key][1]) for key in report.keys() if key != name]
                if sum(transac_nbrs) == 0 or sum(transac_soldes) == 0:
                    element['Type'] = ['CASH_OUT']
                else:
                    element['Type'] = [random.choice(transac_type)]

                if report[name][1] <= 100:
                    element['Type'] = ['CASH_IN']

                if element['Type'] == ['CASH_IN']:
                    element['Nom_src'] = [random.choice(list(agences_map.keys()))]
                    element['Nom_dest'] = [name]
                    element['Montant'] = [random.randint(100, agences_map[element['Nom_src'][0]])]
                    element['Ancien_solde_src'] = [agences_map[element['Nom_src'][0]]]
                    element['Nouveau_solde_src'] = [agences_map[element['Nom_src'][0]] - element['Montant'][0]]
                    element['Ancien_solde_dest'] = [report[name][1]]
                    element['Nouveau_solde_dest'] = [report[name][1] + element['Montant'][0]]
                    element['Date'] = [d]
                    report[name][0] -= 1
                    report[name][1] = element['Nouveau_solde_dest'][0]

                if element['Type'] == ['CASH_OUT']:
                    element['Nom_src'] = [name]
                    element['Nom_dest'] = [random.choice(list(agences_map.keys()))]
                    element['Montant'] = [random.randint(100, report[name][1])]
                    element['Ancien_solde_src'] = [report[name][1]]
                    element['Nouveau_solde_src'] = [report[name][1] - element['Montant'][0]]
                    element['Ancien_solde_dest'] = [agences_map[element['Nom_dest'][0]]]
                    element['Nouveau_solde_dest'] =  [agences_map[element['Nom_dest'][0]] - element['Montant'][0]]
                    element['Date'] = [d]
                    report[name][0] -= 1
                    report[name][1] = element['Nouveau_solde_src'][0]

                if element['Type'] == ['TRANSFER']:
                    is_sender = random.choice([True, False])
                    valid_people = [key for key in list(report.keys())
                                    if report[key][0] >= 1 and report[key][1] >= 100]

                    if valid_people == []:
                        continue

                    one_person = random.choice(valid_people)
                    if is_sender:
                        element['Nom_src'] = [name]
                        element['Nom_dest'] = [one_person]
                        element['Montant'] = [random.randint(100, report[name][1])]
                        element['Ancien_solde_src'] = [report[name][1]]
                        element['Nouveau_solde_src'] = [report[name][1] - element['Montant'][0]]
                        element['Ancien_solde_dest'] = [report[one_person][1]]
                        element['Nouveau_solde_dest'] =  [report[one_person][1] - element['Montant'][0]]
                        element['Date'] = [d]
                        report[name][0] -= 1
                        report[name][1] = element['Nouveau_solde_src'][0]
                        report[one_person][0] -= 1
                        report[one_person][1] = element['Nouveau_solde_dest'][0]

                    else:
                        element['Nom_src'] = [one_person]
                        element['Nom_dest'] = [name]
                        element['Montant'] = [random.randint(100, report[one_person][1])]
                        element['Ancien_solde_src'] = [report[one_person][1]]
                        element['Nouveau_solde_src'] = [report[one_person][1] - element['Montant'][0]]
                        element['Ancien_solde_dest'] = [report[name][1]]
                        element['Nouveau_solde_dest'] =  [report[name][1] + element['Montant'][0]]
                        element['Date'] = [d]
                        report[name][0] -= 1
                        report[name][1] = element['Nouveau_solde_dest'][0]
                        report[one_person][0] -= 1
                        report[one_person][1] = element['Nouveau_solde_src'][0]

                if len(result_df) == 0:
                    result_df = pd.DataFrame(element)
                else:
                    result_df = pd.concat([result_df, pd.DataFrame(element)],
                                          ignore_index=True)

    return result_df

df = get_good_df()
df_moov = generate_momo(df.query("operator == 'moov'"), 'moov')
df_mtn = generate_momo(df.query("operator == 'mtn'"), 'mtn')
df_moov.to_csv('../data/momo_moov.csv', index=False)
df_mtn.to_csv('../data/momo_mtn.csv', index=False)
