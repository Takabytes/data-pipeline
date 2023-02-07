from faker import Faker
from datetime import datetime
import pandas as pd
import random
import os

def get_good_df():
    df = pd.read_csv('../local_data/identities.csv')
    df = df[df['Phone-number'] != 'none']
    mtn = ['50', '51', '52', '53', '54', '56', '57', '59'
           , '61', '62', '66', '67', '69', '90', '91', '96', '97']
    moov = ['64', '65', '68', '94', '95', '98', '99']
    indicator = lambda num: num[4:6]
    network = lambda ind: 'mtn' if ind in mtn else 'moov'
    df['indicator'] = df['Phone-number'].apply(indicator)
    df['operator'] = df['indicator'].apply(network)
    df = df.drop(columns=['indicator'])
    return df

def create_proportion(df):
    assert len(df) == 100
    ndf = df.sample(frac=1).reset_index(drop=True)
    ltail, mid, rtail = ndf[:30].copy(), ndf[30:50].copy(), ndf[50:].copy()
    ltail['nbr_tra'] = [random.randint(0, 5) for i in range(30)]
    mid['nbr_tra'] = [random.randint(21, 30) for i in range(20)]
    rtail['nbr_tra'] = [random.randint(6, 20) for i in range(50)]
    new_df = pd.concat([ltail, mid, rtail])
    return new_df

def create_sample(df):
    fake = Faker(locale="fr_FR")
    new_sample = df.sample(n=100, random_state=1)
    result_df = pd.DataFrame()
    transac_type = ['CASH_IN', 'CASH_OUT', 'TRANSFER', 'RECEIPT']
    amount_range = [i for i in range(500, 500000, 100)]

    for i in range(12):
        new_df = create_proportion(new_sample)
        report = dict(zip(new_df.Name, new_df.nbr_tra))

        for idx, row in new_df.iterrows():
            if row['nbr_tra'] == 0:
                continue
            name = row['Name']
            dates = [fake.date_between_dates(date_start=datetime(2022, i+1, 1),
                                             date_end=datetime(2022, i+1, 28))
                     for k in range(row['nbr_tra'])]
            dates.sort()
            for d in dates:
                element = {'relative_to': [None], 'sender': [None], 'receiver': [None],
                           'operator': [row['operator']], 'type': [None],
                           'date': [d], 'amount': None}

                amount = random.choice(amount_range)
                element['amount'] = [amount]

                remain = new_df.query(f'Name != "{name}"')
                if remain['nbr_tra'].sum() == 0:
                    element['type'] = ['CASH_OUT']
                else:
                    element['type'] = [random.choice(transac_type)]

                valid_keys = [key for key in list(report.keys()) if report[key] > 0]
                remain_2 = remain.query(f'Name in {valid_keys}')

                if len(remain_2) == 0:
                    break

                if element['type'] == ['TRANSFER']:
                    element['relative_to'] = [name] #Added new
                    element['sender'] = [name]
                    report[name] -= 1
                    one_choice = remain_2.sample(1).reset_index()
                    one_choice_n = one_choice.loc[0, 'Name']
                    element['receiver'] = [one_choice_n]
                    # Add 2nd user information
                    element['relative_to'].append(one_choice_n)
                    element['type'].append('RECEIPT') #Added new
                    element['sender'].append(name)
                    element['receiver'].append(one_choice_n)
                    element['operator'].append(row['operator'])
                    element['date'].append(d)
                    element['amount'].append(amount)
                    report[one_choice_n] -= 1

                if  element['type'] == ['CASH_OUT']:
                    element['relative_to'] = [name]
                    report[name] -= 1

                if  element['type'] == ['CASH_IN']:
                    element['relative_to'] = [name]
                    report[name] -= 1

                if  element['type'] == ['RECEIPT']:
                    element['relative_to'] = [name] #Added new
                    element['receiver'] = [name]
                    report[name] -= 1
                    one_choice = remain_2.sample(1).reset_index()
                    one_choice_n = one_choice.loc[0, 'Name']
                    element['sender'] = [one_choice_n]
                    # Add 2nd user information
                    element['relative_to'].append(one_choice_n) #Added new
                    element['type'].append('TRANSFER')
                    element['sender'].append(one_choice_n)
                    element['receiver'].append(name)
                    element['operator'].append(row['operator'])
                    element['date'].append(d)
                    element['amount'].append(amount)
                    report[one_choice_n] -= 1

                if len(result_df) == 0:
                    result_df = pd.DataFrame(element)
                else:
                    result_df = pd.concat([result_df, pd.DataFrame(element)],
                                          ignore_index=True)
    return (result_df)


"""
df = get_good_df()

df_moov = df.query("operator == 'moov'")
df_mtn = df.query("operator == 'mtn'")

momo_moov = create_sample(df_moov)
momo_mtn = create_sample(df_mtn)

momo_moov.to_csv('../local_data/momo_moov.csv', index=False)
momo_mtn.to_csv('../local_data/momo_mtn.csv', index=False)
"""
