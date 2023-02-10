#!/usr/bin/env python3

from faker import Faker
import csv
import random
from datetime import datetime
import pandas
from itertools import permutations 
from random import shuffle
from datetime import date


def generate_moves(name, n):
    fake = Faker()
    fake = Faker(locale="fr_FR")
    movesfile = "all_moves.csv"
    frontieres = ['Burikina-Faso', 'Nigéria', 'Niger', 'Togo', 'Aéroport de Cotonou']
    moves = ['Entrée', 'Sortie']
    documents = ['Passeport', 'Carte nationale dIdentité']
    docs_numbers = []
    l = []
    s = []
    with open (f'{movesfile}', 'a', newline='', encoding='UTF8') as csvfile:
        writer = csv.writer(csvfile)
        for i in range(1, 9):
            l.append(random.randint(0, 9))
        for i in range(1, 9):
            s.append(random.randint(0, 9))
        docs_numbers.append(l)
        docs_numbers.append(s)
        i = 0
        while (i < n):
            document = random.choice(documents)
            if (document == "Passeport"):
                doc_num = docs_numbers[0]
            else:
                doc_num = docs_numbers[1]
            frontiere = random.choice(frontieres)
            move = random.choice(moves)
            if (move == "Entrée"):
                destination = "Bénin"
            elif ('Aéroport' in frontiere and move != "Entrée"):
                destination = fake.country()
            elif ('Aéroport' not in frontiere and move != "Entrée"):
                destination = frontiere
            date = fake.date_between_dates(date_start=datetime(2012,1,1), date_end=datetime(2022,12,31))
            arr = ''.join(str(elem) for elem in doc_num)
            M = name, date, move, document, arr, frontiere, destination
            writer.writerow(M)
            i = i + 1

def generate_phone_number():
    l = []
    operators = ['mtn', 'moov', 'celtis']
    mtn = ['50', '51', '52', '53', '54', '56', '57', '59', '61', '62', '66', '67', '69', '90', '91', '96', '97']
    moov = ['95', '64']
    operator = random.choice(operators)
    if (operator == 'mtn'):
        prefix = random.choice(mtn)
    else:
        prefix = random.choice(moov)
    num = '+229' + prefix
    for i in range(1, 7):
        l.append(random.randint(0, 9))
    arr = ''.join(str(elem) for elem in l)
    num = num + arr
    return(num)


def generate_square_number():
    l = []
    for i in range(1, 5):
        l.append(random.randint(0, 9))
    arr = ''.join(str(elem) for elem in l)
    arr = ' carré ' + arr
    return(str(arr))

def generate_address(lines):
    address =random.choice(lines) + generate_square_number()
    return(address)

def generate_criminal_record(f_name, age, crimes_dict):
    fake = Faker()
    fake = Faker(locale="fr_FR")
    nbr = 0
    i = 0
    crimesfile = "all_crimes.csv"
    if (50 < age < 70):
        nbr = random.randint(0, 8)
    elif (25 < age <= 50):
        nbr = random.randint(0, 10)
    elif (19 < age <= 25):
        nbr = random.randint(0, 5)
    elif (age >= 70):
        nbr = random.randint(0, 3)
    with open (f'{crimesfile}', 'a', newline='', encoding='UTF8') as csvfile:
        writer = csv.writer(csvfile)
        while (i < nbr):
            crime_l = select_crime(crimes_dict)
            crime = crime_l[1]
            crime_type = crime_l[0]
            today = date.today()
            dateminyear = today.year - age + 19
            datemin = date(year=dateminyear, month=1, day=1)
            crime_date = fake.date_between_dates(date_start=datemin, date_end=today)
            C = f_name, crime, crime_type, crime_date, age
            writer.writerow(C)
            i = i + 1

def convert_to_json(csv_file):
    import csv
    import json 
    json_file = 'all_crimes.json'
    my_json = {}
    with open(csv_file, 'r') as fobj:
        reader = csv.DictReader(fobj)
        for row in reader:
            key = row['Name']
            my_json[key] = row 
    with open(json_file,'w') as fobj:
        fobj.write(json.dumps(my_json, indent=2))
    return(my_json)

def generate_crime(file):
    mydict = {}
    content_l = open(file).read().lower().split('\n')
    content_l = list(map(lambda x: x.capitalize(), content_l))
    delit_idx, th_idx = content_l.index('Délits'), content_l.index('Contraventions')
    crime_l = content_l[1:delit_idx]
    delit_l = content_l[(delit_idx + 1): th_idx]
    th_l = content_l[(th_idx + 1):]
    mydict = {'Crimes':crime_l, 'Délits': delit_l, 'Contraventions': th_l}
    return (mydict)

def select_crime(mydict):
    ak= tuple(mydict.keys())
    rk=random.choice(ak)
    vrk=mydict.get(rk)
    rvrk=random.choice(vrk)
    l = []
    l.append(rk)
    l.append(rvrk)
    return (l)

if __name__ == "__main__":
    select_crime(generate_crime('crimes'))

