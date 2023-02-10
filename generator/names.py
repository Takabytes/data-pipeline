from faker import Faker
import csv
from generators import *
import random
from datetime import date

print("""Welcome to Fake Data Generator!!!\nYou can generate fake data like name, username, address, and email also the fake datas is saved as CSV file.""")

print()

total_generate = int(input("How many data you'd like to generate? \n >>> "))

print()
crimes_dict = generate_crime('crimes.txt')
identity_name = 'identities.csv'
fake = Faker()
fake = Faker(locale="fr_FR")
operators = ['Airtel', 'Télécel', 'Orange']
lines = open('adresses.csv').read().splitlines()
with open (f'{identity_name}', 'w', newline='', encoding='UTF8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(('ID', 'Name', 'Address', 'E-mail', 'Date-of-birth', 'Phone-number', 'Job'))
    for i in range(total_generate):
        id = i
        f_name = fake.unique.name()
        firstname = f_name.split()[0]
        if (firstname[len(firstname) - 1] in 'aeiou'):
            sexe = 'Féminin'
        else:
            sexe = 'Masculin'
        f_add = generate_address(lines).replace('\n',', ')
        birthdate = fake.date_between_dates(date_start=datetime(1930,1,1), date_end=datetime(2022,12,31))
        today = date.today()
        age = today.year - birthdate.year - ((today.month, today.day) < (birthdate.month, birthdate.day))
        if (age > 18):
            uname = f_name.lower().replace(' ','') 
            email = uname + '@gmail.com'
            phone = generate_phone_number().replace('\n',', ')
            job = fake.job()
            generate_criminal_record(f_name, age, crimes_dict)
            if (age >= 70):
                n = random.randint(0, 50)
                generate_moves(f_name, n)
            elif (19 < age <= 25):
                n = random.randint(50, 100)
                generate_moves(f_name, n)
            elif (25 < age <= 50):
                n = random.randint(150, 200)
                generate_moves(f_name, n)
            elif (50 < age < 70):
                n = random.randint(100, 150)
                generate_moves(f_name, n)
        else:
            email = 'none'
            phone = 'none'
            job = 'none'
        I = id, f_name, sexe, f_add, email, birthdate, phone, job
        writer.writerow(I)

df = pandas.read_csv("all_moves.csv", index_col='Dates')
df = df.sort_values(by=['Dates'], ascending=True)
df.to_csv("all_moves.csv")
convert_to_json('all_crimes.csv')