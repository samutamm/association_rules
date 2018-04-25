
import pymysql.cursors
import pymysql

import pandas as pd
import numpy as np

def create_connection():
    return pymysql.connect(host='localhost',
                             user='root',
                             password='',
                             db='EconomieStat',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

def get_lines():
    connection = create_connection()
    try:
        with connection.cursor() as cursor:
            # Create a new record
            query = "select p.nom, p.continent, cho.chomageCat, dl.level, e.deficit, d.annee, c.nbMeurtre from EconomieStat as e \
                        inner join Pays as p on e.idPays = p.idPays \
                        inner join Date as d on e.idDate = d.idDate \
                        inner join CrimeStat as c on e.idPays = c.idPays and d.idDate = c.idDate \
                        inner join Chomage as cho on cho.idChomage = e.idChomage \
                        inner join DrugLevel as dl on dl.idDrug = c.idDrug;"
            cursor.execute(query)
            result = [row for row in cursor]
            return result

        # connection is not autocommit by default. So you must commit to save
        # your changes.
        connection.commit()
    finally:
        connection.close()
        
lines = get_lines()
df = pd.DataFrame(lines)

chomage = pd.get_dummies(df['chomageCat'], prefix='_chomagec_cat')
drug_level = pd.get_dummies(df['level'], prefix='_drug_level')
deficit = pd.get_dummies(pd.cut(df['deficit'],3), prefix='_deficit')
meurtre = pd.get_dummies(pd.cut(df['nbMeurtre'],3), prefix='_nb_meurtre')
df = pd.concat([df, chomage, drug_level, deficit, meurtre], axis=1)

grouped_df = df.groupby(['continent', 'annee'])

def combine_continent(group):
    row = [group[col].max() for col in group.columns if col.startswith('_')]
    row.append(group['continent'].iloc[0])
    #row_dict['continent'] = 
    return row

columns = [col for col in df.columns if col.startswith('_')] + ['Continent']
lines = []
for key, item in grouped_df:
    lines.append(combine_continent(grouped_df.get_group(key)))
    
final_df = pd.DataFrame.from_records(lines, columns=columns)
final_df.to_csv('association_data.csv')