#!/usr/bin/env python
# coding: utf-8

# In[1]:


#basic libraries
import pandas as pd
import numpy as np

#gsheet api module
from Google import Create_Service
import sys
sys.path.append("..")
import gsheet_module as gsh
sys.path.append("Traitement de données")


# ## Data preprocessing

# ## Importing customer Table

# In[2]:


#importing table_client from the drive
#table_client allows completing sales price info (parts_collab et parts_employeurs )


SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1STEmSmj7GNGQbzN8zzAhvaFoeF6y8uXim_lXaEZShiA'
RANGE_NAME = 'Table client'
client_table = gsh.pull_sheet_data(SCOPES,SPREADSHEET_ID,RANGE_NAME)
client_table = pd.DataFrame(client_table[1:], columns=client_table[0])


# ## preprocessing new sales data

# Data import

# In[3]:


#fonction pour remplacer les \ par des / dans le chemin d'accès au fichier de ventes site
#le fichier de vente doit être au format xls

def path(answer):
    raw_answer = r'{}'.format(answer)
    raw_answer = raw_answer.replace('\\','/')
                                                     
    return raw_answer


# In[4]:


#C:\Users\grego\Downloads
#export_panier_2021_03_02_11_45_33


# In[5]:


answer_path = str(input('Please type here path of the file to export: '))


# In[6]:


answer_name = str(input('Please type here the name of the file to export: '))


# In[7]:


#importing the sales file into a pd dataframe
chemin_export = path(answer_path) + '/' + path(answer_name) + '.xls'
df_to_export = pd.read_html(chemin_export)
df_to_export = df_to_export[0]

#removing nan data
df_to_export.replace(np.nan, '', inplace=True)


# Data processing

# In[8]:


#variables of interest
var_to_keep = ['Id','Date','Startup','Code postal','Ville','Mail','Entrée','Plat','Dessert','Prix des suppléments','Total Avoir','Prix']

#bulding the dataframe for the export
clean_df = df_to_export[var_to_keep]

#adding pricing info using the client_table
clean_df = clean_df.merge(client_table[['ENTITES','Prix HT catalogue Entrée + Plat', 'Prix HT catalogue Entrée + Plat + Dessert','Prix HT catalogue Plat + Dessert','Prix HT catalogue Plat seul','Participation HT employeur Entrée + Plat','Participation HT employeur Entrée + Plat + Dessert','Participation HT employeur Plat + Dessert','Participation HT employeur Plat seul' ]], left_on = 'Startup', right_on = 'ENTITES', how = 'left') 

#fonction permettant de determiner le prix collab du menu en fonction du panier client

def part_collab(e, d, part_collab_epd, part_collab_ep, part_collab_pd, part_collab_p):
    if (e == '/' and d == '/'):
        return part_collab_p
    elif (e != '/' and d != '/'):
        return part_collab_epd
    elif (e != '/' and d == '/'):
        return part_collab_ep
    else:
        return part_collab_pd

#fonction permettant de determiner le prix employeur du menu en fonction du panier client

def part_employeur(e, d, part_emp_epd, part_emp_ep, part_emp_pd, part_emp_p):
    if (e == '/' and d == '/'):
        return part_emp_p
    elif (e != '/' and d != '/'):
        return part_emp_epd
    elif (e != '/' and d == '/'):
        return part_emp_ep
    else:
        return part_emp_pd
    
clean_df['Part_collaborateur_HT'] = clean_df.apply(lambda x: part_collab(x['Entrée'], x['Dessert'], x['Prix HT catalogue Entrée + Plat + Dessert'], x['Prix HT catalogue Entrée + Plat'], x['Prix HT catalogue Plat + Dessert'] , x['Prix HT catalogue Plat seul']), axis = 1)
clean_df['Part_employeur_HT'] = clean_df.apply(lambda x: part_employeur(x['Entrée'], x['Dessert'], x['Participation HT employeur Entrée + Plat + Dessert'], x['Participation HT employeur Entrée + Plat'], x['Participation HT employeur Plat + Dessert'] , x['Participation HT employeur Plat seul']), axis = 1)

#removing useless col
clean_df.drop(columns = ['Prix HT catalogue Entrée + Plat', 'Prix HT catalogue Entrée + Plat + Dessert','Prix HT catalogue Plat + Dessert','Prix HT catalogue Plat seul','Participation HT employeur Entrée + Plat','Participation HT employeur Entrée + Plat + Dessert','Participation HT employeur Plat + Dessert','Participation HT employeur Plat seul'], inplace = True)

#converting data types:
clean_df['Part_collaborateur_HT'] = pd.to_numeric(clean_df['Part_collaborateur_HT'],errors='coerce')
clean_df['Part_employeur_HT'] = pd.to_numeric(clean_df['Part_employeur_HT'],errors='coerce')

#replacing nan
clean_df = clean_df.fillna(0)

#Fonction pour identifier les part_collab non trouvées au cours du merge avec la table client:
def check_part_collab(a, b):
    if a + b == 0:
        return 1
    else:
        return 0
#Fonction pour remplacer les part_collab non trouvées par le prix HT
def correct_part_collab(a, b, c):
    if a + b == 0:
        return round(c/1.1,2)
    else:
        return a
    
#clean_df["check_prix"] = clean_df.apply(lambda x: check_part_collab(x['Part_collaborateur_HT'], x['Part_employeur_HT']), axis = 1)
nb_err = clean_df.apply(lambda x: check_part_collab(x['Part_collaborateur_HT'], x['Part_employeur_HT']), axis = 1).sum()
clean_df["Part_collaborateur_HT"] = clean_df.apply(lambda x: correct_part_collab(x['Part_collaborateur_HT'], x['Part_employeur_HT'], x['Prix']), axis = 1)
print("Amount of values not found after merging: ", nb_err)

#on renomme colonnes Entrée/Plat/Dessert par nom_Entrée/nom_Plat/nom_Dessert
new_columns = clean_df.columns.values
new_columns[6] = 'nom_Entrée'
new_columns[7] = 'nom_Plat'
new_columns[8] = 'nom_Dessert'
clean_df.columns  = new_columns

#Creation des colonnes Entrée/Plat/Dessert: 
#Si le panier contien une Entrée: 1 sinon 0
#Si le panier contien un Dessert: 1 sinon 0
#Le panier contenant toujours un Plat : la colonne Plat est uniquement constituée de 1

def bool_panier(col_check):
    if col_check == '/': #si la colonne à checker ne contient pas de produit : 0 sinon 1
        return 0
    else:
        return 1
    
clean_df["Entrée"] = clean_df.apply(lambda x: bool_panier(x['nom_Entrée']), axis = 1)
clean_df["Plat"] = clean_df.apply(lambda x: bool_panier(x['nom_Plat']), axis = 1)
clean_df["Dessert"] = clean_df.apply(lambda x: bool_panier(x['nom_Dessert']), axis = 1)


# In[9]:


clean_df.head(3)


# Exporting the clean sales to gsheet

# In[10]:


#clean_df.columns = clean_df.iloc[0].values
#clean_df = clean_df.iloc[1:]


# In[11]:


CLIENT_SECRET_FILE = 'credentials.json'
API_SERVICE_NAME = 'sheets'
API_VERSION = 'v4'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

gsheetId = '1J5JC_o5fGuE_UzU1W35-ZYA7-agSYqOAMbVW_uycbXo'


# In[12]:


service = Create_Service(CLIENT_SECRET_FILE, API_SERVICE_NAME, API_VERSION, SCOPES)


# In[13]:


#Id du gsheet sur lequel on va exporter les ventes clean du site
#gsheetId = '1eEjwyNih7ekfxZ1tarE0cGMjSmK_UUSxOjbQm4wld8k'
gsheetId = '1J5JC_o5fGuE_UzU1W35-ZYA7-agSYqOAMbVW_uycbXo'

#Exporting data to gsheet
response_data = service.spreadsheets().values().append(
        spreadsheetId=gsheetId,
        valueInputOption='RAW',
        range='A572',
        body=dict(
        majorDimension='ROWS',
        values=clean_df.T.reset_index().T.values.tolist())).execute()


# In[14]:


#values=clean_df.T.reset_index().T.values.tolist()[-1]

