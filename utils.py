import pandas as pd
import streamlit as st
import requests
import io



# metodo per creare riferimento al file parquet e creare il dataframe

def leggi_parquet(url):
    response = requests.get(url)
    return pd.read_parquet(io.BytesIO(response.content))


# rimuove i numeri alla fine di una stringa
def remove_number_at_end(df, columns):
   for col in columns:
       df[col] = df[col].str.replace(' \d+$', '', regex=True)
   return df

# metodo per trasformare in minuscolo i valori di una o più colonne di un df
def convert_columns_to_lowercase(df, columns):
   for col in columns:
       df[col] = df[col].str.lower()
   return df

# group by su una o più colonne che somma i valori dei viaggiatori
@st.cache_data
def my_groupby(df, columns):
    return df.groupby(columns)[["LAV_COND","LAV_PAX","LAV_MOTO","LAV_FERRO",
                                                                                    "LAV_GOMMA","LAV_BICI","LAV_PIEDI","LAV_ALTRO",
                                                                                    "STU_COND","STU_PAX","STU_MOTO","STU_FERRO",
                                                                                    "STU_GOMMA","STU_BICI","STU_PIEDI","STU_ALTRO",
                                                                                    "OCC_COND","OCC_PAX","OCC_MOTO","OCC_FERRO",
                                                                                    "OCC_GOMMA","OCC_BICI","OCC_PIEDI","OCC_ALTRO",
                                                                                    "AFF_COND","AFF_PAX","AFF_MOTO","AFF_FERRO",
                                                                                    "AFF_GOMMA","AFF_BICI","AFF_PIEDI","AFF_ALTRO",
                                                                                    "RIT_COND","RIT_PAX","RIT_MOTO","RIT_FERRO",
                                                                                    "RIT_GOMMA","RIT_BICI","RIT_PIEDI",
                                                                                    "RIT_ALTRO"]].sum().reset_index()

# metodo per ottenere la provincia di appartenenza di un comune 
# usabile solo su un df che contiene la colonna comune e provincia
def get_provincia(df, comune):
    return df.loc[df['comune']==comune]['provincia']

def to_string(df):
    return str(df.tolist()).replace("[","").replace("]","").replace("'","")


# @st.cache_data
# def compute_data(result, colonne, hour):
#  selected_columns = result.loc[result['FASCIA_ORARIA'] == hour][colonne]
#  if not selected_columns.empty:
#      return selected_columns.transpose()
#  else:
#      return None

@st.cache_data
def prepare_df(df, motivo):
        if 'Lavoro':
                return df[["LAV_COND","LAV_PAX","LAV_MOTO","LAV_FERRO","LAV_GOMMA","LAV_BICI","LAV_PIEDI","LAV_ALTRO"]].transpose()
        elif 'Studenti':
            return df[["STU_COND","STU_PAX","STU_MOTO","STU_FERRO","STU_GOMMA","STU_BICI","STU_PIEDI","STU_ALTRO"]].transpose()
        elif 'Occasionale':
            return df[["OCC_COND","OCC_PAX","OCC_MOTO","OCC_FERRO","OCC_GOMMA","OCC_BICI","OCC_PIEDI","OCC_ALTRO"]].transpose()
        elif 'Ritorno':
            return df[["RIT_COND","RIT_PAX","RIT_MOTO","RIT_FERRO","RIT_GOMMA","RIT_BICI","RIT_PIEDI","RIT_ALTRO"]].transpose()
        elif 'Affari':
            return df[["AFF_COND","AFF_PAX","AFF_MOTO","AFF_FERRO","AFF_GOMMA","AFF_BICI","AFF_PIEDI","AFF_ALTRO"]].transpose()
        else:
            return None

@st.cache_data
def normalize(df, column):
    df_max_scaled = df.copy()
    df_max_scaled[column] = df_max_scaled[column] / df_max_scaled[column].abs().max()
    return df_max_scaled

@st.cache_data
def hp_threshold(df, column, low_threshold, high_threshold):
    df[column] = df[column].clip(lower=low_threshold, upper=high_threshold)
    return df

# metodo per aggiungere la colonna TOT che contiene la somma di tutte le colonne contenenti dati
@st.cache_data
def calcola_totale(df):
    df['TOT'] = df[["LAV_COND","LAV_PAX","LAV_MOTO","LAV_FERRO",
                                                                                        "LAV_GOMMA","LAV_BICI","LAV_PIEDI","LAV_ALTRO",
                                                                                        "STU_COND","STU_PAX","STU_MOTO","STU_FERRO",
                                                                                        "STU_GOMMA","STU_BICI","STU_PIEDI","STU_ALTRO",
                                                                                        "OCC_COND","OCC_PAX","OCC_MOTO","OCC_FERRO",
                                                                                        "OCC_GOMMA","OCC_BICI","OCC_PIEDI","OCC_ALTRO",
                                                                                        "AFF_COND","AFF_PAX","AFF_MOTO","AFF_FERRO",
                                                                                        "AFF_GOMMA","AFF_BICI","AFF_PIEDI","AFF_ALTRO",
                                                                                        "RIT_COND","RIT_PAX","RIT_MOTO","RIT_FERRO",
                                                                                        "RIT_GOMMA","RIT_BICI","RIT_PIEDI",
                                                                                        "RIT_ALTRO"]].sum(axis=1)
    return df
