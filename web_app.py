import pandas as pd
import streamlit as st

csv_matrice = '/Users/alessandromacrina/Documents/Università/Tesi/CSV/matrice_od_2020_passeggeri.csv'
csv_posizione = '/Users/alessandromacrina/Documents/Università/Tesi/CSV/comuni, province e posizione.csv'


################################################################

# SESSION STATE

if 'database' not in  st.session_state:
    st.session_state.database = pd.read_csv(csv_matrice)

if 'province' not in st.session_state:
    st.session_state.province = st.session_state.database['PROV_ORIG'].unique()


################################################################

st.title('Mobilità nella provincia di Varese e Como')

orig = st.sidebar.selectbox('Seleziona provincia di origine',  st.session_state.province)
dest = st.sidebar.selectbox('Seleziona provincia di destinazione', st.session_state.province)

enter=st.sidebar.button('Invio')

if enter:
    st.write('Spostamenti nella tratta ' + orig + ' -> ' + dest + ' divisi per fascia oraria')
    df = st.session_state.database.loc[(st.session_state.database['PROV_ORIG'] == orig) & (st.session_state.database['PROV_DEST'] == dest )]
    result = df.groupby('FASCIA_ORARIA')[["LAV_COND","LAV_PAX","LAV_MOTO","LAV_FERRO",
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
    st.bar_chart(result, x = 'FASCIA_ORARIA', y = 'LAV_COND')
    st.write(result)

