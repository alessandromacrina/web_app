import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import plotly.express as px
from utils import *
import numpy as np
import streamlit.components.v1 as components

############################################################

#POSIZIONE CSV

csv_matrice = 'https://drive.google.com/file/d/1OjMV_13wHYdRSHM2hQtOP1uy7iTSExxH/view?usp=share_link'
csv_posizione = 'https://drive.google.com/file/d/1hKqonKviR5QOu5cCNkBJQ7ri9GWmhexE/view?usp=share_link'
csv_tur_prov = 'https://drive.google.com/file/d/1MXekSaQdna_MYrpZS8y8pDMkYXOKP64d/view?usp=share_link'
csv_tur_com = 'https://drive.google.com/file/d/1scr2Vp-yxKelPGfuSDCGH6VTELkqp4g7/view?usp=share_link'

############################################################

colonne = ["LAV_COND","LAV_PAX","LAV_MOTO","LAV_FERRO",
                                                                                    "LAV_GOMMA","LAV_BICI","LAV_PIEDI","LAV_ALTRO",
                                                                                    "STU_COND","STU_PAX","STU_MOTO","STU_FERRO",
                                                                                    "STU_GOMMA","STU_BICI","STU_PIEDI","STU_ALTRO",
                                                                                    "OCC_COND","OCC_PAX","OCC_MOTO","OCC_FERRO",
                                                                                    "OCC_GOMMA","OCC_BICI","OCC_PIEDI","OCC_ALTRO",
                                                                                    "AFF_COND","AFF_PAX","AFF_MOTO","AFF_FERRO",
                                                                                    "AFF_GOMMA","AFF_BICI","AFF_PIEDI","AFF_ALTRO",
                                                                                    "RIT_COND","RIT_PAX","RIT_MOTO","RIT_FERRO",
                                                                                    "RIT_GOMMA","RIT_BICI","RIT_PIEDI",
                                                                                    "RIT_ALTRO"]

hours = ["00:00-00:59", "01:00-01:59", "02:00-02:59", "03:00-03:59", "04:00-04:59", "05:00-05:59",
        "06:00-06:59", "07:00-07:59", "08:00-08:59", "09:00-09:59", "10:00-10:59", "11:00-11:59",
        "12:00-12:59", "13:00-13:59", "14:00-14:59", "15:00-15:59", "16:00-16:59", "17:00-17:59",
        "18:00-18:59", "19:00-19:59", "20:00-20:59", "21:00-21:59", "22:00-22:59", "23:00-23:59"]

mesi = ['Gennaio', 'Febbraio', 'Marzo', 'Aprile', 'Maggio', 'Giugno', 'Luglio', 'Agosto', 'Settembre', 'Ottobre', 'Novembre', 'Dicembre']


regioni_italiane = [
    "ABRUZZO",
    "BASILICATA",
    "CALABRIA",
    "CAMPANIA",
    "EMILIA-ROMAGNA",
    "FRIULI-VENEZIA GIULIA",
    "LAZIO",
    "LIGURIA",
    "LOMBARDIA",
    "MARCHE",
    "MOLISE",
    "PIEMONTE",
    "PUGLIA",
    "SARDEGNA",
    "SICILIA",
    "TOSCANA",
    "TRENTINO-ALTO ADIGE",
    "UMBRIA",
    "VALLE D'AOSTA",
    "VENETO"
]



vuoto = True

################################################################

# SESSION STATE

if 'posizione' not in st.session_state:
    st.session_state.posizione = pd.read_csv(csv_posizione)
    convert_columns_to_lowercase(st.session_state.posizione, ('comune', 'provincia'))

if 'database' not in  st.session_state:
    st.session_state.database = pd.read_csv(csv_matrice)
    remove_number_at_end(st.session_state.database, ('ZONA_ORIG', 'ZONA_DEST'))
    convert_columns_to_lowercase(st.session_state.database, ('ZONA_ORIG', 'ZONA_DEST'))
    st.session_state.database = st.session_state.database.merge(st.session_state.posizione, left_on='ZONA_ORIG', right_on='comune')
    st.session_state.database.drop(['comune', 'provincia'], axis=1, inplace=True)
    st.session_state.database.rename(columns={'latitudine':'lat_orig', 'longitudine':'long_orig'}, inplace=True)
    st.session_state.database = st.session_state.database.merge(st.session_state.posizione, left_on='ZONA_DEST', right_on='comune')
    st.session_state.database.drop(['comune', 'provincia'], axis=1, inplace=True)
    st.session_state.database.rename(columns={'latitudine':'lat_dest', 'longitudine':'long_dest'}, inplace=True)

if 'database_red' not in  st.session_state:
    st.session_state.database_red = st.session_state.database.loc[(st.session_state.database['PROV_ORIG']=='VA') | (st.session_state.database['PROV_ORIG']=='CO') | (st.session_state.database['PROV_DEST']=='VA') | (st.session_state.database['PROV_DEST']=='CO')]
    

if 'comune' not in st.session_state:
    st.session_state.comune = st.session_state.database['ZONA_ORIG'].drop_duplicates().sort_values().str.lower().tolist()

if 'province' not in st.session_state:
    st.session_state.province = st.session_state.database['PROV_ORIG'].drop_duplicates().sort_values().str.lower().tolist()

if 'turisti' not in st.session_state:
    st.session_state.turisti = pd.read_csv(csv_tur_prov)

if 'turisti_comuni' not in st.session_state:
    st.session_state.turisti_comuni = pd.read_csv(csv_tur_com)



################################################################

st.title('Mobilità nella provincia di Varese e Como')
st.markdown('***')
toggle = st.toggle('Visualizza tutta la Lombardia', value=False,)

if toggle:
    flow_map_url = "https://www.flowmap.blue/1Ui8Hbu8iLb7V6EtXUfqQ2GnogVrx-qOfOy5y0a1Fcos"
else:
    flow_map_url = "https://www.flowmap.blue/14jvR16yGZV7D_1TUeR8Ws857LPTvHdToVkewxuVHGkE"

components.iframe(flow_map_url, height=400, width=700)

st.markdown('***')



orig = st.sidebar.selectbox('Seleziona comune di origine',  st.session_state.comune, index=None)
dest = st.sidebar.selectbox('Seleziona comune di destinazione', st.session_state.comune, index=None)
st.sidebar.write('---------------')
prov_tur = st.sidebar.selectbox('Seleziona una provincia per visualizzare i dati relativi sui flussi turistici',  st.session_state.turisti['Provincia'].drop_duplicates().sort_values().tolist(), index=None)
com_tur = st.sidebar.selectbox('Seleziona un comune per visualizzare i dati relativi sui flussi turistici', st.session_state.turisti_comuni['Comune'].drop_duplicates().sort_values().tolist(), index=None)



if ((orig != None) & (dest != None)):
    ## BAR CHART SPOSTAMENTI TOTALI ##

    df = st.session_state.database_red.loc[(st.session_state.database_red['ZONA_ORIG'] == orig) & (st.session_state.database_red['ZONA_DEST'] == dest )]

    if df.empty:
        vuoto = True
    else:
        vuoto = False

    if not vuoto:
        st.write('Spostamenti nella tratta ' + orig.capitalize() + ' -> ' + dest.capitalize() + ' divisi per fascia oraria')  

        result = my_groupby(df, ['FASCIA_ORARIA', 'ZONA_DEST', 'ZONA_ORIG','lat_orig', 'long_orig', 'lat_dest', 'long_dest'])
        result['TOT'] = result[["LAV_COND","LAV_PAX","LAV_MOTO","LAV_FERRO",
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
        
        st.bar_chart(result, x = 'FASCIA_ORARIA', y = 'TOT')

        ## LINE CHART MOTIVO SPOSTAMENTO ##

        st.write('Variazione del motivo di spostamento nel tempo')
        lc_df = pd.DataFrame()
        lc_df['FASCIA_ORARIA'] = hours
        lc_df['Lavoro'] = result[["LAV_COND","LAV_PAX","LAV_MOTO","LAV_FERRO","LAV_GOMMA","LAV_BICI","LAV_PIEDI","LAV_ALTRO"]].sum(axis=1)
        lc_df['Studenti'] = result[["STU_COND","STU_PAX","STU_MOTO","STU_FERRO","STU_GOMMA","STU_BICI","STU_PIEDI","STU_ALTRO"]].sum(axis=1)
        lc_df['Occasionale'] = result[["OCC_COND","OCC_PAX","OCC_MOTO","OCC_FERRO","OCC_GOMMA","OCC_BICI","OCC_PIEDI","OCC_ALTRO"]].sum(axis=1)
        lc_df['Ritorno'] = result[["RIT_COND","RIT_PAX","RIT_MOTO","RIT_FERRO","RIT_GOMMA","RIT_BICI","RIT_PIEDI","RIT_ALTRO"]].sum(axis=1)
        lc_df['Affari'] = result[["AFF_COND","AFF_PAX","AFF_MOTO","AFF_FERRO","AFF_GOMMA","AFF_BICI","AFF_PIEDI","AFF_ALTRO"]].sum(axis=1)

        st.line_chart(lc_df, x='FASCIA_ORARIA', y=['Lavoro', 'Studenti', 'Occasionale', 'Ritorno', 'Affari'])

        ## LINE CHART MODO SPOSTAMENTO ##

        st.write('Variazione del modo di spostamento nel tempo')
        lc2_df = pd.DataFrame()
        lc2_df['FASCIA_ORARIA'] = hours
        lc2_df['Macchina conducente'] = result[["LAV_COND","STU_COND","AFF_COND","OCC_COND","RIT_COND","AFF_COND"]].sum(axis=1)
        lc2_df['Macchina passeggero'] = result[["LAV_PAX","STU_PAX","AFF_PAX","OCC_PAX","RIT_PAX","AFF_PAX"]].sum(axis=1)
        lc2_df['Moto'] = result[["LAV_MOTO","STU_MOTO","AFF_MOTO","OCC_MOTO","RIT_MOTO","AFF_MOTO"]].sum(axis=1)
        lc2_df['Pullman'] = result[["LAV_GOMMA","STU_GOMMA","AFF_GOMMA","OCC_GOMMA","RIT_GOMMA","AFF_GOMMA"]].sum(axis=1)
        lc2_df['Treno'] = result[["LAV_FERRO","STU_FERRO","AFF_FERRO","OCC_FERRO","RIT_FERRO","AFF_FERRO"]].sum(axis=1)
        lc2_df['Bici'] = result[["LAV_BICI","STU_BICI","AFF_BICI","OCC_BICI","RIT_BICI","AFF_BICI"]].sum(axis=1)
        lc2_df['Piedi'] = result[["LAV_PIEDI","STU_PIEDI","AFF_PIEDI","OCC_PIEDI","RIT_PIEDI","AFF_PIEDI"]].sum(axis=1)
        lc2_df['Altro'] = result[["LAV_ALTRO","STU_ALTRO","AFF_ALTRO","OCC_ALTRO","RIT_ALTRO","AFF_ALTRO"]].sum(axis=1)


        st.line_chart(lc2_df, x='FASCIA_ORARIA', y=['Macchina conducente', 'Macchina passeggero', 'Moto', 'Treno', 'Pullman', 'Bici', 'Piedi', 'Altro'])

        ## HEATMAP DESTINAZIONE SPOSTAMENTO ##


        heat_df = st.session_state.database.loc[st.session_state.database['ZONA_ORIG'] == orig]
        heat_df = my_groupby(heat_df, ['ZONA_ORIG', 'ZONA_DEST', 'lat_dest', 'long_dest'])
        heat_df['TOT'] = heat_df[["LAV_COND","LAV_PAX","LAV_MOTO","LAV_FERRO",
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
        
        heat_df = hp_threshold(heat_df, 'TOT', 0.1, 1500)

        fig = px.density_mapbox(heat_df, lat = 'lat_dest', lon = 'long_dest', z = 'TOT',
                            radius = 10,
                            center = dict(lat = 45.5, lon = 9.93),
                            zoom = 6,
                            mapbox_style = 'carto-positron',
                            color_continuous_scale= 'inferno')

        st.plotly_chart(fig)

    else:
        st.warning('Per questa tratta non sono disponibili dati :(')
else:
    st.warning('Seleziona un comune di partenza e uno d\'arrivo')
st.markdown('***')
st.title('Flussi turistici nelle province lombarde')
st.markdown('***')


## BAR CHART TURISMO PER PROVINCIA ##

if prov_tur != None:
    df_bct = st.session_state.turisti.loc[st.session_state.turisti['Provincia'] == prov_tur][['Mese', 'Arrivi - Totale', 'Presenze - Totale']]
    df_bct = df_bct.groupby('Mese')[['Arrivi - Totale', 'Presenze - Totale']].sum().reset_index()
    df_bct['Mese'] = pd.Categorical(df_bct['Mese'], categories=mesi, ordered=True)
    df_bct.sort_values('Mese')
    fig = px.histogram(df_bct, x="Mese", y=["Presenze - Totale", 'Arrivi - Totale'],
                barmode='group',
                height=400
                )
    st.plotly_chart(fig)


    ## LINE CHART PROVENIENZA TURISTI ##

    mese = st.select_slider('Mese', mesi)

    df_lct = st.session_state.turisti.loc[(st.session_state.turisti['Provincia'] == prov_tur) & (st.session_state.turisti['Mese'] == mese)].groupby('Provenienza turisti')[['Arrivi - Totale', 'Presenze - Totale']].sum().reset_index()

    st.line_chart(df_lct.loc[df_lct['Provenienza turisti'].isin(regioni_italiane)].sort_values('Arrivi - Totale', ascending = False), x= 'Provenienza turisti', y='Arrivi - Totale')
    st.line_chart(df_lct.loc[-df_lct['Provenienza turisti'].isin(regioni_italiane)].sort_values('Arrivi - Totale', ascending = False).head(25), x= 'Provenienza turisti', y='Arrivi - Totale')
else:
    st.warning('Seleziona una provincia')

st.markdown('***')
st.title('Flussi turistici nei comuni lombardi')
st.markdown('***')

if com_tur != None:
    ## BAR CHART TURISTI PER COMUNE ##

    df_bctc = st.session_state.turisti_comuni.loc[st.session_state.turisti_comuni['Comune'] == com_tur]
    df_bctc2 = df_bctc.groupby(['Comune', 'Mese']).agg({'Presenze': 'mean', 'Permanenza media': 'mean'}).reset_index()
    df_bctc2['Mese'] = pd.Categorical(df_bctc2['Mese'], categories=mesi, ordered=True)
    df_bctc2.sort_values('Mese')


    st.write('Media di turisti nel comune di ' + com_tur)
    st.bar_chart(df_bctc2, x= 'Mese', y = 'Presenze')

    st.write('')
    st.write('Variazione negli anni dei turisti')
    df_bctc_tmp = pd.DataFrame(columns = ['Mese', 2019, 2020, 2021, 2022])

    df_bctc_tmp['Mese'] = mesi
    years = [2019, 2020, 2021, 2022]
    for year in years:
        df_bctc_tmp[year] = pd.Series(df_bctc.loc[df_bctc['Anno'] == year]['Presenze'].tolist())
        

    
    df_bctc_tmp.fillna(0, inplace=True)

    
    fig = px.histogram(df_bctc_tmp, x='Mese', y=[2019, 2020, 2021, 2022],
                barmode='group',
                height=400
                )
    st.plotly_chart(fig)

else:
    st.warning('Seleziona un comune')



####################################################################

## CODICE PER CREARE I FILE UTILIZZATI PER LA MAPPA ##
    

# output = pd.DataFrame(columns = ['orig', 'dest', 'count'])
# tmp_df = st.session_state.database_red.loc[st.session_state.database_red['FASCIA_ORARIA'] == '07:00-07:59']
# tmp_df['TOT'] = tmp_df[["LAV_COND","LAV_PAX","LAV_MOTO","LAV_FERRO",
#                                 "LAV_GOMMA","LAV_BICI","LAV_PIEDI","LAV_ALTRO",
#                                 "STU_COND","STU_PAX","STU_MOTO","STU_FERRO",
#                                 "STU_GOMMA","STU_BICI","STU_PIEDI","STU_ALTRO",
#                                 "OCC_COND","OCC_PAX","OCC_MOTO","OCC_FERRO",
#                                 "OCC_GOMMA","OCC_BICI","OCC_PIEDI","OCC_ALTRO",
#                                 "AFF_COND","AFF_PAX","AFF_MOTO","AFF_FERRO",
#                                 "AFF_GOMMA","AFF_BICI","AFF_PIEDI","AFF_ALTRO",
#                                 "RIT_COND","RIT_PAX","RIT_MOTO","RIT_FERRO",
#                                 "RIT_GOMMA","RIT_BICI","RIT_PIEDI",
#                                 "RIT_ALTRO"]].sum(axis=1)

# output['orig'] = tmp_df[['lat_orig', 'long_orig']].astype(str).agg(', '.join, axis=1)
# output['dest'] = tmp_df[['lat_dest', 'long_dest']].astype(str).agg(', '.join, axis=1)

# output['count'] = tmp_df['TOT']
# output
# Export the DataFrame to a csv file
# output.to_csv('blueflow.csv', index=False)
# output  = st.session_state.posizione
# output['id'] = pd.Series(range(1, len(output) + 1))
# #output.drop(columns= 'provincia', inplace=True)
# output.rename(columns= {'comune':'name', 'latitudine':'lat', 'longitudine':'lon'}, inplace=True)

# output.to_csv('locations.csv', index=False)

# id_csv = pd.read_csv('locations.csv')
# st.write(id_csv.head(10))
# tmp_df = st.session_state.database_red.loc[st.session_state.database_red['FASCIA_ORARIA'] == '07:00-07:59']
# tmp_df['TOT'] = tmp_df[["LAV_COND","LAV_PAX","LAV_MOTO","LAV_FERRO",
#                                 "LAV_GOMMA","LAV_BICI","LAV_PIEDI","LAV_ALTRO",
#                                 "STU_COND","STU_PAX","STU_MOTO","STU_FERRO",
#                                 "STU_GOMMA","STU_BICI","STU_PIEDI","STU_ALTRO",
#                                 "OCC_COND","OCC_PAX","OCC_MOTO","OCC_FERRO",
#                                 "OCC_GOMMA","OCC_BICI","OCC_PIEDI","OCC_ALTRO",
#                                 "AFF_COND","AFF_PAX","AFF_MOTO","AFF_FERRO",
#                                 "AFF_GOMMA","AFF_BICI","AFF_PIEDI","AFF_ALTRO",
#                                 "RIT_COND","RIT_PAX","RIT_MOTO","RIT_FERRO",
#                                 "RIT_GOMMA","RIT_BICI","RIT_PIEDI",
#                                 "RIT_ALTRO"]].sum(axis=1)
# tmp_df.drop(columns= ["LAV_COND","LAV_PAX","LAV_MOTO","LAV_FERRO",
#                                 "LAV_GOMMA","LAV_BICI","LAV_PIEDI","LAV_ALTRO",
#                                 "STU_COND","STU_PAX","STU_MOTO","STU_FERRO",
#                                 "STU_GOMMA","STU_BICI","STU_PIEDI","STU_ALTRO",
#                                 "OCC_COND","OCC_PAX","OCC_MOTO","OCC_FERRO",
#                                 "OCC_GOMMA","OCC_BICI","OCC_PIEDI","OCC_ALTRO",
#                                 "AFF_COND","AFF_PAX","AFF_MOTO","AFF_FERRO",
#                                 "AFF_GOMMA","AFF_BICI","AFF_PIEDI","AFF_ALTRO",
#                                 "RIT_COND","RIT_PAX","RIT_MOTO","RIT_FERRO",
#                                 "RIT_GOMMA","RIT_BICI","RIT_PIEDI",
#                                 "RIT_ALTRO",'PROV_ORIG', 'PROV_DEST', 'FASCIA_ORARIA', 'lat_orig', 'long_orig',
#                                 'lat_dest', 'long_dest'], inplace=True)
# tmp_df.rename(columns={'ZONA_ORIG':'origin', 'ZONA_DEST':'dest', 'TOT':'count'}, inplace=True)
# for o in tmp_df['origin'].drop_duplicates().tolist():
#     if o in id_csv['name'].values:
#         tmp_df.loc[tmp_df['origin'] == o, 'origin'] = id_csv.loc[id_csv['name'] == o, 'id'].values[0]
# for o in tmp_df['dest'].drop_duplicates().tolist():
#     if o in id_csv['name'].values:
#         tmp_df.loc[tmp_df['dest'] == o, 'dest'] = id_csv.loc[id_csv['name'] == o, 'id'].values[0]
# st.write(tmp_df.head(10))
# tmp_df.to_csv('flows.csv', index=False)



# id_csv = pd.read_csv('locations.csv')
# st.write(id_csv.head(10))
# tmp_df = st.session_state.database.loc[st.session_state.database['FASCIA_ORARIA'] == '07:00-07:59']
# tmp_df['TOT'] = tmp_df[["LAV_COND","LAV_PAX","LAV_MOTO","LAV_FERRO",
#                                 "LAV_GOMMA","LAV_BICI","LAV_PIEDI","LAV_ALTRO",
#                                 "STU_COND","STU_PAX","STU_MOTO","STU_FERRO",
#                                 "STU_GOMMA","STU_BICI","STU_PIEDI","STU_ALTRO",
#                                 "OCC_COND","OCC_PAX","OCC_MOTO","OCC_FERRO",
#                                 "OCC_GOMMA","OCC_BICI","OCC_PIEDI","OCC_ALTRO",
#                                 "AFF_COND","AFF_PAX","AFF_MOTO","AFF_FERRO",
#                                 "AFF_GOMMA","AFF_BICI","AFF_PIEDI","AFF_ALTRO",
#                                 "RIT_COND","RIT_PAX","RIT_MOTO","RIT_FERRO",
#                                 "RIT_GOMMA","RIT_BICI","RIT_PIEDI",
#                                 "RIT_ALTRO"]].sum(axis=1)
# tmp_df.drop(columns= ["LAV_COND","LAV_PAX","LAV_MOTO","LAV_FERRO",
#                                 "LAV_GOMMA","LAV_BICI","LAV_PIEDI","LAV_ALTRO",
#                                 "STU_COND","STU_PAX","STU_MOTO","STU_FERRO",
#                                 "STU_GOMMA","STU_BICI","STU_PIEDI","STU_ALTRO",
#                                 "OCC_COND","OCC_PAX","OCC_MOTO","OCC_FERRO",
#                                 "OCC_GOMMA","OCC_BICI","OCC_PIEDI","OCC_ALTRO",
#                                 "AFF_COND","AFF_PAX","AFF_MOTO","AFF_FERRO",
#                                 "AFF_GOMMA","AFF_BICI","AFF_PIEDI","AFF_ALTRO",
#                                 "RIT_COND","RIT_PAX","RIT_MOTO","RIT_FERRO",
#                                 "RIT_GOMMA","RIT_BICI","RIT_PIEDI",
#                                 "RIT_ALTRO",'PROV_ORIG', 'PROV_DEST', 'FASCIA_ORARIA', 'lat_orig', 'long_orig',
#                                 'lat_dest', 'long_dest'], inplace=True)
# tmp_df.rename(columns={'ZONA_ORIG':'origin', 'ZONA_DEST':'dest', 'TOT':'count'}, inplace=True)
# for o in tmp_df['origin'].drop_duplicates().tolist():
#     if o in id_csv['name'].values:
#         tmp_df.loc[tmp_df['origin'] == o, 'origin'] = id_csv.loc[id_csv['name'] == o, 'id'].values[0]
# for o in tmp_df['dest'].drop_duplicates().tolist():
#     if o in id_csv['name'].values:
#         tmp_df.loc[tmp_df['dest'] == o, 'dest'] = id_csv.loc[id_csv['name'] == o, 'id'].values[0]
# st.write(tmp_df.head(10))
# tmp_df.to_csv('flows2.csv', index=False)