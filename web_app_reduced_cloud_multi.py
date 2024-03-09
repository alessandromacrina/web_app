import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import plotly.express as px
from utils import *
import numpy as np
import streamlit.components.v1 as components
import requests
import time
import io
import altair as alt
import openpyxl



############################################################

# definizione variabili globali

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

# timer per calcolare quanto tempo ci vuole per inzializzare il session state
start_time = time.time()

if 'posizione' not in st.session_state:
    st.session_state.posizione = leggi_parquet("https://raw.githubusercontent.com/alessandromacrina/web_app/main/pos.parquet")
    convert_columns_to_lowercase(st.session_state.posizione, ('comune', 'provincia'))


if 'database' not in st.session_state:
    st.session_state.database = leggi_parquet("https://raw.githubusercontent.com/alessandromacrina/web_app/main/matrice_od_2020_passeggeri.parquet")
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
    st.session_state.turisti = leggi_parquet("https://raw.githubusercontent.com/alessandromacrina/web_app/main/Flussi_turistici_per_mese_nelle_province_lombarde_20240113.parquet")

if 'turisti_comuni' not in st.session_state:
    st.session_state.turisti_comuni = leggi_parquet("https://raw.githubusercontent.com/alessandromacrina/web_app/main/Flussi_turistici_per_mese_nei_comuni_lombardi_20240113.parquet")

if 'database_ch' not in st.session_state:
    st.session_state.database_ch = leggi_parquet("https://raw.githubusercontent.com/alessandromacrina/web_app/main/matrice_od_2020_passeggeri.parquet")
    st.session_state.database_ch = st.session_state.database_ch.loc[((st.session_state.database_ch['PROV_ORIG'] == 'VA') | (st.session_state.database_ch['PROV_ORIG'] == 'CO')) & (st.session_state.database_ch['ZONA_DEST'] == 'SVIZZERA')]
    st.session_state.database_ch = my_groupby(st.session_state.database_ch,['PROV_ORIG', 'FASCIA_ORARIA'])
    st.session_state.database_ch['TOT'] = st.session_state.database_ch[colonne].sum(axis=1)

if 'database_emissioni' not in st.session_state:
    response = requests.get('https://raw.githubusercontent.com/alessandromacrina/web_app/main/Tabella1_FE_CO2_veic-km_autovetture_strada.xlsx')
    st.session_state.database_emissioni = pd.read_excel(io.BytesIO(response.content))
# stop del timer e stampa del tempo di elaborazione session state

end_time = time.time()
elapsed_time = end_time - start_time
print("Elaborazione session state: ", elapsed_time, "secondi")


################################################################

# inizio applicazione web con funzioni streamlit

# Definizione funzioni per ogni pagina

def origine_destinazione():

    st.sidebar.markdown('***')

    orig = st.sidebar.selectbox('Seleziona comune di origine',  st.session_state.comune, index=None)
    dest = st.sidebar.selectbox('Seleziona comune di destinazione', st.session_state.comune, index=None)

    st.title('Mobilità nella provincia di Como e Varese')
    st.markdown('I dati mostrati nei grafici sono tratti da [OpenData Regione Lombardia - Matrice OD2020](https://www.dati.lombardia.it/Mobilit-e-trasporti/Matrice-OD2020-Passeggeri/hyqr-mpe2/about_data)')
    st.markdown('***')
    toggle = st.toggle('Visualizza tutta la Lombardia', value=False,)

    if toggle:
        flow_map_url = "https://www.flowmap.blue/1Ui8Hbu8iLb7V6EtXUfqQ2GnogVrx-qOfOy5y0a1Fcos"
    else:
        flow_map_url = "https://www.flowmap.blue/14jvR16yGZV7D_1TUeR8Ws857LPTvHdToVkewxuVHGkE"

    components.iframe(flow_map_url, height=400, width=700)

    st.markdown('***')
    if ((orig != None) & (dest != None)):
    ## BAR CHART SPOSTAMENTI TOTALI ##

        df = st.session_state.database_red.loc[(st.session_state.database_red['ZONA_ORIG'] == orig) & (st.session_state.database_red['ZONA_DEST'] == dest )]

        if df.empty:    
            vuoto = True
        else:
            vuoto = False

        if not vuoto:
            st.write(f'**Spostamenti nella tratta {orig.capitalize()} -> {dest.capitalize()} divisi per fascia oraria**')  

            result = my_groupby(df, ['FASCIA_ORARIA', 'ZONA_DEST', 'ZONA_ORIG','lat_orig', 'long_orig', 'lat_dest', 'long_dest'])
            result = calcola_totale(result)
            
            st.bar_chart(result, x = 'FASCIA_ORARIA', y = 'TOT')

            ## LINE CHART MOTIVO SPOSTAMENTO ##

            st.write('**Variazione del motivo di spostamento nel tempo**')
            lc_df = pd.DataFrame()
            lc_df['FASCIA_ORARIA'] = hours
            lc_df['Lavoro'] = result[["LAV_COND","LAV_PAX","LAV_MOTO","LAV_FERRO","LAV_GOMMA","LAV_BICI","LAV_PIEDI","LAV_ALTRO"]].sum(axis=1)
            lc_df['Studenti'] = result[["STU_COND","STU_PAX","STU_MOTO","STU_FERRO","STU_GOMMA","STU_BICI","STU_PIEDI","STU_ALTRO"]].sum(axis=1)
            lc_df['Occasionale'] = result[["OCC_COND","OCC_PAX","OCC_MOTO","OCC_FERRO","OCC_GOMMA","OCC_BICI","OCC_PIEDI","OCC_ALTRO"]].sum(axis=1)
            lc_df['Ritorno'] = result[["RIT_COND","RIT_PAX","RIT_MOTO","RIT_FERRO","RIT_GOMMA","RIT_BICI","RIT_PIEDI","RIT_ALTRO"]].sum(axis=1)
            lc_df['Affari'] = result[["AFF_COND","AFF_PAX","AFF_MOTO","AFF_FERRO","AFF_GOMMA","AFF_BICI","AFF_PIEDI","AFF_ALTRO"]].sum(axis=1)

            #st.line_chart(lc_df, x='FASCIA_ORARIA', y=['Lavoro', 'Studenti', 'Occasionale', 'Ritorno', 'Affari'])

            lc_df_melted = pd.melt(lc_df, id_vars='FASCIA_ORARIA', value_vars=['Lavoro', 'Studenti', 'Occasionale', 'Ritorno', 'Affari'])
            chart = alt.Chart(lc_df_melted).mark_line().encode(x='FASCIA_ORARIA:N', y='value:Q', color='variable:N').properties(width=825)

            # Display the chart using Streamlit
            st.altair_chart(chart, use_container_width=False)

            ## LINE CHART MODO SPOSTAMENTO ##

            st.write('**Variazione del modo di spostamento nel tempo**')
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

            #st.line_chart(lc2_df, x='FASCIA_ORARIA', y=['Macchina conducente', 'Macchina passeggero', 'Moto', 'Treno', 'Pullman', 'Bici', 'Piedi', 'Altro'])
            lc2_df_melted = pd.melt(lc2_df, id_vars='FASCIA_ORARIA', value_vars=['Macchina conducente', 'Macchina passeggero', 'Moto', 'Treno', 'Pullman', 'Bici', 'Piedi', 'Altro'])
            chart2 = alt.Chart(lc2_df_melted).mark_line().encode(x='FASCIA_ORARIA:N', y='value:Q', color='variable:N').properties(width=875)
            # Display the chart using Streamlit
            st.altair_chart(chart2, use_container_width=False)
        else:
            st.warning('Per questa tratta non sono disponibili dati :(')
    else:
        st.warning('Seleziona un comune di partenza e uno d\'arrivo')

###########################################################
    
def spostamenti_dal_comune():
    st.title('Spostamenti dal comune verso il resto della provincia')
    st.sidebar.markdown('***')

    com_orig = st.sidebar.selectbox('Seleziona un comune per analizzare gli spostamenti', st.session_state.comune, index=None)
    st.markdown('I dati mostrati nei grafici sono tratti da [OpenData Regione Lombardia - Matrice OD2020](https://www.dati.lombardia.it/Mobilit-e-trasporti/Matrice-OD2020-Passeggeri/hyqr-mpe2/about_data)')
    st.markdown('***')


    if com_orig != None:

        ## HEATMAP DESTINAZIONE SPOSTAMENTO ##

        st.write(f'**Heat map degli spostamenti con partenza dal comune di {com_orig.capitalize()}**')
        heat_df = st.session_state.database.loc[st.session_state.database['ZONA_ORIG'] == com_orig]
        heat_df = my_groupby(heat_df, ['ZONA_ORIG', 'ZONA_DEST', 'lat_dest', 'long_dest'])
        heat_df = calcola_totale(heat_df)

        heat_df = hp_threshold(heat_df, 'TOT', 0.1, 1500)

        fig = px.density_mapbox(heat_df, lat = 'lat_dest', lon = 'long_dest', z = 'TOT',
                            radius = 10,
                            center = dict(lat = 45.5, lon = 9.93),
                            zoom = 6,
                            mapbox_style = 'carto-positron',
                            color_continuous_scale= 'inferno')

        st.plotly_chart(fig)

        ## BAR CHAR DESTINAZIONI ##

        st.write(f'**Principali destinazioni partendo dal comune di {com_orig.capitalize()}**')
        df_bcdest = st.session_state.database_red.loc[(st.session_state.database['ZONA_ORIG'] == com_orig) & (st.session_state.database['ZONA_DEST'] != com_orig)]
        ora = st.select_slider('Fascia oraria', hours)
        df_bcdest = df_bcdest.loc[df_bcdest['FASCIA_ORARIA'] == ora]
        df_bcdest = my_groupby(df_bcdest, ['ZONA_ORIG','ZONA_DEST'])
        df_bcdest = calcola_totale(df_bcdest)
        df_bcdest = df_bcdest.sort_values('TOT', ascending=False).head(20)
        st.bar_chart(df_bcdest, x='ZONA_DEST', y='TOT')

        ## PRINCIPALE METODO DI SPOSTAMENTO ##

        #df_bcdest2 = my_groupby(df_bcdest, colonne)
        

    else:
        st.warning('Seleziona un comune')


###########################################################
    
def spostamenti_verso_la_svizzera():
    st.title('Spostamenti verso la Svizzera')
    st.markdown('I dati mostrati nei grafici sono tratti da [OpenData Regione Lombardia - Matrice OD2020](https://www.dati.lombardia.it/Mobilit-e-trasporti/Matrice-OD2020-Passeggeri/hyqr-mpe2/about_data)')

    st.markdown('***')
    st.sidebar.markdown('***')

    col1, col2 = st.columns(2)
    with col1:
        tg_co = st.toggle('Como', False)

    with col2:
        tg_va = st.toggle('Varese', False)



    df_ch_va = st.session_state.database_ch.loc[st.session_state.database_ch['PROV_ORIG']=='VA']
    df_ch_co = st.session_state.database_ch.loc[st.session_state.database_ch['PROV_ORIG']=='CO']

    if tg_va:

        ## LINE CHART TOTALE SPOSTAMENTI PER FASCIA ORARIA ##
        st.write('\n')
        st.write('**Spostamenti totali da Varese verso la Svizzera suddivisi per fascia oraria**')
        st.bar_chart(df_ch_va, x='FASCIA_ORARIA', y='TOT')

        ## LINE CHART MOTIVO SPOSTAMENTO ##

        st.write('**Variazione del motivo di spostamento nel tempo**')

        df_ch_va1 = pd.DataFrame()
        df_ch_va1['FASCIA_ORARIA'] = hours
        df_ch_va1['Lavoro'] = df_ch_va[["LAV_COND","LAV_PAX","LAV_MOTO","LAV_FERRO","LAV_GOMMA","LAV_BICI","LAV_PIEDI","LAV_ALTRO"]].sum(axis=1).tolist()
        df_ch_va1['Studenti'] = df_ch_va[["STU_COND","STU_PAX","STU_MOTO","STU_FERRO","STU_GOMMA","STU_BICI","STU_PIEDI","STU_ALTRO"]].sum(axis=1).tolist()
        df_ch_va1['Occasionale'] = df_ch_va[["OCC_COND","OCC_PAX","OCC_MOTO","OCC_FERRO","OCC_GOMMA","OCC_BICI","OCC_PIEDI","OCC_ALTRO"]].sum(axis=1).tolist()
        df_ch_va1['Ritorno'] = df_ch_va[["RIT_COND","RIT_PAX","RIT_MOTO","RIT_FERRO","RIT_GOMMA","RIT_BICI","RIT_PIEDI","RIT_ALTRO"]].sum(axis=1).tolist()
        df_ch_va1['Affari'] = df_ch_va[["AFF_COND","AFF_PAX","AFF_MOTO","AFF_FERRO","AFF_GOMMA","AFF_BICI","AFF_PIEDI","AFF_ALTRO"]].sum(axis=1).tolist()

        #st.line_chart(df_ch_va1, x='FASCIA_ORARIA', y=['Lavoro', 'Studenti', 'Occasionale', 'Ritorno', 'Affari'])
        
        lc_df_melted = pd.melt(df_ch_va1, id_vars='FASCIA_ORARIA', value_vars=['Lavoro', 'Studenti', 'Occasionale', 'Ritorno', 'Affari'])
        chart = alt.Chart(lc_df_melted).mark_line().encode(x='FASCIA_ORARIA:N', y='value:Q', color='variable:N').properties(width=825)

        # Display the chart using Streamlit
        st.altair_chart(chart, use_container_width=False)
       
        ## LINE CHART MODALITA' DI SPOSTAMENTO ##
        st.write('**Variazione del modo di spostamento nel tempo**')

        df_ch_va2 = pd.DataFrame()
        df_ch_va2['FASCIA_ORARIA'] = hours
        df_ch_va2['Macchina conducente'] = df_ch_va[["LAV_COND","STU_COND","AFF_COND","OCC_COND","RIT_COND","AFF_COND"]].sum(axis=1).tolist()
        df_ch_va2['Macchina passeggero'] = df_ch_va[["LAV_PAX","STU_PAX","AFF_PAX","OCC_PAX","RIT_PAX","AFF_PAX"]].sum(axis=1).tolist()
        df_ch_va2['Moto'] = df_ch_va[["LAV_MOTO","STU_MOTO","AFF_MOTO","OCC_MOTO","RIT_MOTO","AFF_MOTO"]].sum(axis=1).tolist()
        df_ch_va2['Pullman'] = df_ch_va[["LAV_GOMMA","STU_GOMMA","AFF_GOMMA","OCC_GOMMA","RIT_GOMMA","AFF_GOMMA"]].sum(axis=1).tolist()
        df_ch_va2['Treno'] = df_ch_va[["LAV_FERRO","STU_FERRO","AFF_FERRO","OCC_FERRO","RIT_FERRO","AFF_FERRO"]].sum(axis=1).tolist()
        df_ch_va2['Bici'] = df_ch_va[["LAV_BICI","STU_BICI","AFF_BICI","OCC_BICI","RIT_BICI","AFF_BICI"]].sum(axis=1).tolist()
        df_ch_va2['Piedi'] = df_ch_va[["LAV_PIEDI","STU_PIEDI","AFF_PIEDI","OCC_PIEDI","RIT_PIEDI","AFF_PIEDI"]].sum(axis=1).tolist()
        df_ch_va2['Altro'] = df_ch_va[["LAV_ALTRO","STU_ALTRO","AFF_ALTRO","OCC_ALTRO","RIT_ALTRO","AFF_ALTRO"]].sum(axis=1).tolist()
        
        #st.line_chart(df_ch_va2, x='FASCIA_ORARIA', y=['Macchina conducente', 'Macchina passeggero', 'Moto', 'Treno', 'Pullman', 'Bici', 'Piedi', 'Altro'])

        lc_df2_melted = pd.melt(df_ch_va2, id_vars='FASCIA_ORARIA', value_vars=['Macchina conducente', 'Macchina passeggero', 'Moto', 'Treno', 'Pullman', 'Bici', 'Piedi', 'Altro'])
        chart2 = alt.Chart(lc_df2_melted).mark_line().encode(x='FASCIA_ORARIA:N', y='value:Q', color='variable:N').properties(width=875)

        # Display the chart using Streamlit
        st.altair_chart(chart2, use_container_width=False)

    if tg_co:
        ## LINE CHART TOTALE SPOSTAMENTI PER FASCIA ORARIA ##

        st.write('\n')
        st.write('**Spostamenti totali da Como verso la Svizzera suddivisi per fascia oraria**')
        st.bar_chart(df_ch_co, x='FASCIA_ORARIA', y='TOT')

        ## LINE CHART MOTIVO SPOSTAMENTO ##

        st.write('**Variazione del motivo di spostamento nel tempo**')
        df_ch_co1 = pd.DataFrame()
        df_ch_co1['FASCIA_ORARIA'] = hours
        df_ch_co1['Lavoro'] = df_ch_co[["LAV_COND","LAV_PAX","LAV_MOTO","LAV_FERRO","LAV_GOMMA","LAV_BICI","LAV_PIEDI","LAV_ALTRO"]].sum(axis=1)
        df_ch_co1['Studenti'] = df_ch_co[["STU_COND","STU_PAX","STU_MOTO","STU_FERRO","STU_GOMMA","STU_BICI","STU_PIEDI","STU_ALTRO"]].sum(axis=1)
        df_ch_co1['Occasionale'] = df_ch_co[["OCC_COND","OCC_PAX","OCC_MOTO","OCC_FERRO","OCC_GOMMA","OCC_BICI","OCC_PIEDI","OCC_ALTRO"]].sum(axis=1)
        df_ch_co1['Ritorno'] = df_ch_co[["RIT_COND","RIT_PAX","RIT_MOTO","RIT_FERRO","RIT_GOMMA","RIT_BICI","RIT_PIEDI","RIT_ALTRO"]].sum(axis=1)
        df_ch_co1['Affari'] = df_ch_co[["AFF_COND","AFF_PAX","AFF_MOTO","AFF_FERRO","AFF_GOMMA","AFF_BICI","AFF_PIEDI","AFF_ALTRO"]].sum(axis=1)

        #st.line_chart(df_ch_co1, x='FASCIA_ORARIA', y=['Lavoro', 'Studenti', 'Occasionale', 'Ritorno', 'Affari'])

        lc3_df_melted = pd.melt(df_ch_co1, id_vars='FASCIA_ORARIA', value_vars=['Lavoro', 'Studenti', 'Occasionale', 'Ritorno', 'Affari'])
        chart3 = alt.Chart(lc3_df_melted).mark_line().encode(x='FASCIA_ORARIA:N', y='value:Q', color='variable:N').properties(width=825)

        # Display the chart using Streamlit
        st.altair_chart(chart3, use_container_width=False)

        ## LINE CHART MODALITA' DI SPOSTAMENTO ##
        st.write('**Variazione del modo di spostamento nel tempo**')

        df_ch_co2 = pd.DataFrame()
        df_ch_co2['FASCIA_ORARIA'] = hours
        df_ch_co2['Macchina conducente'] = df_ch_co[["LAV_COND","STU_COND","AFF_COND","OCC_COND","RIT_COND","AFF_COND"]].sum(axis=1)
        df_ch_co2['Macchina passeggero'] = df_ch_co[["LAV_PAX","STU_PAX","AFF_PAX","OCC_PAX","RIT_PAX","AFF_PAX"]].sum(axis=1)
        df_ch_co2['Moto'] = df_ch_co[["LAV_MOTO","STU_MOTO","AFF_MOTO","OCC_MOTO","RIT_MOTO","AFF_MOTO"]].sum(axis=1)
        df_ch_co2['Pullman'] = df_ch_co[["LAV_GOMMA","STU_GOMMA","AFF_GOMMA","OCC_GOMMA","RIT_GOMMA","AFF_GOMMA"]].sum(axis=1)
        df_ch_co2['Treno'] = df_ch_co[["LAV_FERRO","STU_FERRO","AFF_FERRO","OCC_FERRO","RIT_FERRO","AFF_FERRO"]].sum(axis=1)
        df_ch_co2['Bici'] = df_ch_co[["LAV_BICI","STU_BICI","AFF_BICI","OCC_BICI","RIT_BICI","AFF_BICI"]].sum(axis=1)
        df_ch_co2['Piedi'] = df_ch_co[["LAV_PIEDI","STU_PIEDI","AFF_PIEDI","OCC_PIEDI","RIT_PIEDI","AFF_PIEDI"]].sum(axis=1)
        df_ch_co2['Altro'] = df_ch_co[["LAV_ALTRO","STU_ALTRO","AFF_ALTRO","OCC_ALTRO","RIT_ALTRO","AFF_ALTRO"]].sum(axis=1)
        
        #st.line_chart(df_ch_co2, x='FASCIA_ORARIA', y=['Macchina conducente', 'Macchina passeggero', 'Moto', 'Treno', 'Pullman', 'Bici', 'Piedi', 'Altro'])

        lc_df4_melted = pd.melt(df_ch_co2, id_vars='FASCIA_ORARIA', value_vars=['Macchina conducente', 'Macchina passeggero', 'Moto', 'Treno', 'Pullman', 'Bici', 'Piedi', 'Altro'])
        chart4 = alt.Chart(lc_df4_melted).mark_line().encode(x='FASCIA_ORARIA:N', y='value:Q', color='variable:N').properties(width=875)

        # Display the chart using Streamlit
        st.altair_chart(chart4, use_container_width=False)

###########################################################

def analisi_turismo():
    st.sidebar.markdown('***')

    prov_tur = st.sidebar.selectbox('Seleziona una provincia per visualizzare i dati relativi sui flussi turistici',  st.session_state.turisti['Provincia'].drop_duplicates().sort_values().tolist(), index=None)
    com_tur = st.sidebar.selectbox('Seleziona un comune per visualizzare i dati relativi sui flussi turistici', st.session_state.turisti_comuni['Comune'].drop_duplicates().sort_values().tolist(), index=None)
    st.title('Flussi turistici nelle province lombarde')
    st.markdown('I dati mostrati nei grafici sono tratti da [OpenData Regione Lombardia - Flussi turistici per provincia](https://www.dati.lombardia.it/Turismo/Flussi-turistici-per-mese-nelle-province-lombarde/xzck-giqt/about_data)')

    st.markdown('***')


    ## BAR CHART TURISMO PER PROVINCIA ##

    if prov_tur != None:
        
        st.write('\n')
        st.write(f'**Presenze e arrivi totali nella provincia di {prov_tur}**')
        df_bct = st.session_state.turisti.loc[st.session_state.turisti['Provincia'] == prov_tur][['Anno','Mese', 'Arrivi - Totale', 'Presenze - Totale']]
        df_bct = df_bct.groupby(['Mese', 'Anno'])[['Arrivi - Totale', 'Presenze - Totale']].sum().reset_index()
        anno = st.radio('Anno', [2019, 2020, 2021, 2022], horizontal=True)
        df_bct = df_bct.loc[df_bct['Anno'] == anno]
        df_bct['Mese'] = pd.Categorical(df_bct['Mese'], categories=mesi, ordered=True)
        df_bct = df_bct.sort_values('Mese').set_index('Mese')

        fig = px.histogram(df_bct, x=df_bct.index, y=["Presenze - Totale", 'Arrivi - Totale'],
                            barmode='group',
                            height=400
                            )
        st.plotly_chart(fig)


        ## LINE CHART PROVENIENZA TURISTI ##

        st.write('**Provenienza turisti**')
        mese = st.select_slider('Mese', mesi, help='muovi il cursore per selezionare il mese')
        df_lct = st.session_state.turisti.loc[(st.session_state.turisti['Provincia'] == prov_tur) & (st.session_state.turisti['Mese'] == mese)].groupby('Provenienza turisti')[['Arrivi - Totale', 'Presenze - Totale']].sum().reset_index()
        
        st.line_chart(df_lct.loc[df_lct['Provenienza turisti'].isin(regioni_italiane)].sort_values('Arrivi - Totale', ascending = False), x= 'Provenienza turisti', y='Arrivi - Totale')
        st.line_chart(df_lct.loc[-df_lct['Provenienza turisti'].isin(regioni_italiane)].sort_values('Arrivi - Totale', ascending = False).head(25), x= 'Provenienza turisti', y='Arrivi - Totale')
        
    else:
        st.warning('Seleziona una provincia')

    st.markdown('***')
    st.title('Flussi turistici nei comuni lombardi')
    st.markdown('I dati mostrati nei grafici sono tratti da [OpenData Regione Lombardia - Flussi turistici per comune](https://www.dati.lombardia.it/Turismo/Flussi-turistici-per-mese-nei-comuni-lombardi/mzxz-sz25/about_data)')

    st.markdown('***')

    if com_tur != None:
        ## BAR CHART TURISTI PER COMUNE ##

        df_bctc = st.session_state.turisti_comuni.loc[st.session_state.turisti_comuni['Comune'] == com_tur]
        df_bctc2 = df_bctc.groupby(['Comune', 'Mese']).agg({'Presenze': 'mean', 'Permanenza media': 'mean'}).reset_index()
        df_bctc2['Mese'] = pd.Categorical(df_bctc2['Mese'], categories=mesi, ordered=True)
        df_bctc2.sort_values('Mese')


        st.write(f'**Media di turisti nel comune di {com_tur}**')
        st.bar_chart(df_bctc2, x= 'Mese', y = 'Presenze')

        st.write('')
        st.write(f'**Variazione negli anni dei turisti nel comune di {com_tur}**')
        df_bctc_tmp = pd.DataFrame(columns = ['Mese', 2019, 2020, 2021, 2022])

        df_bctc_tmp['Mese'] = mesi
        years = [2019, 2020, 2021, 2022]
        df_bctc['Mese'] = pd.Categorical(df_bctc['Mese'], categories=mesi, ordered=True)
        df_bctc = df_bctc.sort_values(['Anno','Mese']).set_index('Mese')


        for year in years:
            df_bctc_tmp[year] = pd.Series(df_bctc.loc[df_bctc['Anno'] == year]['Presenze'].tolist())
            

        
        df_bctc_tmp.fillna(0, inplace=True)

        
        fig = px.histogram(df_bctc_tmp, x='Mese', y=[2019, 2020, 2021, 2022],
                    barmode='group',
                    height=400
                    )
        st.plotly_chart(fig)

        if com_tur == 'Como':
            btn_com = st.button('Stampa tabella')
            if btn_com:
                st.write(df_bctc)
                st.write(df_bctc_tmp)

    else:
        st.warning('Seleziona un comune')

###########################################################

def emissioni_anidride_carbonica():
    st.title('Emissioni anidride carbonica')
    st.write(f'**Come vengono influenzate le emissioni di andride carbonica con l\'inserimento in circolazione di veicoli elettrici?**')
    st.markdown('I dati mostrati nei grafici sono tratti da [OpenData Regione Lombardia - Matrice OD2020](https://www.dati.lombardia.it/Mobilit-e-trasporti/Matrice-OD2020-Passeggeri/hyqr-mpe2/about_data) e [Emissioni specifiche di anidride carbonica - ISPRA](https://indicatoriambientali.isprambiente.it/sys_ind/577)')
    st.markdown('***')
    df_ac_va = st.session_state.database_red.loc[st.session_state.database_red['PROV_ORIG'] == 'VA'][['PROV_ORIG','LAV_COND','STU_COND','OCC_COND','AFF_COND','RIT_COND']]
    df_ac_va['sum'] = df_ac_va[['LAV_COND','STU_COND','OCC_COND','AFF_COND','RIT_COND']].sum(axis=1)

    df_ac_co = st.session_state.database_red.loc[st.session_state.database_red['PROV_ORIG'] == 'CO'][['PROV_ORIG','LAV_COND','STU_COND','OCC_COND','AFF_COND','RIT_COND']]
    df_ac_co['sum'] = df_ac_co[['LAV_COND','STU_COND','OCC_COND','AFF_COND','RIT_COND']].sum(axis=1)

    df_e = st.session_state.database_emissioni.copy()
    df_e = df_e.drop(labels=[0,6,7], axis=0)
    
    df_e = df_e.rename(columns={df_e.columns[0]: 'tipo'})
    df_e = df_e.rename(columns={df_e.columns[13]: '2018'})

    df_e['tipo'].replace("Parco autovetture benzina", "Benzina", inplace=True)
    df_e['tipo'].replace("Parco autovetture gasolio", "Gasolio", inplace=True)
    df_e['tipo'].replace("Parco autovetture GPL", "GPL", inplace=True)
    df_e['tipo'].replace("Parco autovetture gas naturale", "Gas naturale", inplace=True)
    df_e['tipo'].replace("Parco autovetture ibride (benzina - elettrico)", "Ibride", inplace=True)

    st.write(f'**Grafico emissioni di anidride carbonica (gCO₂/km)**')
    st.write('I dati mostrati riguardano i dati relativi alle macchine circolanti in strada nel 2018')
    st.bar_chart(df_e, x='tipo', y=[2018])

    labels = ['Benzina','Gasolio', 'GPL', 'Gas naturale', 'Ibride', 'Elettriche']
    values = [18120336,18122464,3058231,1019485,262320,12441]
    fig = px.pie(values=values, names=labels, title='Tipo di alimentazione dei veicoli circolanti')
    st.plotly_chart(fig)

    totale_va = df_ac_va['sum'].sum()
    totale_co = df_ac_co['sum'].sum()
    df_e = df_e[['tipo','2018']]
    att_co = totale_co * (0.446*df_e.loc[df_e['tipo']=='Benzina']['2018'].to_numpy()[0]+
                      0.446*df_e.loc[df_e['tipo']=='Gasolio']['2018'].to_numpy()[0]+
                      0.075*df_e.loc[df_e['tipo']=='GPL']['2018'].to_numpy()[0]+
                      0.0251*df_e.loc[df_e['tipo']=='Gas naturale']['2018'].to_numpy()[0]+
                      0.00646*df_e.loc[df_e['tipo']=='Ibride']['2018'].to_numpy()[0])

    att_va = totale_va * (0.446*df_e.loc[df_e['tipo']=='Benzina']['2018'].to_numpy()[0]+
                        0.446*df_e.loc[df_e['tipo']=='Gasolio']['2018'].to_numpy()[0]+
                        0.075*df_e.loc[df_e['tipo']=='GPL']['2018'].to_numpy()[0]+
                        0.0251*df_e.loc[df_e['tipo']=='Gas naturale']['2018'].to_numpy()[0]+
                        0.00646*df_e.loc[df_e['tipo']=='Ibride']['2018'].to_numpy()[0])

    cinque_co = totale_co * (0.421*df_e.loc[df_e['tipo']=='Benzina']['2018'].to_numpy()[0]+
                            0.421*df_e.loc[df_e['tipo']=='Gasolio']['2018'].to_numpy()[0]+
                            0.075*df_e.loc[df_e['tipo']=='GPL']['2018'].to_numpy()[0]+
                            0.0251*df_e.loc[df_e['tipo']=='Gas naturale']['2018'].to_numpy()[0]+
                            0.05646*df_e.loc[df_e['tipo']=='Ibride']['2018'].to_numpy()[0])

    cinque_va = totale_va * (0.421*df_e.loc[df_e['tipo']=='Benzina']['2018'].to_numpy()[0]+
                            0.421*df_e.loc[df_e['tipo']=='Gasolio']['2018'].to_numpy()[0]+
                            0.075*df_e.loc[df_e['tipo']=='GPL']['2018'].to_numpy()[0]+
                            0.0251*df_e.loc[df_e['tipo']=='Gas naturale']['2018'].to_numpy()[0]+
                            0.05646*df_e.loc[df_e['tipo']=='Ibride']['2018'].to_numpy()[0])

    dieci_co = totale_co * (0.396*df_e.loc[df_e['tipo']=='Benzina']['2018'].to_numpy()[0]+
                        0.396*df_e.loc[df_e['tipo']=='Gasolio']['2018'].to_numpy()[0]+
                        0.075*df_e.loc[df_e['tipo']=='GPL']['2018'].to_numpy()[0]+
                        0.0251*df_e.loc[df_e['tipo']=='Gas naturale']['2018'].to_numpy()[0]+
                        0.10646*df_e.loc[df_e['tipo']=='Ibride']['2018'].to_numpy()[0])

    dieci_va = totale_va * (0.396*df_e.loc[df_e['tipo']=='Benzina']['2018'].to_numpy()[0]+
                        0.396*df_e.loc[df_e['tipo']=='Gasolio']['2018'].to_numpy()[0]+
                        0.075*df_e.loc[df_e['tipo']=='GPL']['2018'].to_numpy()[0]+
                        0.0251*df_e.loc[df_e['tipo']=='Gas naturale']['2018'].to_numpy()[0]+
                        0.10646*df_e.loc[df_e['tipo']=='Ibride']['2018'].to_numpy()[0])

    venti_co = totale_co * (0.346*df_e.loc[df_e['tipo']=='Benzina']['2018'].to_numpy()[0]+
                        0.346*df_e.loc[df_e['tipo']=='Gasolio']['2018'].to_numpy()[0]+
                        0.075*df_e.loc[df_e['tipo']=='GPL']['2018'].to_numpy()[0]+
                        0.0251*df_e.loc[df_e['tipo']=='Gas naturale']['2018'].to_numpy()[0]+
                        0.20646*df_e.loc[df_e['tipo']=='Ibride']['2018'].to_numpy()[0])

    venti_va = totale_va * (0.346*df_e.loc[df_e['tipo']=='Benzina']['2018'].to_numpy()[0]+
                        0.346*df_e.loc[df_e['tipo']=='Gasolio']['2018'].to_numpy()[0]+
                        0.075*df_e.loc[df_e['tipo']=='GPL']['2018'].to_numpy()[0]+
                        0.0251*df_e.loc[df_e['tipo']=='Gas naturale']['2018'].to_numpy()[0]+
                        0.20646*df_e.loc[df_e['tipo']=='Ibride']['2018'].to_numpy()[0])

    # dato preso dal sito UnipolSai che ha ricavato il dato dalle scatole nere installate sulle auto
    # url: https://www.unipolsai.com/sites/corporate/files/pages_related_documents/cs_osservatorio-unipolsai-2019.pdf
    km_al_giorno = 41

    data_va = [{'Percentuale': 'Attuale', 'Varese': gCO2_to_tonsCO2_per_km(att_va) * km_al_giorno, 'Como': gCO2_to_tonsCO2_per_km(att_co)* km_al_giorno},
            {'Percentuale': '+5%', 'Varese': gCO2_to_tonsCO2_per_km(cinque_va)* km_al_giorno,'Como': gCO2_to_tonsCO2_per_km(cinque_co)* km_al_giorno},
            {'Percentuale': '+10%', 'Varese': gCO2_to_tonsCO2_per_km(dieci_va)* km_al_giorno, 'Como': gCO2_to_tonsCO2_per_km(dieci_co)* km_al_giorno},
            {'Percentuale': '+20%', 'Varese': gCO2_to_tonsCO2_per_km(venti_va)* km_al_giorno, 'Como': gCO2_to_tonsCO2_per_km(venti_co)* km_al_giorno}]
    df_va = pd.DataFrame(data_va)
    df_va['Percentuale'] = pd.Categorical(df_va['Percentuale'], categories=['Attuale', '+5%', '+10%', '+20%'], ordered=True)

    st.write(f'**Emissioni di tonnelate di anidride carbonica con la sostituzione di veicoli a benzina e a gasolio con autovetture ibride:**')
    st.line_chart(df_va, x='Percentuale', y=['Varese', 'Como'])
    st.info(body= 'Nel grafico qui sopra è stata simulata la sostituzione di un numero equivalente di veicoli a benzina e diesel con vetture ibride, mantenendo il totale invariato. Ad esempio, nel caso "+20", è stato eliminato il 10% delle auto a benzina e il 10% di quelle a gasolio, sostituendole con un incremento del 20% di vetture ibride.', icon='ℹ️')

# Sidebar for navigation
pages = {
    "Origine-Destinazione": origine_destinazione,
    "Spostamenti dal Comune": spostamenti_dal_comune,
    "Spostamenti verso la Svizzera": spostamenti_verso_la_svizzera,
    "Analisi Turismo": analisi_turismo,
    "Emissioni Anidride Carbonica": emissioni_anidride_carbonica
}

selected_page = st.sidebar.radio("Pagine", list(pages.keys()))
pages[selected_page]()


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
