import requests as req
import pandas as pd 
from datetime import datetime
import sqlalchemy
import sqlite3
import httplib2
from bs4 import BeautifulSoup

def run_covid_etl(): 

    database_location = "sqlite:///COVID_PR_Completo.sqlite"
    
    if __name__ == "__main__":

        http = httplib2.Http()
        status, response = http.request('https://www.saude.pr.gov.br/Pagina/Coronavirus-COVID-19')
        soup = BeautifulSoup(response)

        result = []
        geral = ['geral','GERAL','Geral']

        for link in soup.find_all('a', href=True):
            if '.csv' in link['href']: 
                result.append(link['href'])

        for results in result:
            for g in geral: 
                try:
                    path = results
                    df = pd.read_csv(path,sep=';')
                    print(df)
                except Exception as e:
                    print(e)
        
        # Creates index based on date        
        today = datetime.today()
        date = today.strftime("%x")
        df["DATA_EXTRACAO_UTC"] = date
        df.set_index("DATA_EXTRACAO_UTC", inplace=True)

        # Creates a dataframe
        data_PR = pd.DataFrame(df,columns=["IBGE_RES_PR",
                                            "IBGE_ATEND_PR",
                                            "SEXO",
                                            "IDADE_ORIGINAL",
                                            "MUN_RESIDENCIA",
                                            "MUN_ATENDIMENTO",
                                            "LABORATORIO",
                                            "DATA_DIAGNOSTICO",
                                            "DATA_CONFIRMACAO_DIVULGACAO",
                                            "DATA_INICIO_SINTOMAS",
                                            "OBITO",
                                            "DATA_OBITO",
                                            "DATA_OBITO_DIVULGACAO",
                                            "STATUS",
                                            "DATA_RECUPERADO_DIVULGACAO",
                                            "FONTE_DADO_RECUPERADO"])

        # Creates engine & connects with it
        engine = sqlalchemy.create_engine(database_location)
        conn = sqlite3.connect('COVID_PR_Completo.db')
        cursor = conn.cursor()

        # Defines tables
        sql_query = """
        CREATE TABLE IF NOT EXISTS COVID_PR_Completo( 
        IBGE_RES_PR VARCHAR(15),
        IBGE_ATEND_PR VARCHAR(15),
        SEXO VARCHAR(5),
        IDADE_ORIGINAL INTEGER,
        MUN_RESIDENCIA VARCHAR(30),
        MUN_ATENDIMENTO VARCHAR(30),
        LABORATORIO VARCHAR(50),
        DATA_DIAGNOSTICO TEXT,
        DATA_CONFIRMACAO_DIVULGACAO TEXT,
        DATA_INICIO_SINTOMAS TEXT,
        OBITO VARCHAR(5),
        DATA_OBITO TEXT,
        DATA_OBITO_DIVULGACAO TEXT,
        STATUS VARCHAR(30),
        DATA_RECUPERADO_DIVULGACAO TEXT,
        FONTE_DADO_RECUPERADO VARCHAR(50)
        )
        """
        # to do: add CONSTRAINT primary_key_constraint PRIMARY KEY () 

        cursor.execute(sql_query)
        print("Open database successfully")

        try: 
            data_PR.to_sql("COVID_PR_Completo", engine, index=True, if_exists='append')
        except:
            print("Data already exists in the database")

        conn.close()
        print("Close database successfully")
        

