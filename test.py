
import matplotlib.pyplot as plt
import numpy as np
import sqlite3
from sqlite3 import Error
import requests
import json
from requests.exceptions import HTTPError
from sys import argv
import math
import sqlite3
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from dateutil import parser
from matplotlib import style
import schedule
import time



def job():
    


    def createTable():
    # Connecting to sqlite
        conn = sqlite3.connect('db1.db')
    # Creating a cursor object using the cursor() method
        cursor = conn.cursor()
    # Deleting old table
        cursor.execute("DROP TABLE IF EXISTS GOLD")
        print("\n\nUsunąłem starą tabelę")
    # Creating table
        print('\nTworzę nową tabele w bazie danych')
        table ="""CREATE TABLE GOLD(ID INTEGER PRIMARY KEY,
                                        VALUE INTEGER,
                                        DATE VARCHAR(255),
                                        CODE VARCHAR(255),
                                        MID INTEGER,
                                        EFFECTIVEDATE VARCHAR(255),
                                        WSKAZNIK INTEGER,
                                        SREDNIA INTEGER,
                                        ODCHYLENIE INTEGER,
                                        SREDNIAWSKAZNIK INTEGER
                                        );"""
        cursor.execute(table)
        print('\nTabela stworzona!')
    # Commit your changes in the database    
        conn.commit()
    # Closing the connection
        conn.close()

    def insertVaribleIntoTable(value, date, code, mid, effectivedate):
            try:
            # Łączenie z bazą
                conn = sqlite3.connect('db1.db')
            # Tworzenie kursora
                cursor = conn.cursor()
                print("\nPołaczono z bazą danych db1")

                sqlite_insert_with_param = """INSERT INTO GOLD
                                (VALUE, DATE, CODE, MID, EFFECTIVEDATE) 
                                VALUES (?, ?, ?, ?, ?);"""
                #Tworzenie tupli
                data_tuple = (value, date, code, mid, effectivedate)
                # Wstawianie danych w tabelę
                cursor.execute(sqlite_insert_with_param, data_tuple)
                # Zapisywanie
                conn.commit()
                print("\nDane skutecznie wprowadzone do bazy")
                cursor.close()

            except sqlite3.Error as error:
                print("\n\n\n\n\nNie udało się wprowadzić danych do bazy", error)
            finally:
                if conn:
                    conn.close()
                    print("\nZakończono połączenie z bazą")

    if len(argv) == 1 :
        """
        downloading and inserting data into db
        py req1.py insert
        """
        createTable()

        topCount = 255 # (int(input('\n\nPodaj liczbę ostatnich notowań, z których mam utworzyć wykres (max 255)\n')))    
        #Pobieranie informacji o cenie złota
        def get_rates_of_gold(topCount):
        
            try:        
                url = f"http://api.nbp.pl/api/cenyzlota/last/{topCount}"
                response = requests.get(url)
            except HTTPError as http_error:
                print(f'HTTP error: {http_error}')
            except Exception as e:
                print(f'Other exception: {e}')
            else:
                if response.status_code == 200:
                    return json.dumps(
                        response.json(),
                        indent=4,
                        sort_keys=True), response.json()
        #Pobieranie informacji o cenie waluty USD
        def get_rates_of_usd(topCount):
            currency = 'USD'
            try:
                table = "A"                
                url = f"http://api.nbp.pl/api/exchangerates/rates/{table}/{currency}/last/{topCount}/"
                response = requests.get(url)
            except HTTPError as http_error:
                print(f'HTTP error: {http_error}')
            except Exception as e:
                print(f'Other exception: {e}')
            else:
                if response.status_code == 200:
                    return json.dumps(
                        response.json(),
                        indent=4,
                        sort_keys=True), response.json()
        #Pobieranie informacji o cenie waluty GBP
        def get_rates_of_gbp(topCount):
            currency = 'GBP'
            try:
                table = "A"                
                url = f"http://api.nbp.pl/api/exchangerates/rates/{table}/{currency}/last/{topCount}/"
                response = requests.get(url)
            except HTTPError as http_error:
                print(f'HTTP error: {http_error}')
            except Exception as e:
                print(f'Other exception: {e}')
            else:
                if response.status_code == 200:
                    return json.dumps(
                        response.json(),
                        indent=4,
                        sort_keys=True), response.json()
        #Pobieranie informacji o cenie waluty EUR
        def get_rates_of_eur(topCount):
            currency = 'EUR'
            try:
                table = "A"                
                url = f"http://api.nbp.pl/api/exchangerates/rates/{table}/{currency}/last/{topCount}/"
                response = requests.get(url)
            except HTTPError as http_error:
                print(f'HTTP error: {http_error}')
            except Exception as e:
                print(f'Other exception: {e}')
            else:
                if response.status_code == 200:
                    return json.dumps(
                        response.json(),
                        indent=4,
                        sort_keys=True), response.json()
            
        if __name__ == '__main__':
    # JSON jako string oraz JSON
            print('ZŁOTO')
            json_caly, Gold = get_rates_of_gold(topCount)
            # Kurs Złota z <topCount> dni.
            
            print('USD')    
            json_caly1, Dol = get_rates_of_usd(topCount)
            print('GBP')  
            json_caly2, Gbp = get_rates_of_gbp(topCount)
            print('EUR')  
            json_caly3, Eur = get_rates_of_eur(topCount)
            
        # Wstawienie kursów dla USD, GBP, EUR do tabeli
            for i in range(len(Gold)):
                insertVaribleIntoTable(Gold[i]['cena'],Gold[i]['data'],'USD',Dol['rates'][i]['mid'], i+1)
            for i in range(len(Gold)):
                insertVaribleIntoTable(Gold[i]['cena'],Gold[i]['data'],'GBP',Gbp['rates'][i]['mid'], i+1)
            for i in range(len(Gold)):
                insertVaribleIntoTable(Gold[i]['cena'],Gold[i]['data'],'EUR',Eur['rates'][i]['mid'], i+1)
            print()
        # Wstawianie pobranych wartości do bazy danych

        def create_connection(conn):
            
            conn = None
            try:
                conn = sqlite3.connect('db1.db')
            except Error as e:
                print(e)

            return conn

        def create_indicator(conn):
                            
                cur = conn.cursor()
                cur.execute("SELECT ID, VALUE, MID FROM GOLD")
                

                rows = cur.fetchall()
                for row in rows:
                    cur.execute(
                        "UPDATE GOLD SET WSKAZNIK = :wartosc_1 WHERE ID = :biezace_id",
                        {"wartosc_1": round((row[1] / row[2])/10, 2),
                        "biezace_id": row[0]}
                    )
                
        def create_indicator2(conn):
                
                
                cur = conn.cursor()
                cur.execute("SELECT SUM(MID)/3, DATE FROM GOLD GROUP BY DATE")      
                rows = cur.fetchall()
                for row in rows:
                    cur.execute(
                        "UPDATE GOLD SET SREDNIA = :wartosc_2 WHERE DATE = :data_2",
                        {"wartosc_2": round(row[0], 2),
                        "data_2": row[1]}
                        
                    )
        def create_indicator4(conn):           
                
                cur = conn.cursor()
                cur.execute("SELECT SUM(SREDNIA)/3, DATE FROM GOLD GROUP BY DATE")           
                
                rows = cur.fetchall()
                for row in rows:
                    cur.execute(
                        "UPDATE GOLD SET SREDNIAWSKAZNIK = :wartosc_5 WHERE DATE = :data_5",
                        {"wartosc_5": round(row[0], 2),
                        "data_5": row[1]}
                        
                    )
        def create_indicator3(conn):            
                
                cur = conn.cursor()
                cur.execute("SELECT MID, ID, CODE, SREDNIA FROM GOLD GROUP BY ID")            
                
                rows = cur.fetchall()
                for row in rows:
                    print(row)
                    cur.execute(
                        "UPDATE GOLD SET ODCHYLENIE = :wartosc_3 WHERE ID = :data_3",
                        {"wartosc_3": round(((row[0]-row[3])/row[3])*100, 2) ,
                        "data_3": row[1],
                        }                    
                    )      

        def graph_data(conn):           
            # Rysowanie wykresu
            cur = conn.cursor()
            cur.execute("SELECT MID, ID, CODE, VALUE/50, DATE FROM GOLD WHERE CODE = :GBP",
            {"GBP": "GBP"})
            data = cur.fetchall()
            
            mid = []
            id = []
            code = []
            value = []
            effectivedate = []

            for row in data:
                id.append(row[1])
                value.append(row[3])
                mid.append(row[0])
                code.append(row[2])
                effectivedate.append(row[4]) 
            
            
            fig = plt.figure()
            ax1 = fig.add_subplot()
            ax1.set_title("Prównanie")
            # Plot values data on left Y axis
            ax1.set_ylabel("Value in PLN")
            ax1.set_xlabel("TIME")
            ax1.plot(effectivedate, value, '-', label="ZŁOTO/50", color='r')
            ax1.plot(effectivedate, mid,'-', label="GBP", color='b')
            plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=10))
            ax1.tick_params(axis='x', labelrotation=45)
        
            # Format the x-axis for dates (label formatting, rotation)
            #fig.autofmt_xdate(rotation=60)
            fig.tight_layout()
            # Show grids and legends
            cur.execute("SELECT MID, ID, CODE, SREDNIA, DATE FROM GOLD WHERE CODE = :USD",
            {"USD": "USD"})
            data1 = cur.fetchall()        
            mid1 = []
            id1 = []
            code1 = []
            srednia1 = []
            effectivedate1 = []

            for row in data1:
                id1.append(row[1])
                srednia1.append(row[3])
                mid1.append(row[0])
                code1.append(row[2])
                effectivedate1.append(row[4]) 
        
            ax1.plot(effectivedate1, mid1,'-', label="USD", color='g')

            cur.execute("SELECT MID, ID, CODE, SREDNIA, DATE FROM GOLD WHERE CODE = :EUR",
            {"EUR": "EUR"})
            data2 = cur.fetchall()
            
            mid2 = []
            id2 = []
            code2 = []
            srednia2 = []
            effectivedate2 = []

            for row in data2:
                id2.append(row[1])
                srednia2.append(row[3])
                mid2.append(row[0])
                code2.append(row[2])
                effectivedate2.append(row[4]) 
            
            ax1.plot(effectivedate2, mid2,'-', label="EUR", color='c')

            # Show grids and legends
            ax1.grid(True)
            ax1.legend(loc='best', framealpha=0.5)        
            plt.savefig("wykres.png")     

        def main():
            database = r"db1.db"
            # create a database connection
            conn = create_connection(database)
            with conn:
                print("Wyliczam i wprowadzam do bazy wskaznik GOLD/CURRENCY")
                create_indicator(conn)
                create_indicator2(conn)
                create_indicator3(conn)
                create_indicator4(conn)
                graph_data(conn)                  


        if __name__ == '__main__':
            main() 

    return


schedule.every().day.at("00:17").do(job)

while True:
    schedule.run_pending()
    time.sleep(60) # wait one minute