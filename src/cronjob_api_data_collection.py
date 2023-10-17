# #!/usr/bin/env python3
import requests
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import json
import os
from sqlalchemy import func

#Crontab Run Test
#* * * * * /usr/bin/env python3 ~/csca_5028_big_data_final_project/src/cronjob_api_data_collection.py

#Local Run Test
#python3 src/cronjob_api_data_collection.py

app = Flask(__name__)
script = os.path.realpath(__file__)
path = os.path.dirname(script)

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///'+path+'//databases//currency_exchangeRates_db.sqlite3'

db = SQLAlchemy(app)


class currency_exchangeRates_db(db.Model):
    currency_id = db.Column('currency_id', db.Integer, primary_key = True)
    base_currency = db.Column(db.String(5), nullable=False)
    GBP_currency_rate = db.Column(db.String(50), nullable=False)
    CAD_currency_rate = db.Column(db.String(50), nullable=False)
    JPY_currency_rate = db.Column(db.String(50), nullable=False)
    INR_currency_rate = db.Column(db.String(50), nullable=False)
    HKD_currency_rate = db.Column(db.String(50), nullable=False)
    last_update_on = db.Column(db.DateTime,default="datetime.utcnow")

def __init__(self, base_currency,GBP_currency_rate,CAD_currency_rate,JPY_currency_rate,INR_currency_rate,HKD_currency_rate,last_update_on):
   self.base_currency = base_currency
   self.GBP_currency_rate = GBP_currency_rate
   self.CAD_currency_rate = CAD_currency_rate
   self.JPY_currency_rate = JPY_currency_rate
   self.INR_currency_rate = INR_currency_rate
   self.HKD_currency_rate = HKD_currency_rate   
   self.last_update_on = last_update_on   

# '''
# Helper function to get Currency Exchnage Rates
# using API
# '''
def get_Currency_Exchnage_Rates(currency):
    response = requests.get("https://open.er-api.com/v6/latest/"+currency)
    return response.json()

# Get the daily currency updates for "ALL", "AUD", "AMD","ARS","DZD","AZN","BSD"
if __name__ == "__main__":

 with app.app_context():
     db.create_all()   
     currency_list =  ["ALL", "AUD", "AMD","ARS","DZD","AZN","BSD"]    
     for currency in currency_list:        
        currentDate = datetime.now().strftime('%Y-%m-%d')
        currency_exists = currency_exchangeRates_db.query.filter(func.DATE(currency_exchangeRates_db.last_update_on)==currentDate,currency_exchangeRates_db.base_currency==currency).first()
        if currency_exists is None: 

            response= get_Currency_Exchnage_Rates(currency)  

            if response["result"]=="success" :

                ts = int(response["time_last_update_unix"])
                last_update_date = datetime.utcfromtimestamp(ts)     
                CurrencyExchnageRate = currency_exchangeRates_db(
                                                                base_currency=response['base_code'], 
                                                                GBP_currency_rate=str(response["rates"]["GBP"]), 
                                                                CAD_currency_rate=str(response["rates"]["CAD"]),
                                                                JPY_currency_rate=str(response["rates"]["JPY"]),
                                                                INR_currency_rate=str(response["rates"]["INR"]),
                                                                HKD_currency_rate=str(response["rates"]["HKD"]),
                                                                last_update_on=last_update_date
                                                            )
                db.session.add(CurrencyExchnageRate)
                db.session.commit()     
                 
            
             
