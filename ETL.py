#Importar las librerias
from abc import ABC, abstractmethod
import logging
import numpy as np
import pandas as pd
import pyspark.pandas as ps
from pyspark.sql import SparkSession

# Configurar logging para los modulos de observabilidad (fecha y nivel de mensaje)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#Definir la claseq
class ETL(ABC):
    def __init__(self, file_path, sheet_name):
        self.file_path = file_path
        self.sheet_name = sheet_name
        self.data = None
        
#Extraer los datos de Excel 
    def extract(self):
        logging.info(f'Extrayendo datos de {self.sheet_name}')
        self.data = pd.read_excel(self.file_path, sheet_name=self.sheet_name)
        return self.data

#Limpieza de los datos
    def clean_column_names(self):
        self.data.columns = self.data.columns.str.strip()
    
    def clean_numeric_columns(self, numeric_columns):
        for col in numeric_columns:
            if col in self.data.columns:
                self.data[col] = self.data[col].astype(str).str.extract(r'(\d+\.\d+|\d+)')[0]
                self.data[col] = pd.to_numeric(self.data[col], errors='coerce')
    
    def replace_null_values(self, null_columns):
        for col in null_columns:
            if col in self.data.columns:
                self.data[col].replace(['NULL', 'null', ''], np.nan, inplace=True)
        self.data.dropna(axis=1, how='all', inplace=True) 
    
    def clean_datetime_columns(self, datetime_columns):
        for col in datetime_columns:
            if col in self.data.columns:
                self.data[col] = self.data[col].astype(str).str.strip()
                self.data[col] = pd.to_datetime(self.data[col], errors='coerce')
    
    def clean_data(self, numeric_columns=[], null_columns=[], datetime_columns=[]):
        logging.info(f'Limpiando datos de {self.sheet_name}')
        self.clean_column_names()
        self.clean_numeric_columns(numeric_columns)
        self.replace_null_values(null_columns)
        self.clean_datetime_columns(datetime_columns)
        return self.data

#La clase abstracta hereda de la clase ETL
    @abstractmethod
    def transform(self):
        pass

#Guarda los datos que se transformaron en un CSV 
    def load(self, output_path):
        logging.info(f'Cargando datos transformados en {output_path}')
        self.data.to_csv(output_path, index=False)

#ETL para la tabla Film
class film(ETL):
    def transform(self):
        logging.info('Transformando datos de Film')
        self.clean_data(numeric_columns=['release_year', 'rental_rate', 'replacement_cost', 'num_voted_users'],
                        null_columns=['original_language_id'],
                        datetime_columns=['last_update'])
        return self.data
    
#ETL para la tabla inventory   
class inventory(ETL):
    def transform(self):
        logging.info('Transformando datos de Inventory')
        self.clean_data(numeric_columns=['store_id'], datetime_columns=['last_update'])
        return self.data

#ETL para la tabla rental   
class rental(ETL):
    def transform(self):
        logging.info('Transformando datos de Rental')
        self.clean_data(datetime_columns=['return_date', 'last_update'], null_columns=['return_date'])
        self.data.dropna(subset=['return_date'], inplace=True)      
        return self.data

#ETL para la tabla customer   
class customer(ETL):
    def transform(self):
        logging.info('Transformando datos de Customer')
        self.clean_data(null_columns=['customer_id_old', 'segment'],
                        datetime_columns=['last_update'])
        return self.data

#ETL para la tabla store   
class store(ETL):
    def transform(self):
        logging.info('Transformando datos de Store')
        self.clean_data(datetime_columns=['last_update'])
        return self.data


# Inicializar Spark
spark = SparkSession.builder.appName("ETL_Films").getOrCreate()

file_path = "Films_2 .xlsx"

tables = {
    "film": film,
    "inventory": inventory,
    "rental": rental,
    "customer": customer,
    "store": store
}

# Ejecutar ETLs
for sheet_name, etl_class in tables.items():
    etl = etl_class(file_path, sheet_name)
    data_extracted = etl.extract()
    data_cleaned = etl.clean_data()
    data_transformed = etl.transform()
    etl.load(f"{sheet_name}_cleaned.csv")