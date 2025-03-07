ETL para procesamiento de Datos

Descripción Esta prueba implementa una ETL en Python utilizando Pandas para extraer, transformar y cargar datos desde un archivo Excel con información de películas, inventario, rentas, clientes y tiendas.

Tecnologías 
-Python (lenguaje de programación) 
-Pandas (Manipulación de datos) 
-Logging (Módulos de observabilidad) 
-Numpy (Manejo de nulos y transformaciones)

Arquitectura 
-Extracción: Se leen los datos de un archivo de excel Films_2.xlsx 
-Transformación: Se limpian los espacios en las columnas, se manejan los valores nulos, formatos de fechas 
-Carga: Se ponen todos los datos procesados y limpios y se guardan en nuevos archivos CSV

Justificación del diseño 
-Se aplica la estructura modular con la clase base (ETL) y las subclases para cada tabla 
-Se usa POO como paradigma de programación para estructurar la ETL 
-Se usa Logging para implementar los módulos de observabilidad para ver el paso a paso en la ejecución

Informe 
-Arquitectura de datos y arquetipo de la aplicación 
-Análisis exploratorio de datos 
-Preguntas de negocio con sus respectivas respuestas representadas a través de gráficas