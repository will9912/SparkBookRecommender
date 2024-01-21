from pyspark.sql import SparkSession

# Crea una SparkSession
spark = SparkSession.builder.appName("Read Oracle Data").getOrCreate()

# Define la URL JDBC de Oracle
url = f"jdbc:oracle:thin:@inserta aquí el host de Oracle:inserta el puerto:inserta aquí el SID"

# Define el nombre de la clase del controlador JDBC de Oracle
driver = "oracle.jdbc.driver.OracleDriver"

# Define las propiedades de conexión JDBC de Oracle
properties = {
    "user": "inserta aquí el usuario de Oracle",
    "password": "inserta aquí la contraseña de Oracle",
    "driver": driver
}

# Define la consulta SQL de Oracle para leer datos
query = """
SELECT
--insertar los column_names
FROM base_oracle
"""

# Lee los datos de Oracle como un DataFrame
df = spark.read.jdbc(url=url, table=query, properties=properties)

# Transform
sql_code = """
-- INSERTAR CÓDIGO DE TRANSFORMACIÓN AQUÍ
"""
df_transformed = spark.sql(sql_code)

# Escribe los datos en formato CSV
csv_name = "datos_base.csv"
df_transformed.write.csv(csv_name)


print(f"Los datos de Oracle se leyeron correctamente y se guardaron como {csv_name}")