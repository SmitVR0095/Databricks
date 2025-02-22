// Databricks notebook source
// MAGIC %md
// MAGIC # RETO

// COMMAND ----------

// MAGIC %md
// MAGIC ## MÉTODO 01: ARQUETIPO DE PROCESAMIENTO AVANZADO

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.types.{StringType, IntegerType, DoubleType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

// COMMAND ----------

// MAGIC %md
// MAGIC ### Paso 01: Lectura

// COMMAND ----------

//Leemos el archivo de RIESGO_CREDITICIO
var dfRiesgo = spark.read.format("csv").option("header", "true").option("delimiter", ";").schema(
    StructType(
        Array(
            StructField("ID_CLIENTE", IntegerType, true),
            StructField("RIESGO_CENTRAL_1", DoubleType, true),
            StructField("RIESGO_CENTRAL_2", DoubleType, true),
            StructField("RIESGO_CENTRAL_3", DoubleType, true)
        )
    )
).load("dbfs:/FileStore/_spark/RIESGO_CREDITICIO.csv")
//Mostramos los datos
dfRiesgo.show(10)

// COMMAND ----------

// Ruta del archivo JSON
val jsonFilePath = "dbfs:/FileStore/_spark/transacciones_bancarias.json"
// Leer el archivo JSON
val dfTxnBancarias = spark.read.option("multiline", "false").json(jsonFilePath)
// Mostrar el esquema y los datos
dfTxnBancarias.printSchema()
// Explorar datos anidados con select
var dfTxnBancariasFinal = dfTxnBancarias.select(
  col("EMPRESA.ID_EMPRESA").alias("ID_EMPRESA"),
  col("EMPRESA.NOMBRE_EMPRESA").alias("NOMBRE_EMPRESA"),
  col("PERSONA.ID_PERSONA").alias("ID_PERSONA"),
  col("PERSONA.NOMBRE_PERSONA").alias("NOMBRE_PERSONA"),
  col("PERSONA.EDAD").alias("EDAD"),
  col("PERSONA.SALARIO").alias("SALARIO"),
  col("TRANSACCION.FECHA").alias("FECHA"),
  col("TRANSACCION.MONTO").alias("MONTO")
)

dfTxnBancariasFinal.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Paso 02: Modelamiento

// COMMAND ----------

val  dfTransacciones=dfTxnBancariasFinal.select(
  col("ID_PERSONA").alias("ID_PERSONA"),  
  col("ID_EMPRESA").alias("ID_EMPRESA"),
  col("FECHA").alias("FECHA"),
  col("MONTO").alias("MONTO")
)

dfTransacciones.show(10)

// COMMAND ----------

var dfTransacciones_cache = cache(dfTransacciones)

// COMMAND ----------

val  dfPersona=dfTxnBancariasFinal.select(
  col("ID_PERSONA").alias("ID_PERSONA"),  
  col("NOMBRE_PERSONA").alias("NOMBRE_PERSONA"),
  col("EDAD").alias("EDAD"),
  col("SALARIO").alias("SALARIO")
)

dfPersona.show(10)

// COMMAND ----------

val  dfEmpresa=dfTxnBancariasFinal.select(
  col("ID_EMPRESA").alias("ID_EMPRESA"),
  col("NOMBRE_EMPRESA").alias("NOMBRE_EMPRESA")
)

dfEmpresa.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Paso 03: Reglas de Calidad

// COMMAND ----------

// MAGIC %md
// MAGIC #### Transacciones
// MAGIC 1. Validamos que los registros esten completos y cumplan con ciertas regla de calidad

// COMMAND ----------

// Rango permitido para el MONTO
val montoMinimoPermitido = 0

// Calcular las métricas de calidad
val calidadTxn = dfTransacciones.agg(
  count("*").alias("Total_Registros"),
  sum(when(col("ID_PERSONA").isNull, 1).otherwise(0)).alias("ID_PERSONA_Nulos"),
  sum(when(col("ID_EMPRESA").isNull, 1).otherwise(0)).alias("ID_EMPRESA_Nulos"),
  min(col("MONTO")).alias("Monto_Minimo"),
  max(col("MONTO")).alias("Monto_Maximo"),
  sum(when(col("FECHA").isNull, 1).otherwise(0)).alias("Fechas_Nulas")
)
// Obtener los valores calculados fuera del DataFrame de calidad
val montoMinimo = montoMinimoPermitido
// Obtener salario máximo desde calidadPersona
val montoMax = calidadTxn.select("Monto_Maximo").first().getAs[Double]("Monto_Maximo")

// Crear un DataFrame con las métricas en formato de descripción y valor
val resumenCalidad_Txn = calidadTxn.select(
  lit("Total de registros").alias("Métrica"),
  col("Total_Registros").cast("string").alias("Valor")
).union(
  calidadTxn.select(
    lit("ID_PERSONA nulos").alias("Métrica"),
    col("ID_PERSONA_Nulos").cast("string").alias("Valor")
  )
).union(
  calidadTxn.select(
    lit("ID_EMPRESA nulos").alias("Métrica"),
    col("ID_EMPRESA_Nulos").cast("string").alias("Valor")
  )
).union(
  calidadTxn.select(
    lit("Monto mínimo").alias("Métrica"),
    col("Monto_Minimo").cast("string").alias("Valor")
  )
).union(
  calidadTxn.select(
    lit("Monto máximo").alias("Métrica"),
    col("Monto_Maximo").cast("string").alias("Valor")
  )
).union(
  calidadTxn.select(
    lit("Fechas nulas").alias("Métrica"),
    col("Fechas_Nulas").cast("string").alias("Valor")
  )
).union(
  Seq(
    ("Rango de monto", s"[$montoMinimo - $montoMax)")
  ).toDF("Métrica", "Valor")
)

resumenCalidad_Txn.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC 2. Se valida si existe registros duplicados

// COMMAND ----------

// Contar los duplicados en el DataFrame por la combinación de todas las columnas
val duplicados = dfTransacciones.groupBy("ID_PERSONA", "ID_EMPRESA", "MONTO", "FECHA")
  .count()
  .filter(col("count") > 1)  // Filtrar los registros que tienen más de un duplicado

// Mostrar los duplicados encontrados
duplicados.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC 3. Se realiza la limpieza respectiva.

// COMMAND ----------

// Limpiar el DataFrame según las reglas de calidad
var montoMaximoPermitido = 100000
val dfTransaccionLimpio = dfTransacciones.filter(
  col("ID_PERSONA").isNotNull &&
  col("ID_EMPRESA").isNotNull &&
  col("MONTO").between(montoMinimoPermitido, montoMaximoPermitido)
).dropDuplicates(Seq("ID_PERSONA", "ID_EMPRESA", "MONTO", "FECHA"))
dfTransaccionLimpio.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Empresa
// MAGIC 1. Validamos que los registros tengan datos coherentes y completos.

// COMMAND ----------

// Calcular las métricas de calidad
val calidadEmpresa = dfEmpresa.agg(
  count("*").alias("Total_Registros"),
  sum(when(col("ID_EMPRESA").isNull, 1).otherwise(0)).alias("ID_PERSONA_Nulos")
)
// Crear un DataFrame con las métricas en formato de descripción y valor
val resumenCalidad_EMPRESA = calidadEmpresa.select(
  lit("ID_EMPRESA nulos").alias("Métrica"),
  col("ID_PERSONA_Nulos").cast("string").alias("Valor")
)
resumenCalidad_EMPRESA.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC 2. Validamos si hay registros duplicados.

// COMMAND ----------

// Contar los duplicados en base a la columna "ID_EMPRESA"
val duplicados = dfEmpresa.groupBy("ID_EMPRESA")
  .count()
  .filter(col("count") > 1)  // Filtrar los registros que tienen más de un duplicado

// Mostrar los duplicados encontrados
duplicados.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC 3. Realizamos la limpieza respectiva.

// COMMAND ----------

// Eliminar duplicados basados en la clave "ID_PERSONA" primero
val dfEmpresaSinDuplicados = dfEmpresa.dropDuplicates(Seq("ID_EMPRESA"))

// Limpiar el DataFrame según las reglas de calidad
val dfEmpresaLimpio = dfEmpresaSinDuplicados.filter(
  col("ID_EMPRESA").isNotNull
)
dfEmpresaLimpio

dfEmpresaLimpio.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Persona
// MAGIC 1. Revisamos que los registros cumplan ccon ciertas reglas de calidad

// COMMAND ----------

val SalarioMinimoPermitido = 0

// Calcular las métricas de calidad
val calidadPersona = dfPersona.agg(
  count("*").alias("Total_Registros"),
  sum(when(col("ID_PERSONA").isNull, 1).otherwise(0)).alias("ID_PERSONA_Nulos"),
  min(col("SALARIO")).alias("Salario_Minimo"),
  max(col("SALARIO")).alias("Salario_Maximo"),
  min(col("EDAD")).alias("Edad_Minimo"),
  max(col("EDAD")).alias("Edad_Maximo")
)

// Verificar la existencia de registros duplicados basados en la clave "ID_PERSONA"
val registrosDuplicados = dfPersona.groupBy("ID_PERSONA")
  .count()
  .filter(col("count") > 1)  // Filtra registros que aparecen más de una vez
  .agg(sum("count").alias("Total_Registros_Duplicados"))

// Obtener los valores calculados fuera del DataFrame de calidad
val salarioMinimo = SalarioMinimoPermitido
val salarioMax = calidadPersona.select("Salario_Maximo").first().getAs[Double]("Salario_Maximo")
val edadMinima = calidadPersona.select("Edad_Minimo").first().getAs[Long]("Edad_Minimo").toInt
val edadMaxima = calidadPersona.select("Edad_Maximo").first().getAs[Long]("Edad_Maximo").toInt

// Obtener los registros duplicados
val totalDuplicados = if (registrosDuplicados.head().getAs[Long]("Total_Registros_Duplicados") > 0) {
  registrosDuplicados.head().getAs[Long]("Total_Registros_Duplicados")
} else {
  0L
}

// Crear un DataFrame con las métricas en formato de descripción y valor
val resumenCalidad_Persona = calidadPersona.select(
  lit("Total de registros").alias("Métrica"),
  col("Total_Registros").cast("string").alias("Valor")
).union(
  calidadPersona.select(
    lit("ID_PERSONA nulos").alias("Métrica"),
    col("ID_PERSONA_Nulos").cast("string").alias("Valor")
  )
).union(
  calidadPersona.select(
    lit("Salario mínimo").alias("Métrica"),
    col("Salario_Minimo").cast("string").alias("Valor")
  )
).union(
  calidadPersona.select(
    lit("Salario máximo").alias("Métrica"),
    col("Salario_Maximo").cast("string").alias("Valor")
  )  
).union(
  calidadPersona.select(
    lit("Edad mínimo").alias("Métrica"),
    col("Edad_Minimo").cast("string").alias("Valor")
  ) 
).union(
  calidadPersona.select(
    lit("Edad máximo").alias("Métrica"),
    col("Edad_Maximo").cast("string").alias("Valor")
  )  
).union(
  // Agregar el rango de salario y edad en la tabla resumen
  Seq(
    ("Rango de salario", s"[$salarioMinimo - $salarioMax]"),
    ("Rango de edad", s"[$edadMinima - $edadMaxima]"),
    ("Total registros duplicados", totalDuplicados.toString) // Agregar el conteo de duplicados
  ).toDF("Métrica", "Valor")
)

// Mostrar el resumen de calidad de los datos
resumenCalidad_Persona.show(false)


// COMMAND ----------

// MAGIC %md
// MAGIC 2. Realizamos la limpieza respectiva.

// COMMAND ----------

// Definir los límites de calidad
var SalarioMaximoPermitido = 100000
var SalarioMinimoPermitido = 0
var EdadMaximoPermitido = 60
var EdadMinimoPermitido = 0

// Eliminar duplicados basados en la clave "ID_PERSONA" primero
val dfPersonaSinDuplicados = dfPersona.dropDuplicates(Seq("ID_PERSONA"))

// Luego, aplicar las reglas de calidad para limpiar los datos
val dfPersonaLimpio = dfPersonaSinDuplicados.filter(
  col("ID_PERSONA").isNotNull &&  // Verificar que "ID_PERSONA" no sea nulo
  col("SALARIO").between(SalarioMinimoPermitido, SalarioMaximoPermitido) &&  // Verificar rango de salario
  col("EDAD").between(EdadMinimoPermitido + 1, EdadMaximoPermitido - 1)  // Verificar rango de edad
)

// Mostrar el DataFrame limpio después de eliminar duplicados y aplicar las reglas de calidad
dfPersonaLimpio.show(10)


// COMMAND ----------

// MAGIC %md
// MAGIC #### Riesgo

// COMMAND ----------

// Calcular las métricas de calidad
val calidadRiesgo = dfRiesgo.agg(
  count("*").alias("Total_Registros"),
  sum(when(col("ID_CLIENTE").isNull, 1).otherwise(0)).alias("ID_CLIENTE_Nulos"),
  min(col("RIESGO_CENTRAL_1")).alias("RIESGO_CENTRAL_1_Minimo"),
  max(col("RIESGO_CENTRAL_1")).alias("RIESGO_CENTRAL_1_Maximo"),
  min(col("RIESGO_CENTRAL_2")).alias("RIESGO_CENTRAL_2_Minimo"),
  max(col("RIESGO_CENTRAL_2")).alias("RIESGO_CENTRAL_2_Maximo"),
  min(col("RIESGO_CENTRAL_3")).alias("RIESGO_CENTRAL_3_Minimo"),
  max(col("RIESGO_CENTRAL_3")).alias("RIESGO_CENTRAL_3_Maximo")
)

// Obtener los RANGOS CENTRALES
val RiesgoCentral_1_Min = calidadRiesgo.select("RIESGO_CENTRAL_1_Minimo").first().getAs[Double]("RIESGO_CENTRAL_1_Minimo").toInt
val RiesgoCentral_1_Max = calidadRiesgo.select("RIESGO_CENTRAL_1_Maximo").first().getAs[Double]("RIESGO_CENTRAL_1_Maximo").toInt
val RiesgoCentral_2_Min = calidadRiesgo.select("RIESGO_CENTRAL_2_Minimo").first().getAs[Double]("RIESGO_CENTRAL_2_Minimo").toInt
val RiesgoCentral_2_Max = calidadRiesgo.select("RIESGO_CENTRAL_2_Maximo").first().getAs[Double]("RIESGO_CENTRAL_2_Maximo").toInt
val RiesgoCentral_3_Min = calidadRiesgo.select("RIESGO_CENTRAL_3_Minimo").first().getAs[Double]("RIESGO_CENTRAL_3_Minimo").toInt
val RiesgoCentral_3_Max = calidadRiesgo.select("RIESGO_CENTRAL_3_Maximo").first().getAs[Double]("RIESGO_CENTRAL_3_Maximo").toInt

// Crear un DataFrame con las métricas en formato de descripción y valor
val resumenCalidad_Riesgo= calidadRiesgo.select(
  lit("Total de registros").alias("Métrica"),
  col("Total_Registros").cast("string").alias("Valor")
).union(
  calidadRiesgo.select(
    lit("ID_CLIENTE nulos").alias("Métrica"),
    col("ID_CLIENTE_Nulos").cast("string").alias("Valor")
  )
).union(
  // Agregar el rango de salario y edad en la tabla resumen
  Seq(
    ("Rango Riesgo Central 1", s"[$RiesgoCentral_1_Min - $RiesgoCentral_1_Max]"),
    ("Rango Riesgo Central 1", s"[$RiesgoCentral_2_Min - $RiesgoCentral_2_Max]"),
    ("Rango Riesgo Central 1", s"[$RiesgoCentral_3_Min - $RiesgoCentral_3_Max]")
  ).toDF("Métrica", "Valor")
)

resumenCalidad_Riesgo.show(false)

// COMMAND ----------

val dfRiesgoLimpio = dfRiesgo
dfRiesgoLimpio.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Paso 04: Creación UDF

// COMMAND ----------

// Definir la UDF para calcular el riesgo ponderado
val calcularRiesgoPonderado = udf((riesgo1: Double, riesgo2: Double, riesgo3: Double) => {
  (2 * riesgo1 + 3 * riesgo2 + 2 * riesgo3) / 7
})

// Aplicar la UDF para calcular la columna de riesgo ponderado
val dfRiesgo2 = dfRiesgo.withColumn(
  "RIESGO_PONDERADO",
  bround(calcularRiesgoPonderado(col("RIESGO_CENTRAL_1"), col("RIESGO_CENTRAL_2"), col("RIESGO_CENTRAL_3")), 1)
)

// Mostrar el DataFrame con la nueva columna de riesgo ponderado
dfRiesgo2.show(10)

// COMMAND ----------

// Filtamos y nos quedamos con la columna RIESGO_PONDERADO
val dfRiesgoPonderado = dfRiesgo2.select(col("ID_CLIENTE"),col("RIESGO_PONDERADO"))
dfRiesgoPonderado.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Paso 05: Preparación de Tablones

// COMMAND ----------

// MAGIC %md
// MAGIC #### Sub Paso 01: Construnción df1

// COMMAND ----------

val df1 = dfTransaccionLimpio.join(
  dfPersonaLimpio,
  dfTransaccionLimpio("ID_PERSONA") === dfPersonaLimpio("ID_PERSONA"),
  "inner"
).select(
  dfTransaccionLimpio("ID_PERSONA"),
  dfPersonaLimpio("NOMBRE_PERSONA"),
  dfPersonaLimpio("EDAD"),
  dfPersonaLimpio("SALARIO"),
  dfTransaccionLimpio("ID_EMPRESA"),
  dfTransaccionLimpio("FECHA"),
  dfTransaccionLimpio("MONTO") 
)
df1.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Sub Paso 02: Construnción dfTablon

// COMMAND ----------

val dfTablon = df1.join(
  dfEmpresaLimpio,
  df1("ID_EMPRESA") === dfEmpresaLimpio("ID_EMPRESA"),
  "inner"
).select(
  df1("ID_PERSONA"),
  df1("NOMBRE_PERSONA"),
  df1("EDAD"),
  df1("SALARIO"),
  df1("ID_EMPRESA"),
  dfEmpresaLimpio("NOMBRE_EMPRESA"),
  df1("FECHA"),
  df1("MONTO") 
)
dfTablon.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Sub Paso 03: Construnción df2

// COMMAND ----------

val df2 = dfTablon.join(
  dfRiesgoPonderado,
  dfTablon("ID_PERSONA") === dfRiesgoPonderado("ID_CLIENTE"),
  "inner"
).select(
  dfTablon("ID_PERSONA"),
  dfTablon("NOMBRE_PERSONA"),
  dfTablon("EDAD"),
  dfTablon("SALARIO"),
  dfRiesgoPonderado("RIESGO_PONDERADO"),
  dfTablon("ID_EMPRESA"),
  dfTablon("NOMBRE_EMPRESA"),
  dfTablon("FECHA"),
  dfTablon("MONTO") 
)
df2.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Paso 06: Pre - Procesamiento

// COMMAND ----------

val dfTablon1 = df2.filter(
  col("MONTO") > 500 && 
  col("NOMBRE_EMPRESA") === "Amazon"
)
dfTablon1.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Paso 07: Procesamiento

// COMMAND ----------

// MAGIC %md
// MAGIC #### Sub Paso 01: Construcción del Reporte 1

// COMMAND ----------

val dfReporte1 = dfTablon1.filter(
  col("EDAD").between(30, 39) &&
  col("SALARIO").between(1000, 5000)
)
dfReporte1.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Sub Paso 02: Construcción del Reporte 2

// COMMAND ----------

val dfReporte2 = dfTablon1.filter(
  col("EDAD").between(40, 49) &&
  col("SALARIO").between(2500, 7000)
)
dfReporte2.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Sub Paso 03: Construcción del Reporte 3

// COMMAND ----------

val dfReporte3 = dfTablon1.filter(
  col("EDAD").between(50, 60) &&
  col("SALARIO").between(3500, 10000)
)
dfReporte3.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Paso 08: Almacenamiento

// COMMAND ----------

dfReporte1.write
  .option("header", "true")
  .option("delimiter", "|")  
  .option("encoding", "ISO-8859-1")
  .csv("dbfs:///FileStore/_bigdata/output/arquetipo-proc-avanzado/REPORTE_1")

// COMMAND ----------

dfReporte2.write
  .option("header", "true")
  .option("delimiter", "|")  
  .option("encoding", "ISO-8859-1")
  .csv("dbfs:///FileStore/_bigdata/output/arquetipo-proc-avanzado/REPORTE_2")

// COMMAND ----------

dfReporte3.write
  .option("header", "true")
  .option("delimiter", "|")  
  .option("encoding", "ISO-8859-1")
  .csv("dbfs:///FileStore/_bigdata/output/arquetipo-proc-avanzado/REPORTE_3")

// COMMAND ----------

// MAGIC %md
// MAGIC ## MÉTODO 02: ARQUETIPO DE PROCESAMIENTO OPTIMIZADO

// COMMAND ----------

// MAGIC %md
// MAGIC ### Paso 00: Preliminares

// COMMAND ----------

// MAGIC %md
// MAGIC #### 1. Librerias

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.types.{StringType, IntegerType, DoubleType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

// COMMAND ----------

// MAGIC %md
// MAGIC #### 2. Instancia Spark

// COMMAND ----------

val spark = SparkSession.builder()
  .appName("Calidad de Datos y Modelamiento")
  .config("spark.executor.memory", "900g") // Reserva el 90% de la memoria
  .config("spark.executor.cores", "360")   // Reserva el 90% de las CPU
  .getOrCreate()

// COMMAND ----------

// MAGIC %md
// MAGIC #### 3. Declaracion de Funciones de Tuneado

// COMMAND ----------

//Función de reparticionamiento
def reparticionar(df: DataFrame, registrosPorParticion: Int): DataFrame = {
  var dfReparticionado: DataFrame = null
  
  //Obtenemos el número de particiones actuales
  val numeroDeParticionesActuales = df.rdd.getNumPartitions
  
  //Obtenemos la cantidad de registros del dataframe
  val cantidadDeRegistros = df.count()
  
  //Obtenemos el nuevo número de particiones
  val nuevoNumeroDeParticiones = (cantidadDeRegistros / (registrosPorParticion * 1.0)).ceil.toInt
  
  //Reparticionamos
  println(s"Reparticionando a $nuevoNumeroDeParticiones particiones...")
  if (nuevoNumeroDeParticiones > numeroDeParticionesActuales) {
    dfReparticionado = df.repartition(nuevoNumeroDeParticiones)
  } else {
    dfReparticionado = df.coalesce(nuevoNumeroDeParticiones)
  }
  println("Reparticionado completado!")
  
  dfReparticionado
}

// COMMAND ----------

// Implementación de la función Cache
def Cache(df: DataFrame, liberarMemoria: Boolean): DataFrame = {
  if (liberarMemoria) {
    // Validar si el DataFrame está en caché antes de liberarlo
    if (df.storageLevel.useMemory || df.storageLevel.useDisk) {
      print("Liberando cache...")
      df.unpersist(blocking = true)
      println(", cache liberado!")
    } else {
      println("Error: El DataFrame no está almacenado en cache, no se puede liberar.")
    }
    df
  } else {
    // Almacenar en cache
    print("Almacenando en cache...")
    val dfCacheado = df.cache()
    println(", almacenado en cache!")
    dfCacheado
  }
}

// COMMAND ----------

// Implementamos la función checkpoint
def checkpoint(df: DataFrame, nombreArchivo: String = (math.random * 100000000).toString): DataFrame = {
  var dfCheckpoint: DataFrame = null
  
  // Generamos la ruta completa para la carpeta utilizando el nombre proporcionado
  val carpeta = s"dbfs:///FileStore/tmp/$nombreArchivo"
  
  // Guardamos el DataFrame en la carpeta para liberar memoria de la cadena de procesos
  println(s"Aplicando checkpoint en la carpeta: $carpeta...")
  df.write.mode("overwrite").format("parquet").save(carpeta)
  df.unpersist(blocking = true)
  println("Checkpoint aplicado!")
  
  // Volvemos a leerlo
  dfCheckpoint = spark.read.format("parquet").load(carpeta)
  
  return dfCheckpoint
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Paso 01: Lectura

// COMMAND ----------

//Leemos el archivo de RIESGO_CREDITICIO
var dfRiesgo = spark.read.format("csv").option("header", "true").option("delimiter", ";").schema(
    StructType(
        Array(
            StructField("ID_CLIENTE", IntegerType, true),
            StructField("RIESGO_CENTRAL_1", DoubleType, true),
            StructField("RIESGO_CENTRAL_2", DoubleType, true),
            StructField("RIESGO_CENTRAL_3", DoubleType, true)
        )
    )
).load("dbfs:/FileStore/_spark/RIESGO_CREDITICIO.csv")
//Mostramos los datos
dfRiesgo.show(10)

// COMMAND ----------

// Ruta del archivo JSON
val jsonFilePath = "dbfs:/FileStore/_spark/transacciones_bancarias.json"
// Leer el archivo JSON
var dfTxnBancarias = spark.read.option("multiline", "false").json(jsonFilePath)
// Mostrar el esquema y los datos
dfTxnBancarias.printSchema()
// Explorar datos anidados con select
var dfTransacciones = dfTxnBancarias.select(
  col("EMPRESA.ID_EMPRESA").alias("ID_EMPRESA"),
  col("EMPRESA.NOMBRE_EMPRESA").alias("NOMBRE_EMPRESA"),
  col("PERSONA.ID_PERSONA").alias("ID_PERSONA"),
  col("PERSONA.NOMBRE_PERSONA").alias("NOMBRE_PERSONA"),
  col("PERSONA.EDAD").alias("EDAD"),
  col("PERSONA.SALARIO").alias("SALARIO"),
  col("TRANSACCION.FECHA").alias("FECHA"),
  col("TRANSACCION.MONTO").alias("MONTO")
)

dfTransacciones.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Aplicación del Tuneado de Reparticionar

// COMMAND ----------

dfRiesgo.count()


// COMMAND ----------

dfTransacciones.count()

// COMMAND ----------

//Reparticionamos el data set
dfTransacciones = reparticionar(dfTransacciones,10000)
//Mostramos los datos
dfTransacciones.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Aplicación del Tuneado de Cache

// COMMAND ----------

val dfTransaccionesCacheado = Cache(dfTransacciones,false)

// COMMAND ----------

val dfRiesgoCacheado = Cache(dfRiesgo,false)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Paso 02: Modelamiento

// COMMAND ----------

var  dfTransaccion=dfTransaccionesCacheado.select(
  col("ID_PERSONA").alias("ID_PERSONA"),  
  col("ID_EMPRESA").alias("ID_EMPRESA"),
  col("FECHA").alias("FECHA"),
  col("MONTO").alias("MONTO")
)

dfTransacciones.show(10)

// COMMAND ----------

var  dfPersona=dfTransaccionesCacheado.select(
  col("ID_PERSONA").alias("ID_PERSONA"),  
  col("NOMBRE_PERSONA").alias("NOMBRE_PERSONA"),
  col("EDAD").alias("EDAD"),
  col("SALARIO").alias("SALARIO")
)

dfPersona.show(10)

// COMMAND ----------

var  dfEmpresa=dfTransaccionesCacheado.select(
  col("ID_EMPRESA").alias("ID_EMPRESA"),
  col("NOMBRE_EMPRESA").alias("NOMBRE_EMPRESA")
)

dfEmpresa.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Paso 03: Reglas de Calidad

// COMMAND ----------

// MAGIC %md
// MAGIC #### Transacciones

// COMMAND ----------

// Limpiar el DataFrame según las reglas de calidad
val montoMinimoPermitido = 0
val montoMaximoPermitido = 100000
var dfTransaccionLimpio = dfTransaccion.filter(
  col("ID_PERSONA").isNotNull &&
  col("ID_EMPRESA").isNotNull &&
  col("MONTO").between(montoMinimoPermitido, montoMaximoPermitido)
).dropDuplicates(Seq("ID_PERSONA", "ID_EMPRESA", "MONTO", "FECHA"))
dfTransaccionLimpio.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Aplicar Tuneado Checkpoint

// COMMAND ----------

dfTransaccionLimpio = checkpoint(dfTransaccionLimpio,"Transacciones")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Aplicar Tuneado de Reparticionar

// COMMAND ----------

dfTransaccionLimpio.count()

// COMMAND ----------

//Reparticionamos el data set
dfTransaccionLimpio = reparticionar(dfTransaccionLimpio,10000)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Aplicar Tuneado de Cache

// COMMAND ----------

val dfTransaccionLimpioCacheado = Cache(dfTransaccionLimpio,false)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Empresa

// COMMAND ----------

// Eliminar duplicados basados en la clave "ID_PERSONA" primero
val dfEmpresaSinDuplicados = dfEmpresa.dropDuplicates(Seq("ID_EMPRESA"))

// Limpiar el DataFrame según las reglas de calidad
var dfEmpresaLimpio = dfEmpresaSinDuplicados.filter(
  col("ID_EMPRESA").isNotNull
)
dfEmpresaLimpio.show()


// COMMAND ----------

// MAGIC %md
// MAGIC ##### Aplicar Tuneado Checkpoint

// COMMAND ----------

dfEmpresaLimpio = checkpoint(dfEmpresaLimpio,"Empresas")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Aplicar Tuneado de Cache

// COMMAND ----------

val dfEmpresaLimpioCacheado = Cache(dfEmpresaLimpio,false)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Persona

// COMMAND ----------

// Eliminar duplicados basados en la clave "ID_PERSONA" primero
val dfPersonaSinDuplicados = dfPersona.dropDuplicates(Seq("ID_PERSONA"))

// Definir los límites de calidad
val SalarioMaximoPermitido = 100000
val SalarioMinimoPermitido = 0
val EdadMaximoPermitido = 60
val EdadMinimoPermitido = 0

// Luego, aplicar las reglas de calidad para limpiar los datos
var dfPersonaLimpio = dfPersonaSinDuplicados.filter(
  col("ID_PERSONA").isNotNull &&  // Verificar que "ID_PERSONA" no sea nulo
  col("SALARIO").between(SalarioMinimoPermitido, SalarioMaximoPermitido) &&  // Verificar rango de salario
  col("EDAD").between(EdadMinimoPermitido + 1, EdadMaximoPermitido - 1)  // Verificar rango de edad
)

// Mostrar el DataFrame limpio después de eliminar duplicados y aplicar las reglas de calidad
dfPersonaLimpio.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Aplicar Tuneado Checkpoint

// COMMAND ----------

dfPersonaLimpio = checkpoint(dfPersonaLimpio,"Personas")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Aplicar Tuneado de Cache

// COMMAND ----------

val dfPersonaLimpioCacheado = Cache(dfPersonaLimpio,false)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Riesgo

// COMMAND ----------

var dfRiesgoLimpio = dfRiesgoCacheado
dfRiesgoLimpio.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Aplicar Tuneado Checkpoint

// COMMAND ----------

dfRiesgoLimpio = checkpoint(dfRiesgoLimpio,"Riesgo")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Aplicar Tuneado de Cache

// COMMAND ----------

val dfRiesgoLimpioCacheado = Cache(dfRiesgoLimpio,false)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Paso 04: Creación UDF

// COMMAND ----------

// Definir la UDF para calcular el riesgo ponderado
val calcularRiesgoPonderado = udf((riesgo1: Double, riesgo2: Double, riesgo3: Double) => {
  (2 * riesgo1 + 3 * riesgo2 + 2 * riesgo3) / 7
})

// Aplicar la UDF para calcular la columna de riesgo ponderado
var dfRiesgo2 = dfRiesgoLimpioCacheado.withColumn(
  "RIESGO_PONDERADO",
  bround(calcularRiesgoPonderado(col("RIESGO_CENTRAL_1"), col("RIESGO_CENTRAL_2"), col("RIESGO_CENTRAL_3")), 1)
)

// Mostrar el DataFrame con la nueva columna de riesgo ponderado
dfRiesgo2.show(10)

// COMMAND ----------

// Filtamos y nos quedamos con la columna RIESGO_PONDERADO
var dfRiesgoPonderado = dfRiesgo2.select(col("ID_CLIENTE"),col("RIESGO_PONDERADO"))
dfRiesgoPonderado.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Paso 05: Preparación de Tablones

// COMMAND ----------

// MAGIC %md
// MAGIC #### Sub Paso 01: Construnción df1

// COMMAND ----------

var df1 = dfTransaccionLimpioCacheado.join(
  dfPersonaLimpioCacheado,
  dfTransaccionLimpioCacheado("ID_PERSONA") === dfPersonaLimpioCacheado("ID_PERSONA"),
  "inner"
).select(
  dfTransaccionLimpioCacheado("ID_PERSONA"),
  dfPersonaLimpioCacheado("NOMBRE_PERSONA"),
  dfPersonaLimpioCacheado("EDAD"),
  dfPersonaLimpioCacheado("SALARIO"),
  dfTransaccionLimpioCacheado("ID_EMPRESA"),
  dfTransaccionLimpioCacheado("FECHA"),
  dfTransaccionLimpioCacheado("MONTO") 
)
df1.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Sub Paso 02: Construnción dfTablon

// COMMAND ----------

var dfTablon = df1.join(
  dfEmpresaLimpioCacheado,
  df1("ID_EMPRESA") === dfEmpresaLimpioCacheado("ID_EMPRESA"),
  "inner"
).select(
  df1("ID_PERSONA"),
  df1("NOMBRE_PERSONA"),
  df1("EDAD"),
  df1("SALARIO"),
  df1("ID_EMPRESA"),
  dfEmpresaLimpioCacheado("NOMBRE_EMPRESA"),
  df1("FECHA"),
  df1("MONTO") 
)
dfTablon.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Sub Paso 03: Construnción df2

// COMMAND ----------

var df2 = dfTablon.join(
  dfRiesgoPonderado,
  dfTablon("ID_PERSONA") === dfRiesgoPonderado("ID_CLIENTE"),
  "inner"
).select(
  dfTablon("ID_PERSONA"),
  dfTablon("NOMBRE_PERSONA"),
  dfTablon("EDAD"),
  dfTablon("SALARIO"),
  dfRiesgoPonderado("RIESGO_PONDERADO"),
  dfTablon("ID_EMPRESA"),
  dfTablon("NOMBRE_EMPRESA"),
  dfTablon("FECHA"),
  dfTablon("MONTO") 
)
df2.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Paso 06: Pre - Procesamiento

// COMMAND ----------

var dfTablon1 = df2.filter(
  col("MONTO") > 500 && 
  col("NOMBRE_EMPRESA") === "Amazon"
)
dfTablon1.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Aplicar Tuneado Checkpoint

// COMMAND ----------

dfTablon1 = checkpoint(dfTablon1,"TablonFinal")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Aplicar Tuneado de Reparticionar

// COMMAND ----------

dfTablon1.count()

// COMMAND ----------

//Reparticionamos el data set
dfTablon1 = reparticionar(dfTablon1,10000)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Aplicar Tuneado de Cache

// COMMAND ----------

val dfTablon1Cacheado = Cache(dfTablon1,false)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Paso 07: Procesamiento

// COMMAND ----------

// MAGIC %md
// MAGIC #### Sub Paso 01: Construcción del Reporte 1

// COMMAND ----------

var dfReporte1 = dfTablon1Cacheado.filter(
  col("EDAD").between(30, 39) &&
  col("SALARIO").between(1000, 5000)
)
dfReporte1.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Aplicar Tuneado de Cache

// COMMAND ----------

val dfReporte1Cacheado = Cache(dfReporte1,false)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Sub Paso 02: Construcción del Reporte 1

// COMMAND ----------

var dfReporte2 = dfTablon1Cacheado.filter(
  col("EDAD").between(40, 49) &&
  col("SALARIO").between(2500, 7000)
)
dfReporte2.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Aplicar Tuneado de Cache

// COMMAND ----------

val dfReporte2Cacheado = Cache(dfReporte2,false)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Sub Paso 03: Construcción del Reporte 3

// COMMAND ----------

var dfReporte3 = dfTablon1Cacheado.filter(
  col("EDAD").between(50, 60) &&
  col("SALARIO").between(3500, 10000)
)
dfReporte3.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Aplicar Tuneado de Cache

// COMMAND ----------

val dfReporte3Cacheado = Cache(dfReporte3,false)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Paso 08: Almacenamiento

// COMMAND ----------

// MAGIC %md
// MAGIC #### Reporte 1

// COMMAND ----------

dfReporte1Cacheado.write
  .option("header", "true")
  .option("delimiter", "|")  
  .option("encoding", "ISO-8859-1")
  .csv("dbfs:///FileStore/_bigdata/output/arquetipo-proc-avanzado/REPORTE_TUNEADO_1")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Reporte 2

// COMMAND ----------

dfReporte2Cacheado.write
  .option("header", "true")
  .option("delimiter", "|")  
  .option("encoding", "ISO-8859-1")
  .csv("dbfs:///FileStore/_bigdata/output/arquetipo-proc-avanzado/REPORTE_TUNEADO_2")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Reporte 3

// COMMAND ----------

dfReporte3Cacheado.write
  .option("header", "true")
  .option("delimiter", "|")  
  .option("encoding", "ISO-8859-1")
  .csv("dbfs:///FileStore/_bigdata/output/arquetipo-proc-avanzado/REPORTE_TUNEADO_3")
