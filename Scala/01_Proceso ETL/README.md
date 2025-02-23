# Introducción a Apache Spark con Scala

## 🌟 ¿Qué es Apache Spark?
Apache Spark es un motor de procesamiento de datos en clúster de código abierto diseñado para velocidad y facilidad de uso. Permite el procesamiento de grandes volúmenes de datos de manera distribuida y eficiente, soportando múltiples lenguajes como Scala, Python, Java y R.

Scala es el lenguaje nativo de Spark y ofrece ventajas de rendimiento al ejecutarse en la JVM.

## 🛠️ Uso de Scala en Databricks
Databricks es una plataforma optimizada para trabajar con Apache Spark. Para usar Scala en Databricks, sigue estos pasos:

1. **Crear un Notebook:**
   - Entra a tu cuenta de Databricks.
   - En la barra lateral izquierda, selecciona "Workspace" y crea un nuevo notebook.
   - Asigna un nombre y elige **Scala** como lenguaje de programación.

2. **Configurar el Cluster:**
   - En la pestaña "Clusters", inicia o crea un nuevo cluster.
   - Asegúrte de que el cluster tiene una versión compatible de Apache Spark.
   - Asigna el notebook al cluster para poder ejecutar código Scala.

## 📝 Comandos Básicos en Spark con Scala

### 🔧 Declaración de Variables en Scala
En Scala, las variables se pueden declarar de dos maneras:

- **`var`**: Permite modificar su valor después de la asignación.
- **`val`**: Crea una constante inmutable.

Ejemplo:
```scala
var nombre = "Apache Spark"
nombre = "Big Data" // Permitido

val version = "3.5.0"
// version = "3.4.0" // Error: no se puede reasignar un val
```

### 🛠️ Creación de una Sesión de Spark
Para trabajar con Spark, se debe inicializar una sesión:
```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("Ejemplo Spark")
  .master("local[*]")
  .getOrCreate()
```
En Databricks, la sesión de Spark ya está creada, por lo que puedes usar `spark` directamente.

## 📚 RDDs, DataFrames y Datasets en Spark

Spark ofrece tres estructuras principales para manejar datos: **RDDs, DataFrames y Datasets**.

### 📊 RDD (Resilient Distributed Dataset)
Los RDDs son la estructura más básica y flexible en Spark. Son inmutables, tolerantes a fallos y distribuidos en un clúster.
```scala
val rdd = spark.sparkContext.parallelize(Seq((1, "Juan"), (2, "María")))
rdd.collect().foreach(println)
```

### 📊 DataFrames
Los DataFrames son estructuras tabulares optimizadas, similares a las tablas SQL, con un mejor rendimiento que los RDDs.
```scala
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

val data = Seq((1, "Juan", 25), (2, "María", 30))
val df: DataFrame = spark.createDataFrame(data).toDF("id", "nombre", "edad")
df.show()
```

### 📊 Datasets
Los Datasets combinan la eficiencia de los DataFrames con la seguridad de tipos de datos de los RDDs. Se usan principalmente con Scala y Java.
```scala
case class Persona(id: Int, nombre: String, edad: Int)
val ds = df.as[Persona]
ds.show()
```

**Diferencias clave:**
| Estructura | Seguridad de tipos | Optimizado | Uso recomendado |
|------------|-------------------|------------|----------------|
| RDD        | ✅ Alta          | ❌ No      | Transformaciones complejas |
| DataFrame  | ❌ Baja          | ✅ Sí      | Grandes volúmenes de datos |
| Dataset    | ✅ Alta          | ✅ Sí      | Seguridad de tipos en Scala |

## 📂 Lectura y Escritura de Datos
```scala
val df = spark.read.option("header", "true").csv("/mnt/datos/datos.csv")
df.write.format("parquet").save("/mnt/datos/output.parquet")
```

## 👁️ Transformaciones y Acciones en Spark
### Transformaciones (Lazy Evaluation)
Las transformaciones no se ejecutan inmediatamente, sino que crean un plan de ejecución que se materializa solo cuando se requiere un resultado.
```scala
val dfFiltrado = df.filter(col("edad") > 25)
val dfSeleccionado = dfFiltrado.select("nombre", "edad")
```

### Acciones (Ejecutan el DAG)
Las acciones disparan la ejecución de las transformaciones y devuelven un resultado.
```scala
df.show() // Muestra los datos
df.count() // Cuenta las filas
df.collect() // Devuelve un array con los datos
```

## 🔢 Operaciones sobre DataFrames
### 👤 Agrupamiento de Datos
```scala
df.groupBy("edad").agg(count("id"), sum("edad"), max("edad")).show()
```

### 🌍 Lectura de JSON con Esquema
```scala
import org.apache.spark.sql.types._
val esquema = StructType(Array(
  StructField("id", IntegerType, true),
  StructField("nombre", StringType, true),
  StructField("edad", IntegerType, true)
))
val dfJson = spark.read.schema(esquema).json("/mnt/datos/datos.json")
dfJson.show()
```

### ➕ Agregar Columnas
```scala
val dfNuevo = df.withColumn("edad_5_anios_despues", col("edad") + 5)
dfNuevo.show()
```

### 🤝 Combinaciones de DataFrames (Join)
```scala
val df1 = Seq((1, "Juan"), (2, "María")).toDF("id", "nombre")
val df2 = Seq((1, "Lima"), (2, "Cusco")).toDF("id", "ciudad")
val dfJoin = df1.join(df2, "id")
dfJoin.show()
```

## 💡 Conclusión
Apache Spark con Scala es una poderosa combinación para el procesamiento distribuido de datos. Scala permite escribir código eficiente y expresivo para manipular grandes volúmenes de datos en Spark.

Para aprender más, visita la documentación oficial de Spark: [https://spark.apache.org/docs/latest/](https://spark.apache.org/docs/latest/)
