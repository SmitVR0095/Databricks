// Databricks notebook source
// MAGIC %md
// MAGIC # INTRODUCCIÓN A SCALA

// COMMAND ----------

// MAGIC %md
// MAGIC ## Tema 01: Conceptos básicos de Scala

// COMMAND ----------

// MAGIC %md
// MAGIC ###Estructuras Condicionales

// COMMAND ----------

/*1. Dado un número, imprimir "Positivo" si es mayor que cero, "Negativo" si es menor que cero, o "Cero" si es igual a cero.*/
var numero = 0
if (numero > 0) {
  println("Positivo")
} else if (numero < 0) {
  println("Negativo")
} else {
  println("Cero")
}

// COMMAND ----------

  def valNumero(numero: Int): String = {
    if (numero > 0) "Positivo"
    else if (numero < 0) "Negativo"
    else "Cero"
  }

// COMMAND ----------

var num1 = -10
valNumero (num1)

// COMMAND ----------

/*2. Dado un carácter, imprimir "Es vocal" si es una vocal (a, e, i, o, u), o "No es vocal" si no lo es*/
var caracter = '-'
if ("aeiou".contains(caracter.toLower)) {
  println("Es vocal")
} else {
  println("No es vocal")
}

// COMMAND ----------

  def valVocal(caracter: String): String = {
    if ("aeiouAEIOU".contains(caracter)) "Es vocal"
    else "No es vocal"
  }

// COMMAND ----------

var x = "e"
valVocal (x)

// COMMAND ----------

/*3. Dado un número, imprimir "Dígito" si es un número de un solo dígito, o "Número de múltiples dígitos" si tiene más de un dígito.*/
var x = 1
if (x >= -9 && x <= 9) {
  println("Dígito")
} else {
  println("Número de múltiples dígitos")
}

// COMMAND ----------

def valCantDig(numero: Int): String = {
    if (numero >= -9 && numero <= 9) "Dígito"
    else "Número de múltiples dígitos"
  }

// COMMAND ----------

var x = 2
valCantDig (x)

// COMMAND ----------

/*4. Dada una cadena de texto, imprimir "Cadena vacía" si está vacía, o "Cadena no vacía" si contiene al menos un carácter.*/
var cadena = "asda"
if (cadena.isEmpty) {
  println("Cadena vacía")
} else {
  println("Cadena no vacía")
}

// COMMAND ----------

def valCadena(cadena: String): String = {
    if (cadena.isEmpty) "Cadena vacía"
    else "Cadena no vacía"
  }

// COMMAND ----------

var x = ""
valCadena (x)

// COMMAND ----------

/*5. Dado un año, imprimir "Año bisiesto" si es divisible entre 4 pero no entre 100, o si es divisible entre 400. En caso contrario, imprimir "Año no bisiesto".*/
val anio = 1995
if ((anio % 4 == 0 && anio % 100 != 0) || (anio % 400 == 0)) {
  println("Año bisiesto")
} else {
  println("Año no bisiesto")
}

// COMMAND ----------

def valAnioBisiesto(anio: Int): String = {
    if ((anio % 4 == 0 && anio % 100 != 0) || anio % 400 == 0) "Año bisiesto"
    else "Año no bisiesto"
  }

// COMMAND ----------

var x = 1995
valAnioBisiesto (x)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Estructuras de Iteración

// COMMAND ----------

/*1. Imprimir los números del 10 al 1 en orden descendente*/
def imprimirNumerosDescendentes(): Unit = {
  for (i <- 10 to 1 by -1) {
    println(i)
  }
}

// Llamada
imprimirNumerosDescendentes()

// COMMAND ----------

/*2. Imprimir los elementos de un arreglo de enteros*/
def imprimirArregloEnteros(arreglo: Array[Int]): Unit = {
  for (elemento <- arreglo) {
    println(elemento)
  }
}

// Llamada
imprimirArregloEnteros(Array(1, 2, 3, 4, 5))

// COMMAND ----------

/*3. Calcular la potencia de un número utilizando un bucle for*/
def calcularPotencia(base: Int, exponente: Int): Int = {
  var resultado = 1
  for (_ <- 1 to exponente) {
    resultado *= base
  }
  resultado
}

// Llamada
println(calcularPotencia(2, 3)) // 2^3 = 8

// COMMAND ----------

/*4. Imprimir los números impares del 1 al 50 utilizando un bucle do-while*/
def imprimirImparesDoWhile(): Unit = {
  var i = 1
  do {
    if (i % 2 != 0) {
      println(i)
    }
    i += 1
  } while (i <= 50)
}

// Llamada
imprimirImparesDoWhile()

// COMMAND ----------

/*5. Calcular la suma de los primeros n números naturales utilizando un bucle while*/
def calcularSumaPrimerosN(n: Int): Int = {
  var suma = 0
  var i = 1
  while (i <= n) {
    suma += i
    i += 1
  }
  suma
}

// Llamada
println(calcularSumaPrimerosN(10)) // 1+2+...+10 = 55

// COMMAND ----------

// MAGIC %md
// MAGIC ### Funciones en Scala

// COMMAND ----------

/*1.	Escribe una función en Scala llamada "obtenerPromedio" que tome una lista de números enteros como argumento y devuelva el promedio de esos números.*/
def obtenerPromedio(numeros: List[Int]): Double = {
  if (numeros.isEmpty) 0.0
  else numeros.sum.toDouble / numeros.size
}

// Llamada
println(obtenerPromedio(List(10, 20, 30, 40, 50))) // Salida: 30.0

// COMMAND ----------

/*2.	Escribe una función en Scala llamada "concatenarCadenas" que tome dos cadenas como argumento y las concatene.*/
def concatenarCadenas(cadena1: String, cadena2: String): String = {
  cadena1 + cadena2
}

// Llamada
println(concatenarCadenas("Hola, ", "Mundo!")) // Salida: Hola, Mundo!

// COMMAND ----------

/*3.	Escribe una función en Scala llamada "esPalindromo" que tome una cadena como argumento y devuelva true si es un palíndromo y false si no lo es.*/
def esPalindromo(cadena: String): Boolean = {
  val normalizada = cadena.replaceAll("\\s+", "").toLowerCase
  normalizada == normalizada.reverse
}

// Llamada
println(esPalindromo("Anita lava la tina")) // Salida: true
println(esPalindromo("Scala"))             // Salida: false

// COMMAND ----------

/*4.	Escribe una función en Scala llamada "duplicarElementos" que tome una lista de enteros como argumento y devuelva una nueva lista con cada elemento duplicado.*/
def duplicarElementos(lista: List[Int]): List[Int] = {
  lista.map(_ * 2)
}

// Llamada
println(duplicarElementos(List(1, 2, 3, 4))) // Salida: List(2, 4, 6, 8)

// COMMAND ----------

/*5.	Escribe una función en Scala llamada "esCapicua" que tome un número entero como argumento y devuelva true si es capicúa y false si no lo es.*/
def esCapicua(numero: Int): Boolean = {
  val strNumero = numero.toString
  strNumero == strNumero.reverse
}

// Llamada
println(esCapicua(121))  // Salida: true
println(esCapicua(123))  // Salida: false

// COMMAND ----------

// MAGIC %md
// MAGIC ## Tema 02: Listas

// COMMAND ----------

/*1. Crea una lista inmutable de números enteros del 1 al 10. Filtra y genera una nueva lista que contenga solo los números que son mayores que 5.*/
def filtrarNumero(lista: List[Int], num: Int): List[Int] = {
  lista.filter(_ > num)
}

var lista = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
var filter = 5
println(filtrarNumero(lista, filter))

// COMMAND ----------

/*2. Crea una lista inmutable de números del 1 al 100. Calcula la suma de todos los números en la lista y muestra el resultado.*/
def Suma_Elementos(inicio: Int, fin: Int): Int = {
  val lista = List.range(inicio, fin + 1) 
  lista.sum
}

var inicio = 1
var fin = 100
println(Suma_Elementos(inicio, fin)) 

// COMMAND ----------

/*3. Crea una lista inmutable de nombres de personas. Utiliza map para crear una nueva lista que contenga todos los nombres en mayúsculas.*/
def Convertir_A_Mayusculas(nombres: List[String]): List[String] = {
  nombres.map(_.toUpperCase)
}

var nombres = List("Ana", "Juan", "María", "Luis")
println(Convertir_A_Mayusculas(nombres))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Tema 03: Conjuntos

// COMMAND ----------

/*1. Crea un conjunto inmutable de números del 1 al 5. Añade los números 6 y 7 al conjunto. Imprime el conjunto original y el actualizado.*/
def Agregar_Elementos_Al_Conjunto(conjuntoInicial: Set[Int], elementosAAgregar: Set[Int]): Set[Int] = {
  val conjuntoActualizado = conjuntoInicial ++ elementosAAgregar
  // Mostramos
  conjuntoActualizado
}

var conjuntoInicial = Set(1, 2, 3, 4, 5)
var elementos = Set(6, 7)
println(Agregar_Elementos_Al_Conjunto(conjuntoInicial, elementos)) 

// COMMAND ----------

/*2. Crea un conjunto de números del 1 al 10. Filtra los números impares y genera un nuevo conjunto con esos números. Imprime el conjunto resultante.*/
def Obtener_Numero_Impares(conjunto: Set[Int]): Set[Int] = {
  conjunto.filter(_ % 2 != 0)
}

var conjunto = Set(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
println(Obtener_Numero_Impares(conjunto))

// COMMAND ----------

/*3. Crea dos conjuntos: uno con los días de la semana y otro con días de fin de semana. Encuentra la intersección de ambos conjuntos y muestra los días que son fines de semana.*/
def Interseccion_Conjuntos(conjunto1: Set[String], conjunto2: Set[String]): Set[String] = {
  conjunto1.intersect(conjunto2)
}

var conjunto1 = Set("lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo")
var conjunto2 = Set("sábado", "domingo")
println(Interseccion_Conjuntos(conjunto1, conjunto2)) 

// COMMAND ----------

// MAGIC %md
// MAGIC ## Tema 04: Mapas

// COMMAND ----------

/*1. Crea un mapa inmutable de productos y sus precios. Añade un nuevo producto y muestra el mapa original y el actualizado.*/
def mapaConProductoAgregado(): Unit = {
  val productos = Map("Manzana" -> 0.5, "Plátano" -> 0.3, "Pera" -> 0.7)
  
  // Crear un nuevo mapa con el producto añadido
  val mapaActualizado = productos + ("Naranja" -> 0.6)
  
  // Mostrar el mapa original y el actualizado
  println("Mapa original: " + productos)
  println("Mapa actualizado: " + mapaActualizado)
}

// Llamada
mapaConProductoAgregado()

// COMMAND ----------

/*2. Usa un mapa de productos y precios para filtrar solo aquellos que cuestan más de 0.4. Imprime el mapa resultante.*/
def filtrarProductosMasCostosos(): Map[String, Double] = {
  val productos = Map("Manzana" -> 0.5, "Plátano" -> 0.3, "Pera" -> 0.7, "Naranja" -> 0.6)
  
  // Filtrar los productos cuyo precio es mayor que 0.4
  productos.filter(_._2 > 0.4)
}

// Llamada
println(filtrarProductosMasCostosos())

// COMMAND ----------

// MAGIC %md
// MAGIC ## Tema 05: Extra RDD

// COMMAND ----------

// 1. Filtrar elementos mayores que un valor en un RDD
val rdd = sc.parallelize(1 to 10)

// Filtrar elementos mayores que 5
val filtrados = rdd.filter(_ > 5)
println(s"Elementos mayores que 5: ${filtrados.collect().mkString(", ")}")

// COMMAND ----------

// 2. Calcular el promedio de un RDD
val rdd = sc.parallelize(Seq(10, 20, 30, 40, 50))

// Calcular el promedio
val suma = rdd.sum()
val conteo = rdd.count()
val promedio = if (conteo > 0) suma / conteo else 0

println(s"El promedio de los elementos es: $promedio")

// COMMAND ----------

// 3. Crear un RDD con palabras y contar la cantidad de veces que aparece cada palabra
val rdd = sc.parallelize(Seq("manzana", "pera", "manzana", "naranja", "pera", "manzana"))

// Contar la cantidad de veces que aparece cada palabra
val conteoPalabras = rdd.map(palabra => (palabra, 1)).reduceByKey(_ + _)
conteoPalabras.collect().foreach { case (palabra, conteo) =>
  println(s"Palabra: $palabra, Cantidad: $conteo")
}

// COMMAND ----------

// 4. Dado un RDD con números enteros, encontrar el valor máximo y mínimo
val rdd = sc.parallelize(Seq(10, 20, 5, 40, 30))

// Encontrar el valor máximo y mínimo
val maximo = rdd.max()
val minimo = rdd.min()

println(s"Valor máximo: $maximo")
println(s"Valor mínimo: $minimo")

// COMMAND ----------

// 5. Crear un RDD con números enteros repetidos y eliminar los duplicados
val rdd = sc.parallelize(Seq(1, 2, 2, 3, 4, 4, 5))

// Eliminar duplicados
val sinDuplicados = rdd.distinct()
println(s"RDD sin duplicados: ${sinDuplicados.collect().mkString(", ")}")

// COMMAND ----------

// 6. Dado un RDD con palabras, convertir todas las palabras a minúsculas
val rdd = sc.parallelize(Seq("Manzana", "PERA", "nARANJA", "Pera", "MANZANA"))

// Convertir todas las palabras a minúsculas
val palabrasMinusculas = rdd.map(_.toLowerCase)
println(s"Palabras en minúsculas: ${palabrasMinusculas.collect().mkString(", ")}")

// COMMAND ----------

// 7. Dado un RDD con números enteros, calcular el producto de todos los elementos
val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5))

// Calcular el producto de todos los elementos
val producto = rdd.reduce(_ * _)
println(s"El producto de todos los elementos es: $producto")

// COMMAND ----------

// 8. Realizar una unión de dos RDDs
val rdd1 = sc.parallelize(Seq(1, 2, 3))
val rdd2 = sc.parallelize(Seq(4, 5, 6))

// Realizar la unión de los dos RDDs
val unionRDD = rdd1.union(rdd2)
println(s"Unión de los RDDs: ${unionRDD.collect().mkString(", ")}")

// COMMAND ----------

// 9. Realizar una intersección de dos RDDs
val rdd1 = sc.parallelize(Seq(1, 2, 3, 4))
val rdd2 = sc.parallelize(Seq(3, 4, 5, 6))

// Realizar la intersección de los dos RDDs
val interseccionRDD = rdd1.intersection(rdd2)
println(s"Intersección de los RDDs: ${interseccionRDD.collect().mkString(", ")}")

// COMMAND ----------

// 10. Calcular el producto cartesiano de dos RDDs
val rdd1 = sc.parallelize(Seq(1, 2))
val rdd2 = sc.parallelize(Seq("A", "B"))

// Calcular el producto cartesiano de los dos RDDs
val productoCartesiano = rdd1.cartesian(rdd2)
println("Producto cartesiano:")
productoCartesiano.collect().foreach { case (x, y) =>
  println(s"($x, $y)")
}


// COMMAND ----------

// MAGIC %md
// MAGIC # FUNCIONES Y PYSPARK CON SCALA

// COMMAND ----------

// MAGIC %md
// MAGIC ## Tema 01: Scala con PySpark

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.types.{StringType, IntegerType, DoubleType}

// COMMAND ----------

val dfCase = spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .schema(
        StructType(
            Array(
                StructField("case_id", IntegerType, true),
                StructField("province", StringType, true),
                StructField("city", StringType, true),
                StructField("group", StringType, true),
                StructField("infection_case", StringType, true),
                StructField("confirmed", IntegerType, true),
                StructField("latitude", StringType, true),
                StructField("longitude", StringType, true)
            )
        )
    )
    .load("dbfs:/FileStore/Case.csv")

// Mostrar datos
dfCase.show()

// COMMAND ----------

val dfTopCities = dfCase.groupBy("province", "city")
    .agg(sum("confirmed").as("total_confirmed"))
    .orderBy(desc("total_confirmed"))
    .limit(3)

dfTopCities.show()

// COMMAND ----------

val dfPatientInfo = spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .schema(
        StructType(
            Array(
                StructField("patient_id", IntegerType, true),
                StructField("sex", StringType, true),
                StructField("age", StringType, true),
                StructField("country", StringType, true),
                StructField("province", StringType, true),
                StructField("city", StringType, true),
                StructField("infection_case", StringType, true),
                StructField("infected_by", StringType, true),
                StructField("contact_number", IntegerType, true),
                StructField("symptom_onset_date", StringType, true),
                StructField("confirmed_date", StringType, true),
                StructField("released_date", StringType, true),
                StructField("deceased_date", StringType, true),
                StructField("state", StringType, true)
            )
        )
    )
    .load("dbfs:/FileStore/PatientInfo.csv")
    .dropDuplicates()

dfPatientInfo.show()

// COMMAND ----------

val dfInfectedBy = dfPatientInfo.filter($"infected_by".isNotNull)
dfInfectedBy.show()

// COMMAND ----------

val dfFemalePatients = dfInfectedBy.filter($"sex" === "female")
    .drop("released_date", "deceased_date")

dfFemalePatients.show()

// COMMAND ----------

val dfPartitioned = dfFemalePatients.repartition(2)

dfPartitioned.write
    .mode("overwrite")
    .partitionBy("province")
    .parquet("dbfs:/FileStore/PatientInfoPartitioned")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Tema 02: UDF con Scala

// COMMAND ----------

// MAGIC %md
// MAGIC ### Ejercicio 1:
// MAGIC Concepto: Crear una UDF avanzada para convertir una cadena en mayúsculas y eliminar los espacios en blanco alrededor.

// COMMAND ----------

//Completar
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

// Crear la sesión de Spark
val spark = SparkSession.builder()
  .appName("PracticaUDF")
  .getOrCreate()

import spark.implicits._

// Datos de ejemplo
val data1 = Seq("  hola mi nombre es smit  ", "Aprendiento SCALA", " Quinta clase con Layla ")
val df1 = data1.toDF("cadena")

// Definir la función avanzada
def procesarCadena(cadena: String): String = {
  if (cadena != null) cadena.trim.toUpperCase else null
}

// Registrar la UDF
val procesarCadenaUDF = udf(procesarCadena _)

// Aplicar la UDF al DataFrame
val dfResultado1 = df1.withColumn("cadena_procesada", procesarCadenaUDF($"cadena"))

// Mostrar el resultado
dfResultado1.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Ejercicio 2:
// MAGIC Concepto: Crear una UDF avanzada para contar el número de palabras en una cadena.

// COMMAND ----------

//Completar
// Definir la función avanzada
def contarPalabras(cadena: String): Int = {
  if (cadena != null && cadena.trim.nonEmpty) cadena.trim.split("\\s+").length
  else 0
}

// Registrar la UDF
val contarPalabrasUDF = udf(contarPalabras _)

// Aplicar la UDF al DataFrame
val dfResultado2 = df1.withColumn("numero_palabras", contarPalabrasUDF($"cadena"))

// Mostrar el resultado
dfResultado2.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Ejercicio 3:
// MAGIC Concepto: Crear una UDF avanzada para calcular el promedio de una lista de números, la cual deberá convertirse a un DF.

// COMMAND ----------

// Datos de ejemplo: listas de números
val data2 = Seq(
  (1, Seq(10.0, 20.0, 30.0)),
  (2, Seq(5.5, 15.5, 25.5, 35.5)),
  (3, Seq()),
  (4, Seq(100.0))
)
val df2 = data2.toDF("id", "numeros")

// Definir la función avanzada para calcular el promedio
def calcularPromedio(numeros: Seq[Double]): Double = {
  if (numeros != null && numeros.nonEmpty) {
    numeros.sum / numeros.size
  } else {
    0.0 
  }
}

// Registrar la UDF
val calcularPromedioUDF = udf(calcularPromedio _)

// Aplicar la UDF al DataFrame
val dfResultado3 = df2.withColumn("promedio", calcularPromedioUDF($"numeros"))

// Mostrar el resultado
dfResultado3.show(truncate = false)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Ejercicio 4:
// MAGIC Concepto: Crear una UDF avanzada para verificar si una cadena contiene solo dígitos numéricos.

// COMMAND ----------

//Completar
// Datos de ejemplo
val data3 = Seq(
  (1, "12345"),
  (2, "Smit123"),
  (3, "  "),
  (4, "67890"),
  (5, null),
  (6, "123.45")
)
val df3 = data3.toDF("id", "cadena")

// Definir la función avanzada para verificar si la cadena contiene solo dígitos
def contieneSoloDigitos(cadena: String): Boolean = {
  if (cadena != null && cadena.trim.nonEmpty) cadena.matches("\\d+")
  else false
}

// Registrar la UDF
val contieneSoloDigitosUDF = udf(contieneSoloDigitos _)

// Aplicar la UDF al DataFrame
val dfResultado4= df3.withColumn("solo_digitos", contieneSoloDigitosUDF($"cadena"))

// Mostrar el resultado
dfResultado4.show(truncate = false)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Ejercicio 5:
// MAGIC Concepto: Crear una UDF avanzada para concatenar dos cadenas y agregar un separador personalizado.

// COMMAND ----------

//Completar
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import spark.implicits._
import org.apache.spark.sql.functions.{udf, lit}
// Datos de ejemplo
val data4 = Seq(
  (1, "Smit", "Villafranca"),
  (2, "Spark", "Scala"),
  (3, "Python", "R"),
  (4, null, "Programación"),
  (5, "Data", null)
)
val df4 = data4.toDF("id", "cadena1", "cadena2")

// Definir la función avanzada para concatenar dos cadenas con un separador personalizado
def concatenarConSeparador(cadena1: String, cadena2: String, separador: String): String = {
  (cadena1, cadena2) match {
    case (null, _) | (_, null) => ""  // Manejo de valores nulos
    case _ => cadena1 + separador + cadena2
  }
}

// Registrar la UDF
val concatenarConSeparadorUDF = udf(concatenarConSeparador _)

// Aplicar la UDF al DataFrame con un separador personalizado
val dfResultado5 = df4.withColumn("concatenado", concatenarConSeparadorUDF($"cadena1", $"cadena2", lit("-")))

// Mostrar el resultado
dfResultado5.show(truncate = false)
