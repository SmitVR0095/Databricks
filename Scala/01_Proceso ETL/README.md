# 🔥 Optimización y Patrones Avanzados en Apache Spark

## 🚀 Introducción
La optimización en Apache Spark es clave para mejorar el rendimiento y eficiencia en el procesamiento de grandes volúmenes de datos. En este documento exploraremos técnicas avanzadas como:

- **Repartition & Coalesce** 🧩
- **Cache & Persist** ⚡
- **Checkpointing** ✅

También proporcionaremos ejemplos prácticos y cuándo **NO** usarlos. 

---

## 🔄 Repartition & Coalesce

### 📌 ¿Qué es `repartition()`?
`repartition(n)` redistribuye los datos en `n` particiones, utilizando un **shuffle** costoso. Se recomienda en los siguientes casos:

✅ Cuando se necesita una distribución **más equilibrada** de los datos.  
✅ Para mejorar el **paralelismo** en operaciones intensivas.  
✅ Cuando se leen archivos con un número ineficiente de particiones.

🔴 **Ejemplo de uso**:
```scala
val df = spark.read.parquet("/data/dataset.parquet")
val dfRepartitioned = df.repartition(10) // Redistribuye en 10 particiones
```

### 📌 `coalesce(n)`, una alternativa eficiente
A diferencia de `repartition()`, `coalesce(n)` **reduce** el número de particiones sin un shuffle completo. Es útil en los siguientes casos:

✅ Después de una transformación que reduce los datos significativamente.  
✅ Para mejorar el rendimiento en escrituras a disco.  

🔴 **Ejemplo de uso**:
```scala
val dfOptimized = df.repartition(10).coalesce(5) // Reduce a 5 particiones sin shuffle completo
```

🚨 **No usar `coalesce()` si necesitas un balance uniforme de datos**, ya que puede generar particiones desiguales.

---

## ⚡ Cache & Persist

### 📌 `cache()`: Almacenar temporalmente en memoria
`cache()` almacena el **DataFrame en memoria RAM** para acelerar múltiples reutilizaciones.

✅ Útil cuando se reutiliza un DataFrame varias veces en una sesión.  
✅ Reduce el tiempo de cómputo en operaciones repetitivas.  

🔴 **Ejemplo de uso**:
```scala
val dfCached = df.cache()
dfCached.show() // Primera ejecución carga los datos en memoria
dfCached.count() // Segunda ejecución será más rápida
```

🚨 **No usar `cache()` si los datos son demasiado grandes para la memoria**, ya que puede generar errores de `OutOfMemory`.

### 📌 `persist()`: Control de almacenamiento en memoria y disco
`persist(StorageLevel.MEMORY_AND_DISK)` permite elegir cómo almacenar los datos:

| Nivel de Persistencia | Ubicación | Evita pérdida de datos |
|-----------------------|-----------|-----------------------|
| `MEMORY_ONLY`       | Solo RAM  | ❌ No |
| `MEMORY_AND_DISK`   | RAM y Disco | ✅ Sí |
| `DISK_ONLY`         | Solo Disco | ✅ Sí |

🔴 **Ejemplo de uso**:
```scala
import org.apache.spark.storage.StorageLevel
val dfPersisted = df.persist(StorageLevel.MEMORY_AND_DISK)
```

🚨 **Usar `persist()` en lugar de `cache()` cuando los datos no caben en memoria**.

---

## ✅ Checkpointing: Persistencia a largo plazo

### 📌 ¿Qué es `checkpoint()`?
`checkpoint()` guarda los datos en **disco HDFS** o almacenamiento distribuido, eliminando la dependencia de DAG anterior.

✅ Útil en flujos de datos largos o iterativos (como algoritmos de ML).  
✅ Reduce la acumulación de linaje de RDDs, evitando problemas de memoria.  

🔴 **Ejemplo de uso**:
```scala
spark.sparkContext.setCheckpointDir("/tmp/checkpoints")
val dfCheckpointed = df.checkpoint()
```

🚨 **No usar `checkpoint()` para pequeños DataFrames temporales**, ya que introduce I/O innecesario.

---

## 📚 Recursos Adicionales
📖 Para más información, consulta la documentación oficial de Spark:  
🔗 [Optimización en Spark](https://spark.apache.org/docs/latest/tuning.html)  

🎯 Con estas estrategias, puedes mejorar significativamente el rendimiento de Spark y evitar cuellos de botella. 🚀
