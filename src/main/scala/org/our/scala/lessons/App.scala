package org.our.scala.lessons

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.our.scala.lessons.GeneralFunctions.{import_csv_with_schema, import_csv_without_schema}
import org.our.scala.lessons.SchemasToUse.schema_races

 * @author ${mirallers}
 
object App {
  System.setProperty("hadoop.home.dir", "C:\\Users\\your_user\\")
  val spark = SparkSession
    .builder()
    .appName("appName")
    .master("local[*]")
    .getOrCreate()

  val ruta_csv="C:\\una_ruta_al_csv\\races.csv"
  def main(args : Array[String]) {
    println( "Comenzando la lectura de nuestro csv")
    val df= import_csv_without_schema(spark,ruta_csv)
    //val df = import_csv_with_schema(spark,schema_races,ruta_csv)
    df.show(truncate = false)
    df.printSchema()

    // Ejemplos de transformaciones (Descomentar para probar) DAG Directed Acyclic Graph, grafico acíclico dirigido
    // El select sirve para seleccionar las columnas que queramos de nuestro dataframe.
    
    val resultadoSelect = df.select("name", "date")
    println("Resultado Select:")
    resultadoSelect.show()
    

    // Transformación 1: El filter sirve para filtrar un dataframe mediante una condicion en una columna.
    
    val resultadoFilter = df.filter(col("year") > 2009)
    println("Resultado Filter:")
    resultadoFilter.show()
    

    // Transformación 2: El group by sirve para agrupar un dataframe por los valores de cierta columna.
    
    val resultadoGroupBy = df.groupBy("round").agg(avg("year"))
    println("Resultado GroupBy:")
    resultadoGroupBy.show()
    

    //  Transformación 3: Sirve para ordenar un dataframe mediante una columna.
    
    val resultadoOrderBy = df.orderBy(desc("year"))
    println("Resultado OrderBy:")
    resultadoOrderBy.show()
    


    //  Transformación 4: Sirve para eliminar filas duplicadas de un dataframe.
    
    val resultadoDistinct = df.distinct()
    println("Resultado Distinct:")
    resultadoDistinct.show()
    


    // Transformación 5: Sirve para eliminar una columna
    
    val resultadoDropColumn = df.drop("time")
    println("Resultado DropColumn:")
    resultadoDropColumn.show()
    


    // Transformación 6: Sirve para renombrar una columna
    
    val resultadoRenameColumn = df.withColumnRenamed("raceId", "newRaceId")
    println("Resultado RenameColumn:")
    resultadoRenameColumn.show()
    


    // Transformación 7: Sirve para rellenar los valores null por lo que queramos
    
    val resultadoFillNA = df.na.fill("N/A")
    println("Resultado FillNA:")
    resultadoFillNA.show()
    

    // Transformación 8: Sirve para añadir una columna aplicando tranformaciones a otras.
    
     *  val resultadoWithColumn = df.withColumn("newColumn", concat(col("year"), lit("-"), col("round")))
     *  println("Resultado withColumn:")
     *  resultadoWithColumn.show()
     

    // WithColumn es una de las transformaciones más usadas, pues da versatilidad y suele ser la pieza central.
    // Aquí vemos algunos ejemplos del potencial que tiene:

    // Combinación 1: Añadir un literal
    
    val resultadoWithColumnLit = df.withColumn("nuevaColumnaLit", lit("valorLiteral"))
    resultadoWithColumnLit.show()
    

    // Combinación 2: Concatenar columnas de tipo String
    
    val resultadoWithColumnConcat = df.withColumn("nuevaColumnaConcat", concat(col("name"), lit("_"), col("date")))
    resultadoWithColumnConcat.show()
    

    // Combinación 3: Aplicar una condición con when y otherwise
    
    val resultadoWithColumnWhen = df.withColumn("nuevaColumnaWhen", when(col("year") > 2010, "Posterior a 2010").otherwise("Anterior o igual a 2010"))
    resultadoWithColumnWhen.show()
    

    // Combinación 4: Utilizar expresiones SQL
    
    val resultadoWithColumnExpr = df.withColumn("nuevaColumnaExpr", expr("year + round"))
    resultadoWithColumnExpr.show()
    

    // Combinación 5: Reemplazar valores con regexp_replace
    
    val resultadoWithColumnRegexpReplace = df.withColumn("nuevaColumnaRegexpReplace", regexp_replace(col("name"), "Grand Prix", "GP"))
    resultadoWithColumnRegexpReplace.show()
    

    // Combinación 6: Extraer subcadena con substring
    
    val resultadoWithColumnSubstring = df.withColumn("nuevaColumnaSubstring", substring(col("name"), 1, 3))
    resultadoWithColumnSubstring.show()
    

    // Combinación 7: Convertir a mayúsculas con upper
    
    val resultadoWithColumnUpper = df.withColumn("nuevaColumnaUpper", upper(col("name")))
    resultadoWithColumnUpper.show()
    

    // Combinación 8: Dividir columna con split
    
    val resultadoWithColumnSplit = df.withColumn("nuevaColumnaSplit", split(col("name"), " "))
    resultadoWithColumnSplit.show()
    

    // Combinación 9: Coalesce para valores no nulos.
    // Da la primera fila que no sea nula en las columnas seleccionadas
    
    val resultadoWithColumnCoalesce = df.withColumn("nuevaColumnaCoalesce", coalesce(col("name"), col("url"), lit("SinValor")))
    resultadoWithColumnCoalesce.show()
    

    // Combinación 10: Extraer partes con regexp_extract
    
    val resultadoWithColumnRegexpExtract = df.withColumn("nuevaColumnaRegexpExtract", regexp_extract(col("url"), "\\d+", 0))
    resultadoWithColumnRegexpExtract.show()
    

    spark.stop()
  }

}
