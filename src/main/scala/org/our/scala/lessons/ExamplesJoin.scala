package org.our.scala.lessons
import org.apache.spark.sql.SparkSession

object ExamplesJoin {

  System.setProperty("hadoop.home.dir", "C:\\Users\\your_user\\")
  val spark = SparkSession
    .builder()
    .appName("appName")
    .master("local[*]")
    .getOrCreate()
  def main(args : Array[String]): Unit = {
    // Ejemplo de DataFrame 1
    val data1 = Seq(
      (1, "Alice", "Departmento A"),
      (2, "Bob", "Departmento B"),
      (3, "Charlie", "Departmento A"),
      (4, "Pablo", "Departmento C")
    )
    val columns1 = Seq("id", "nombre", "departamento")
    val df1 = spark.createDataFrame(data1).toDF(columns1: _*)

    // Ejemplo de DataFrame 2
    val data2 = Seq(
      (4, "Martinez", "Almeria"),
      (5, "Navarro", "Murcia"),
      (6, "Rodriguez", "Barcelona"),
      (7, "Lora", "Valencia")
    )
    val columns2 = Seq("id", "apellido", "ciudad")
    val df2 = spark.createDataFrame(data2).toDF(columns2: _*)

    // Ejemplo de union de tipo 'inner'

    //val resultadoInner = df1.join(df2, Seq("id"), "inner")
    val resultadoInner=df1.join(df2,Seq("id"),"inner")
    println("Resultado Inner Join:")
    resultadoInner.show()


    // Ejemplo de union de tipo 'left'
    
    val resultadoLeft = df1.join(df2, Seq("id"), "left")
    println("Resultado Left Join:")
    resultadoLeft.show()
    

    // Ejemplo de union de tipo 'right'
    
    val resultadoRight = df1.join(df2, Seq("id"), "right")
    println("Resultado Right Join:")
    resultadoRight.show()
    

    // Ejemplo de union de tipo 'outer' o 'full'
    
    val resultadoOuter = df1.join(df2, Seq("id"), "outer")
    println("Resultado Outer Join:")
    resultadoOuter.show()
    

    // Ejemplo de union utilizando union()
    
    val resultadoUnion = df1.union(df2)
    println("Resultado Union:")
    resultadoUnion.show()
    

    // Cierra la sesión de Spark al finalizar
    spark.stop()
  }

}
