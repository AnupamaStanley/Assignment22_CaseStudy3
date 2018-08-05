import org.apache.spark.sql.SparkSession

object SensorCaseStudy {
  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.master("local")
      .appName("spark session example")
      .getOrCreate()

    val HVACData = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true")
                        .load("F:\\PDF Architect\\HVAC.csv")
    HVACData.show(5)
    import sparkSession.implicits._

    HVACData.createOrReplaceTempView("HVAC_Data")
    val newhvac = HVACData.select($"Date", $"Time", $"TargetTemp".cast("Int"), $"ActualTemp".
                      cast("Int"), $"System".cast("Int"), $"SystemAge".cast("Int"), $"BuildingID")
    val newcolhvac = sparkSession.sql("select *,IF((targettemp - actualtemp) > 5, '1', IF" +
                        "((targettemp - actualtemp) < -5, '1', 0)) AS tempchange from HVAC_Data").toDF()
    newcolhvac.createOrReplaceTempView("newColHvac")
   newcolhvac.printSchema()

    val buildingData = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true")
      .load("F:\\PDF Architect\\building.csv").toDF()
    buildingData.createOrReplaceTempView("Building_Data")
    buildingData.show(5)

     val joinedDF = newcolhvac.as("HD").join(buildingData.as("BD"),
             $"BD.BuildingID" === $"HD.BuildingID").filter($"tempchange" === 1).groupBy("Country").count().show()
  }
}
