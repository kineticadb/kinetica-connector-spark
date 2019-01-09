
// table to read from
val TableName = "flights"

// config options
val KineticaOptions = Map(
    "database.jdbc_url" -> "jdbc:kinetica://gpudb:9191;ParentSet=test",
    "database.username" -> "",
    "database.password" -> "",
    "table.name" -> TableName,
    "spark.num_partitions" -> "4")
    
val tableDF = spark.read.format("com.kinetica.spark").options(KineticaOptions).load()

println(s"Schema for table: ${TableName}")
tableDF.printSchema()

println(s"Writing output lines. (rows = ${tableDF.count()})")
tableDF.coalesce(1).write.mode("overwrite").option("header", "true").csv("output_csv")
