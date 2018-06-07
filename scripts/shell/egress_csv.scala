
// table to read from
val TableName = "flights"

// config options
val KineticaOptions = Map(
    "kinetica-jdbcurl" -> "jdbc:simba://gpudb:9292;ParentSet=test",
    "kinetica-username" -> "",
    "kinetica-password" -> "",
    "kinetica-desttablename" -> TableName,
    "connector-numparitions" -> "4")

val tableDF = spark.read.format("com.kinetica.spark").options(KineticaOptions).load()

println(s"Schema for table: ${TableName}")
tableDF.printSchema()

println(s"Writing output lines. (rows = ${tableDF.count()})")
tableDF.coalesce(1).write.mode("overwrite").option("header", "true").csv("output_csv")
