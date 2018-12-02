package com.kinetica.spark;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import com.thedeanda.lorem.Lorem;

public class KineticaSubmitSqlTest {
    
    @Test
    public void testImportSqlJob() throws Exception {

        try (SparkSession sess = SparkSession.builder()
                .master("local")
                .appName(SparkKineticaDriver.class.getName())
                .enableHiveSupport()
                .getOrCreate())
        {
            String[] args = new String[] { "src/test/resources/sql-test.properties" };
            new SparkKineticaDriver(args).start(sess);
        }
    }
    
    @Test
    public void testQuery() throws AnalysisException {

        try(SparkSession sess = SparkSession.builder()
            .master("local")
            .appName(SparkKineticaDriver.class.getName())
            .getOrCreate() )
        {
            Dataset<Row> df = sess.read().json("src/test/resources/employees.json");
            df.createGlobalTempView("employees");
            sess.sql("SELECT name, salary FROM global_temp.employees ORDER BY name").show();
        }
    }
    
    @Test
    public void testCreateTable() throws Exception {
        try(SparkSession sess = SparkSession.builder()
                .master("local")
                .appName(SparkKineticaDriver.class.getName())
                .enableHiveSupport()
                .getOrCreate() )
        {
            Dataset<Row> df = createTestTable(sess);
            df.createGlobalTempView("test_table");
            sess.sql("DROP TABLE IF EXISTS default.sample_table");
            sess.sql("CREATE TABLE default.sample_table AS SELECT * FROM global_temp.test_table");
        }
    }

    public static Dataset<Row> createTestTable(SparkSession spark) {

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("test_name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("test_timestamp", DataTypes.TimestampType, true));
        fields.add(DataTypes.createStructField("test_double", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("test_long", DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("test_date", DataTypes.DateType, true));
        StructType schema = DataTypes.createStructType(fields);

        ArrayList<Row> rows = new ArrayList<>();
        Timestamp nowTs = Timestamp.valueOf(LocalDateTime.now());
        Date nowDt = Date.valueOf(LocalDate.now());
        rows.add(RowFactory.create(Lorem.getName(), nowTs, 1.5D, 55L, nowDt));
        rows.add(RowFactory.create(Lorem.getName(), null, null, null, null));
        rows.add(RowFactory.create(Lorem.getName(), nowTs, 2.5D, 66L, nowDt));

        return spark.createDataFrame(rows, schema);
    }
    
}
