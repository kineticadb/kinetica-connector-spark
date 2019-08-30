package com.kinetica.spark.util;

object ConfigurationConstants {

    val SPARK_DATASOURCE_VERSION: String = "spark.datasource_api_version";
    
    val KINETICA_URL_PARAM: String = "database.url";
    val KINETICA_PRIMARY_URL_PARAM: String = "database.primary_url";
    val KINETICA_JDBCURL_PARAM: String = "database.jdbc_url";
    val KINETICA_STREAMURL_PARAM: String = "database.stream_url";
    val KINETICA_USERNAME_PARAM: String = "database.username";
    val KINETICA_PASSWORD_PARAM: String = "database.password";
    val KINETICA_RETRYCOUNT_PARAM: String = "database.retry_count";
    val KINETICA_TIMEOUT_PARAM: String = "database.timeout_ms";

    val KINETICA_MULTIHEAD_PARAM: String = "ingester.multi_head";
    val KINETICA_IPREGEX_PARAM: String = "ingester.ip_regex";
    val KINETICA_NUMTHREADS_PARAM: String = "ingester.num_threads";
    val KINETICA_BATCHSIZE_PARAM: String = "ingester.batch_size";
    val KINETICA_USESNAPPY_PARAM: String = "ingester.use_snappy";
    val KINETICA_USETIMEZONE_PARAM: String = "ingester.use_timezone";
    val KINETICA_ERROR_HANDLING_PARAM: String = "ingester.fail_on_errors";


    val CONNECTOR_NUMPARTITIONS_PARAM: String = "spark.num_partitions";
    val CONNECTOR_ROWSPERPARTITION_PARAM: String = "spark.rows_per_partition";

    val KINETICA_SSLBYPASSCERTCHECK_PARAM: String = "ssl.bypass_cert_check";
    val KINETICA_TRUSTSTOREJKS_PARAM: String = "ssl.truststore_jks";
    val KINETICA_TRUSTSTOREPASSWORD_PARAM: String = "ssl.truststore_password";
    val KINETICA_KEYSTOREP12_PARAM: String = "ssl.keystore_p12";
    val KINETICA_KEYSTOREPASSWORD_PARAM: String = "ssl.keystore_password";

    val KINETICA_UPDATEONEXISTINGPK_PARAM: String = "table.update_on_existing_pk";
    val KINETICA_REPLICATEDTABLE_PARAM: String = "table.is_replicated";
    val KINETICA_TRUNCATE_TO_SIZE: String = "table.truncate_to_size";

    val KINETICA_TABLENAME_PARAM: String = "table.name";
    val KINETICA_TABLENAME_CONTAINS_SCHEMA_PARAM: String = "table.name_contains_schema";
    val KINETICA_CREATETABLE_PARAM: String = "table.create";
    val KINETICA_TRUNCATETABLE_PARAM: String = "table.truncate";
    val KINETICA_USETEMPLATES_PARAM: String = "table.use_templates";
    val KINETICA_ALTERTABLE_PARAM: String = "table.append_new_columns";
    val KINETICA_MAPTOSCHEMA_PARAM: String = "table.map_columns_by_name";

    val CONNECTOR_DATAPATH_PARAM: String = "source.data_path";
    val CONNECTOR_SQLFILE_PARAM: String = "source.sql_file";
    val CONNECTOR_DATAFORMAT_PARAM: String = "source.data_format";
    val KINETICA_CSV_HEADER: String = "source.csv_header";


    val ACCUMULATOR_NAME: String = "acc_name";

    val LOADERCODEPATH: String = "loadercodepath";
    val KINETICA_DRYRUN: String = "ingester.analyze_data_only";
    val KINETICA_FLATTEN_SCHEMA: String = "ingester.flatten_source_schema";
}
