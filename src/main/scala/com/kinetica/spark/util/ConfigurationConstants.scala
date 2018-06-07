package com.kinetica.spark.util

object ConfigurationConstants {

    val KINETICA_URL_PARAM: String = "kinetica-url"
    val KINETICA_ENABLEAUTH_PARAM: String = "kinetica-enableauth"
    val KINETICA_USERNAME_PARAM: String = "kinetica-username"
    val KINETICA_PASSWORD_PARAM: String = "kinetica-password"
    val KINETICA_NUMTHREADS_PARAM: String = "kinetica-numthreads"
    val KINETICA_BATCHSIZE_PARAM: String = "kinetica-batchsize"
    val KINETICA_MULTIHEAD_PARAM: String = "kinetica-multihead"
    val KINETICA_UPDATEONEXISTINGPK_PARAM: String = "kinetica-updateonexistingpk"
    val KINETICA_REPLICATEDTABLE_PARAM: String = "kinetica-replicatedtable"
    val KINETICA_IPREGEX_PARAM: String = "kinetica-ipregex"
    val KINETICA_TIMEOUT_PARAM: String = "kinetica-timeoutms"
    val KINETICA_PARTROWS_PARAM: String = "kinetica-partitionrows"

    val KINETICA_USESNAPPY_PARAM: String = "kinetica-usesnappy"
    val KINETICA_RETRYCOUNT_PARAM: String = "kinetica-retrycount"
    val KINETICA_JDBCURL_PARAM: String = "kinetica-jdbcurl"
    val KINETICA_TABLENAME_PARAM: String = "kinetica-desttablename"
    val KINETICA_CREATETABLE_PARAM: String = "kinetica-createtable"
    val KINETICA_TRUNCATETABLE_PARAM: String = "kinetica-truncatetable"
    val KINETICA_USETEMPLATES_PARAM: String = "kinetica-usetemplates"
    val KINETICA_ALTERTABLE_PARAM: String = "kinetica-altertable"
    val KINETICA_MAPTOSCHEMA_PARAM: String = "kinetica-maptoschema"

    val CONNECTOR_DATAPATH_PARAM: String = "connector-datapath"
    val CONNECTOR_SQLFILE_PARAM: String = "connector-sqlfile"
    val CONNECTOR_DATAFORMAT_PARAM: String = "connector-dataformat"
    val CONNECTOR_NUMPARTITIONS_PARAM: String = "connector-numparitions"

    val KINETICA_SSLBYPASSCERTCJECK_PARAM: String = "kinetica-sslbypasscertcheck"
    val KINETICA_TRUSTSTOREJKS_PARAM: String = "kinetica-truststorejks"
    val KINETICA_TRUSTSTOREPASSWORD_PARAM: String = "kinetica-truststorepassword"
    val KINETICA_KEYSTOREP12_PARAM: String = "kinetica-keystorep12"
    val KINETICA_KEYSTOREPASSWORD_PARAM: String = "kinetica-keystorepassword"

    val ACCUMULATOR_NAME: String = "acc_name"

    val LOADERCODEPATH: String = "loadercodepath"
}
