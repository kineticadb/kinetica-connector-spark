
CREATE or replace TABLE test."s-avro_test-20171220" (
    bool_val INTEGER (int8),
    event_dt TYPE_TIMESTAMP,
    event_ts TYPE_TIMESTAMP,
    full_name VARCHAR,
    test_double DOUBLE,
    test_long LONG
);

CREATE or replace TABLE test."s-avro_test-20180130" (
    bool_val INTEGER (int8),
    event_dt TYPE_TIMESTAMP,
    event_ts TYPE_TIMESTAMP(shard_key),
    full_name VARCHAR,
    test_double DOUBLE,
    test_long LONG
);


create or replace TABLE "test.template"."avro_test.20180230" (
    full_name VARCHAR NOT NULL,
    test_double DOUBLE NOT NULL,
    test_extra INTEGER NULL,
    test_long LONG NOT NULL,
    test_float DOUBLE NOT NULL,
    test_int LONG NOT NULL,
    bool_val INTEGER (int8) NOT NULL,
    event_dt TYPE_TIMESTAMP NULL,
    event_ts TYPE_TIMESTAMP(shard_key) NOT NULL
);
