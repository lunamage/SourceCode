create external table t_temp_event_current_haojia ( `profileid` string COMMENT 'from deserializer',
  `tablename` string COMMENT 'from deserializer',
  `ga_datehour` string COMMENT 'from deserializer',
  `ga_productaddstocart` string COMMENT 'from deserializer')
PARTITIONED BY (
  `dt` string,
  `sys` string)
row format SERDE'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
with serdeproperties(
"field.delim"="@^!"
);
