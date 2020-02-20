#!/bin/bash
set -x
./sql-bench -c 128 -addr 10.40.81.2:4001 -db zpmbenchmark -u zpmbenchmark -p yb@vyC78Buy@g7cbB8 -datas \
../sql/tables_drop.sql,\
../sql/tables_create.sql,\
../sql/insert_sequently_user_hash_partition.sql,\
../sql/insert_sequently_user_no_partition.sql,\
../sql/insert_sequently_user_range_partition.sql,\
../sql/query_range_user_hash_partition.sql,\
../sql/query_range_user_no_partition.sql,\
../sql/query_range_user_range_partition.sql,\
../sql/tables_drop.sql,\
../sql/tables_create.sql,\
../sql/insert_sequently_user_hash_partition.sql,\
../sql/insert_sequently_user_no_partition.sql,\
../sql/insert_sequently_user_range_partition.sql,\
../sql/query_single_user_hash_partition.sql,\
../sql/query_single_user_no_partition.sql,\
../sql/query_single_user_range_partition.sql\
 -sql-count 1000000 -max-time 6000
