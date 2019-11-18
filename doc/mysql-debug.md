# mac debug mysql

## 下载编译

```zsh
git clone https://github.com/percona/percona-server.git
cd /Users/rsy56640/ndb/percona-server
mkdir build
cd build
```

```cmake
cmake \
-DCMAKE_INSTALL_PREFIX=/Users/rsy56640/ndb/mysql_data/percona-8 \
-DMYSQL_DATADIR=/Users/rsy56640/ndb/mysql_data/percona-8/data \
-DSYSCONFDIR=/Users/rsy56640/ndb/mysql_data/percona-8 \
-DMYSQL_UNIX_ADDR=/Users/rsy56640/ndb/mysql_data/percona-8/data/mysql.sock \
-DWITH_DEBUG=1  \
-DWITHOUT_CSV_STORAGE_ENGINE=1 \
-DWITHOUT_BLACKHOLD_STORAGE_ENGINE=1 \
-DWITHOUT_FEDERATED_STORAGE_ENGINE=1 \
-DWITHOUT_ARCHIVE_STORAGE_ENGINE=1 \
-DWITHOUT_MRG_MYISAM_STORAGE_ENGINE=1 \
-DWITHOUT_NDBCLUSTER_STORAGE_ENGINE=1 \
-DWITHOUT_TOKUDB_STORAGE_ENGINE=1 \
-DWITHOUT_TOKUDB=1 \
-DWITHOUT_ROCKSDB_STORAGE_ENGINE=1 \
-DWITHOUT_ROCKSDB=1 \
-DDOWNLOAD_BOOST=1 \
-DWITH_BOOST=/Users/rsy56640/ndb/boost_1_69_0 ..
```

> 要求ndb文件夹内有 boost1_69.tar.gz

```zsh
make -j 4
make install -j 4
```


## 安装启动与赋权

```zsh
vim /Users/rsy56640/ndb/mysql_data/percona-8/my.cnf

cd /Users/rsy56640/ndb/mysql_data/percona-8/bin

./mysqld --basedir=/Users/rsy56640/ndb/mysql_data/percona-8 --datadir=/Users/rsy56640/ndb/mysql_data/percona-8/data --initialize-insecure --user=rsy56640

cd /Users/rsy56640/ndb/mysql_data/percona-8

mkdir /Users/rsy56640/ndb/mysql_data/percona-8/tmp

./bin/mysqld_safe --defaults-file=my.cnf --user=rsy56640&

./bin/mysql --socket=/Users/rsy56640/ndb/mysql_data/percona-8/data/mysql.sock -uroot -hlocalhost -p

CREATE USER 'dbatman'@'%' IDENTIFIED BY 'dbatman';
CREATE USER 'dbatman'@'localhost' IDENTIFIED BY 'dbatman';
GRANT ALL ON *.* TO 'dbatman'@'%' WITH GRANT OPTION;
GRANT ALL ON *.* TO 'dbatman'@'localhost' WITH GRANT OPTION;

./bin/mysql -udbatman -pdbatman -h127.0.0.1 -P3306
```

> 注意 mysqld_safe 会不断创建 mysqld，需要 kill 掉   
> 每次 debug 时都得先开 mysqld_safe，要不然开不了 mysql。。。   
> 开启 mysql 命令行后，要 kill 掉所有 mysqld   
> 命令行是 mysql，vscode 上是 mysqld   



## 配置 vs debug

添加如下配置
```json
{
    // Use IntelliSense to learn about possible attributes.
    // Hover to view descriptions of existing attributes.
    // For more information, visit: https://go.microsoft.com/fwlink/?linkid=830387
    "version": "0.2.0",
    "configurations": [{
        "name": "Launch",
        "type": "lldb", // install "codelldb"
        "request": "launch",
        "program": "/Users/rsy56640/ndb/mysql_data/percona-8/bin/mysqld",
        "args": ["--defaults-file=/Users/rsy56640/ndb/mysql_data/percona-8/my.cnf", "--user=rsy56640"],
    }]
}
```

vscode -> debug -> start debugging

设置断点，执行 sql 触发


如果没有对上就 lsof -i:3306，把 mysqld kill -9   
参考：https://serverfault.com/questions/477448/mysql-keeps-crashing-innodb-unable-to-lock-ibdata1-error-11




### my.cnf

```
[client]
port                            = 3306
socket                          = /Users/rsy56640/ndb/mysql_data/percona-8/data/mysql.sock
default-character-set           = utf8mb4

[mysql]
default-character-set           = utf8mb4

[mysqld]
default_authentication_plugin   = mysql_native_password
transaction-isolation           = READ-COMMITTED
character-set-server            = utf8mb4
tmpdir                          = /Users/rsy56640/ndb/mysql_data/percona-8/tmp
port                            = 3306
socket                          = /Users/rsy56640/ndb/mysql_data/percona-8/data/mysql.sock
pid_file                        = /Users/rsy56640/ndb/mysql_data/percona-8/data/mysql9999.pid
read_only                       = OFF
max_connect_errors              = 99
max_connections                 = 50000
max_user_connections            = 50000
back_log                        = 512
thread_cache_size               = 800

skip-external-locking
skip-name-resolve
safe-user-create
interactive_timeout             = 360
wait_timeout                    = 360

open_files_limit                = 65535
key_buffer_size                 = 16M
max_allowed_packet              = 512M
table_definition_cache          = 65535
table_open_cache                = 65535
max_length_for_sort_data        = 8M
max_heap_table_size             = 256M
tmp_table_size                  = 256M

datadir                         = /Users/rsy56640/ndb/mysql_data/percona-8/data/
memlock
default-storage-engine          = innodb

innodb_file_per_table
innodb_buffer_pool_size         = 100M
innodb_change_buffering         = all
innodb_data_file_path           = ibdata1:100M:autoextend
innodb_concurrency_tickets      = 1024

innodb_max_dirty_pages_pct      = 50
innodb_flush_method             = O_DIRECT
innodb_log_file_size            = 671088640
innodb_log_files_in_group       = 3


innodb_open_files               = 896
innodb_lock_wait_timeout        = 60
sync_binlog                     = 0
innodb_sync_spin_loops          = 0
innodb_flush_log_at_trx_commit  = 2

server-id                       = 391529999
log-slave-updates
log-bin                         = mysql-bin
relay-log                       = mysql-relay-bin
binlog_format                   = ROW
expire_logs_days                = 7
max_binlog_size                 = 100M
log_bin_trust_function_creators = 1
skip_slave_start
slave_net_timeout               = 20
slave-skip-errors               = 1032,1062
gtid_mode                       = on
enforce-gtid-consistency        = on
log_slave_updates               = on


general-log                     = 0
general_log_file                = general.log

slow-query-log                  = 1
slow_query_log_file             = slow_query.log
long_query_time                 = 1
log-queries-not-using-indexes   = 1

log_error                       = log_error.err

[mysqldump]
quick
max_allowed_packet              = 256M

[mysql]
no-auto-rehash

[myisamchk]
key_buffer_size                 = 8M
sort_buffer_size                = 8M

[mysqlhotcopy]
interactive-timeout
```
