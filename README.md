Mars
======

### Overview
mars is a tool for backing up multiple MySQL databases with multiples options. The backups are outputted as a .tar.gz and are stored locally, there is also support for retention in days/weeks/months


### Usage

```
$ go run mars.go --help

  -additionals string
    	Additional parameters that will be appended to mysqldump command
  -batchsize int
    	Split mysqldumps in order to get each file contains batchsize number of records (default 1000000)
  -bind string
    	Port of the mysql server to connect to (default "3306")
  -daily-rotation int
    	Number of backups on the daily rotation (default 5)
  -databases string
    	List of databases as comma seperated values to dump. OBS: If not specified, --all-databases is the default (default "--all-databases")
  -dbthreshold int
    	Do not split mysqldumps, if total rowcount of tables in database is less than dbthreshold value for whole database (default 10000000)
  -excluded-databases string
    	List of databases excluded to be excluded. OBS: Only valid if -databases is not specified
  -forcesplit
    	Split schema and data dumps even if total rowcount of tables in database is less than dbthreshold value. if false one dump file will be created
  -hostname string
    	Hostname of the mysql server to connect to (default "localhost")
  -montly-rotation int
    	Number of backups on the montly rotation (default 1)
  -mysqldump-path string
    	Absolute path for mysqldump executable. (default "/usr/bin/mysqldump")
  -output-dir string
    	Default is the value of os.Getwd(). The backup files will be placed to output-dir /{DATABASE_NAME}/{DATABASE_NAME}_{TABLENAME|SCHEMA|DATA|ALL}_{TIMESTAMP}.sql
  -password string
    	password of the mysql server to connect to (default "1234")
  -tablethreshold int
    	Do not split mysqldumps, if rowcount of table is less than dbthreshold value for table (default 5000000)
  -test
    	test
  -username string
    	username of the mysql server to connect to (default "root")
  -verbosity int
    	0 = only errors, 1 = important things, 2 = all (default 2)
  -weekly-rotation int
    	Number of backups on the weekly rotation (default 2)

```

### Structure

```
**mysqldump-path (default os.Getwd() )** / `/daily/XXXX-XX-XX/database/*`
**mysqldump-path (default os.Getwd() )** / `/weekly/XXXX-XX-XX/database/*`
**mysqldump-path (default os.Getwd() )** / `/montly/XXXX-XX-XX/datavase/*`

```
