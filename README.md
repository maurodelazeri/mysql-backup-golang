Mars
======

### Overview
mars is a tool for backing up multiple MySQL databases with multiples options. The backups are outputted as a .tar.gz and are stored locally, there is also support for retention in days/weeks/months


### Usage

```
$ go run mars.go --help
  -hostname string
    	Hostname of the mysql server to connect to (default "localhost")
  -bind string
    	Port of the mysql server to connect to (default "3306")
  -password string
    	password of the mysql server to connect to (default "1234")
  -username string
    	username of the mysql server to connect to (default "root")
  -additionals string
    	Additional parameters that will be appended to mysqldump command
  -tablethreshold int
    	Do not split mysqldumps, if rowcount of table is less than dbthreshold value for table (default 5000000)      
  -batchsize int
    	Split mysqldumps in order to get each file contains batchsize number of records (default 1000000)
  -databases string
    	List of databases as comma seperated values to dump. OBS: If not specified, --all-databases is the default (default "--all-databases")
  -dbthreshold int
    	Do not split mysqldumps, if total rowcount of tables in database is less than dbthreshold value for whole database (default 10000000)
  -excluded-databases string
    	List of databases excluded to be excluded. OBS: Only valid if -databases is not specified
  -forcesplit
    	Split schema and data dumps even if total rowcount of tables in database is less than dbthreshold value. if false one dump file will be created
  -mysqldump-path string
    	Absolute path for mysqldump executable. (default "/usr/bin/mysqldump")
  -output-dir string
    	Default is the value of os.Getwd(). The backup files will be placed to output-dir {DATE/{DATABASE_NAME}/{DATABASE_NAME}_{TABLENAME|SCHEMA|DATA|ALL}_{TIMESTAMP}.sql
  -daily-rotation int
    	Number of days of retention (default 5)   	
  -daily-rotation-files int
    	Number of files on the daily retention (default 5)   	
  -weekly-rotation int
    	Number of weeks of retention (default 2)
  -weekly-rotation-files int
    	Number of files on the weekly retention (default 2)
  -monthly-rotation int
    	Number of months of retention (default 1)
  -monthly-rotation-files int
    	Number of files on the monthly retention (default 1)       
  -verbosity int
    	0 = only errors, 1 = important things, 2 = all (default 2)      
  -test
    	test
```

### Rotation folders structure

```
**mysqldump-path (default os.Getwd() )** / `/daily/XXXX-XX-XX/{DATABASE_NAME}/{DATABASE_NAME}_{TABLENAME|SCHEMA|DATA|ALL}_{TIMESTAMP}.tar.gz`
**mysqldump-path (default os.Getwd() )** / `/weekly/XXXX-XX-XX/{DATABASE_NAME}/{DATE}/{DATABASE_NAME}_{TABLENAME|SCHEMA|DATA|ALL}_{TIMESTAMP}.tar.gz`
**mysqldump-path (default os.Getwd() )** / `/monthly/XXXX-XX-XX/{DATABASE_NAME}_{TABLENAME|SCHEMA|DATA|ALL}_{TIMESTAMP}.tar.gz`

```

### Example
Running a backup of only one database:

$go run mars.go --databases "mysql"

```
Running with parameters
{
	"HostName": "localhost",
	"Bind": "3306",
	"UserName": "root",
	"Password": "1234",
	"Databases": [
		"mysql"
	],
	"ExcludedDatabases": [],
	"DatabaseRowCountTreshold": 10000000,
	"TableRowCountTreshold": 5000000,
	"BatchSize": 1000000,
	"ForceSplit": false,
	"AdditionalMySQLDumpArgs": "",
	"Verbosity": 2,
	"MySQLDumpPath": "/usr/bin/mysqldump",
	"OutputDirectory": "/home/mauro/Downloads/mysql-dump-goland",
	"DefaultsProvidedByUser": true,
	"ExecutionStartDate": "2017-08-05T22:39:26.473773337-04:00",
	"DailyRotation": 5,
	"DailyRotationFiles": 5,
	"WeeklyRotation": 2,
	"WeeklyRotationFiles": 2,
	"MonthlyRotation": 1,
	"MonthlyRotationFiles": 1
}
Running on operating system : linux
Processing Database : mysql
Getting tables for database : mysql
30 tables retrived : mysql
options.ForceSplit (false) && totalRowCount (2102) <= options.DatabaseRowCountTreshold (10000000)
Generating single file backup : mysql
mysqldump is being executed with parameters : -hlocalhost -uroot -p1234 -r/home/mauro/Downloads/mysql-dump-goland/daily/2017-08-05/mysql-2017-08-05/mysql_ALL_20170805.sql mysql
mysqldump output is : 
Compressing table file : /home/mauro/Downloads/mysql-dump-goland/daily/2017-08-05/mysql-2017-08-05/mysql_ALL_20170805.sql
Single file backup successfull : mysql
Processing done for database : mysql
```
