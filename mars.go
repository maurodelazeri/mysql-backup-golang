package main

import (
	"archive/tar"
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	_ "github.com/go-sql-driver/mysql"
)

const (
	// Info messages
	Info = 1 << iota // a == 1 (iota has been reset)

	// Warning Messages
	Warning = 1 << iota // b == 2

	// Error Messages
	Error = 1 << iota // c == 4
)

// Table model struct for table metadata
type Table struct {
	TableName string
	RowCount  int
}

// Options model for commandline arguments
type Options struct {
	HostName          string
	Bind              string
	UserName          string
	Password          string
	Databases         []string
	ExcludedDatabases []string

	DatabaseRowCountTreshold int
	TableRowCountTreshold    int
	BatchSize                int
	ForceSplit               bool

	AdditionalMySQLDumpArgs string

	Verbosity              int
	MySQLDumpPath          string
	OutputDirectory        string
	DefaultsProvidedByUser bool
	ExecutionStartDate     time.Time

	DailyRotation  int
	WeeklyRotation int
	MontlyRotation int
}

func main() {
	options := GetOptions()

	for _, db := range options.Databases {
		printMessage("Processing Database : "+db, options.Verbosity, Info)

		tables := GetTables(options.HostName, options.Bind, options.UserName, options.Password, db, options.Verbosity)
		totalRowCount := getTotalRowCount(tables)

		if !options.ForceSplit && totalRowCount <= options.DatabaseRowCountTreshold {
			// options.ForceSplit is false
			// and if total row count of a database is below defined threshold
			// then generate one file containing both schema and data

			printMessage(fmt.Sprintf("options.ForceSplit (%t) && totalRowCount (%d) <= options.DatabaseRowCountTreshold (%d)", options.ForceSplit, totalRowCount, options.DatabaseRowCountTreshold), options.Verbosity, Info)
			generateSingleFileBackup(*options, db)
		} else if options.ForceSplit && totalRowCount <= options.DatabaseRowCountTreshold {
			// options.ForceSplit is true
			// and if total row count of a database is below defined threshold
			// then generate two files one for schema, one for data

			generateSchemaBackup(*options, db)
			generateSingleFileDataBackup(*options, db)
		} else if totalRowCount > options.DatabaseRowCountTreshold {
			generateSchemaBackup(*options, db)

			for _, table := range tables {
				generateTableBackup(*options, db, table)
			}
		}

		printMessage("Processing done for database : "+db, options.Verbosity, Info)
	}

}

// NewTable returns a new Table instance.
func NewTable(tableName string, rowCount int) *Table {
	return &Table{
		TableName: tableName,
		RowCount:  rowCount,
	}
}

// GetTables retrives list of tables with rowcounts
func GetTables(hostname string, bind string, username string, password string, database string, verbosity int) []Table {
	printMessage("Getting tables for database : "+database, verbosity, Info)

	db, err := sql.Open("mysql", username+":"+password+"@tcp("+hostname+":"+bind+")/"+database)

	checkErr(err)

	defer db.Close()

	rows, err := db.Query("SELECT table_name as TableName, table_rows as RowCount FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + database + "'")
	checkErr(err)

	var result []Table

	for rows.Next() {
		var tableName string
		var rowCount int

		err = rows.Scan(&tableName, &rowCount)
		checkErr(err)

		result = append(result, *NewTable(tableName, rowCount))
	}

	printMessage(strconv.Itoa(len(result))+" tables retrived : "+database, verbosity, Info)

	return result
}

// GetDatabaseList retrives list of databases on mysql
func GetDatabaseList(hostname string, bind string, username string, password string, verbosity int) []string {
	printMessage("Getting databases : "+hostname, verbosity, Info)

	//	db, err := sql.Open("mysql", username+":"+password+"@tcp("+hostname+":"+bind+")")
	db, err := sql.Open("mysql", username+":"+password+"@tcp("+hostname+":"+bind+")/mysql")
	checkErr(err)

	defer db.Close()

	rows, err := db.Query("SHOW DATABASES")
	checkErr(err)

	var result []string

	for rows.Next() {
		var databaseName string

		err = rows.Scan(&databaseName)
		checkErr(err)

		result = append(result, databaseName)
	}

	printMessage(strconv.Itoa(len(result))+" databases retrived : "+hostname, verbosity, Info)

	return result
}

// NewOptions returns a new Options instance.
func NewOptions(hostname string, bind string, username string, password string, databases string, excludeddatabases string, databasetreshold int, tablethreshold int, batchsize int, forcesplit bool, additionals string, verbosity int, mysqldumppath string, outputDirectory string, defaultsProvidedByUser bool, dailyrotation int, weeklyrotation int, montlyrotation int) *Options {

	databases = strings.Replace(databases, " ", "", -1)
	databases = strings.Replace(databases, " , ", ",", -1)
	databases = strings.Replace(databases, ", ", ",", -1)
	databases = strings.Replace(databases, " ,", ",", -1)
	dbs := strings.Split(databases, ",")
	dbs = removeDuplicates(dbs)

	excludeddbs := []string{}

	if databases == "--all-databases" {

		excludeddatabases = excludeddatabases + ",information_schema,performance_schema"

		dbslist := GetDatabaseList(hostname, bind, username, password, verbosity)
		databases = strings.Join(dbslist, ",")

		excludeddatabases = strings.Replace(excludeddatabases, " ", "", -1)
		excludeddatabases = strings.Replace(excludeddatabases, " , ", ",", -1)
		excludeddatabases = strings.Replace(excludeddatabases, ", ", ",", -1)
		excludeddatabases = strings.Replace(excludeddatabases, " ,", ",", -1)
		excludeddbs := strings.Split(excludeddatabases, ",")
		excludeddbs = removeDuplicates(excludeddbs)

		// Databases to not be in the backup
		dbs = difference(dbslist, excludeddbs)
	}

	return &Options{
		HostName:                 hostname,
		Bind:                     bind,
		UserName:                 username,
		Password:                 password,
		Databases:                dbs,
		ExcludedDatabases:        excludeddbs,
		DatabaseRowCountTreshold: databasetreshold,
		TableRowCountTreshold:    tablethreshold,
		BatchSize:                batchsize,
		ForceSplit:               forcesplit,
		AdditionalMySQLDumpArgs:  additionals,
		Verbosity:                verbosity,
		MySQLDumpPath:            mysqldumppath,
		OutputDirectory:          outputDirectory,
		DefaultsProvidedByUser:   defaultsProvidedByUser,
		ExecutionStartDate:       time.Now(),
		DailyRotation:            dailyrotation,
		WeeklyRotation:           weeklyrotation,
		MontlyRotation:           montlyrotation,
	}
}

func removeDuplicates(elements []string) []string {
	// Use map to record duplicates as we find them.
	encountered := map[string]bool{}
	result := []string{}

	for v := range elements {
		if encountered[elements[v]] == true {
			// Do not add duplicate.
		} else {
			// Record this element as an encountered element.
			encountered[elements[v]] = true
			// Append to result slice.
			result = append(result, elements[v])
		}
	}
	// Return the new slice.
	return result
}

// difference returns the elements in a that aren't in b
func difference(a, b []string) []string {
	mb := map[string]bool{}
	for _, x := range b {
		mb[x] = true
	}
	ab := []string{}
	for _, x := range a {
		if _, ok := mb[x]; !ok {
			ab = append(ab, x)
		}
	}
	return ab
}

func generateTableBackup(options Options, db string, table Table) {
	printMessage("Generating table backup. Database : "+db+"\t\tTableName : "+table.TableName+"\t\tRowCount : "+strconv.Itoa(table.RowCount), options.Verbosity, Info)

	index := 1
	for counter := 0; counter <= table.RowCount; counter += options.BatchSize {

		var args []string
		args = append(args, fmt.Sprintf("-h%s", options.HostName))
		args = append(args, fmt.Sprintf("-u%s", options.UserName))
		args = append(args, fmt.Sprintf("-p%s", options.Password))

		args = append(args, "--no-create-db")
		args = append(args, "--skip-triggers")
		args = append(args, "--no-create-info")

		if options.AdditionalMySQLDumpArgs != "" {
			args = append(args, strings.Split(options.AdditionalMySQLDumpArgs, " ")...)
		}

		timestamp := strings.Replace(strings.Replace(options.ExecutionStartDate.Format("2006-01-02_15:04:05"), "-", "", -1), ":", "", -1)
		filename := path.Join(options.OutputDirectory, db, fmt.Sprintf("%s_%s%d_%s.sql", db, table.TableName, index, timestamp))
		_ = os.Mkdir(path.Dir(filename), os.ModePerm)

		args = append(args, fmt.Sprintf("-r%s", filename))

		args = append(args, fmt.Sprintf("--where=1=1 LIMIT %d, %d", counter, options.BatchSize))

		args = append(args, db)
		args = append(args, table.TableName)

		cmd := exec.Command(options.MySQLDumpPath, args...)
		cmdOut, _ := cmd.StdoutPipe()
		cmdErr, _ := cmd.StderrPipe()

		printMessage("mysqldump is being executed with parameters : "+strings.Join(cmd.Args, " "), options.Verbosity, Info)
		cmd.Start()

		output, _ := ioutil.ReadAll(cmdOut)
		err, _ := ioutil.ReadAll(cmdErr)
		cmd.Wait()

		printMessage("mysqldump output is : "+string(output), options.Verbosity, Info)

		if string(err) != "" {
			printMessage("mysqldump error is: "+string(err), options.Verbosity, Error)
			os.Exit(4)
		}

		// Compressing
		printMessage("Compressing table file : "+filename, options.Verbosity, Info)

		// set up the output file
		file, errcreate := os.Create(filename + ".tar.gz")

		if errcreate != nil {
			printMessage("error to create a compressed file: "+filename, options.Verbosity, Error)
			os.Exit(4)
		}

		defer file.Close()
		// set up the gzip writer
		gw := gzip.NewWriter(file)
		defer gw.Close()
		tw := tar.NewWriter(gw)
		defer tw.Close()

		if errcompress := Compress(tw, filename); errcompress != nil {
			printMessage("error to compress file: "+filename, options.Verbosity, Error)
			os.Exit(4)
		}

		index++
	}

	printMessage("Table backup successfull. Database : "+db+"\t\tTableName : "+table.TableName, options.Verbosity, Info)
}

func generateSchemaBackup(options Options, db string) {
	printMessage("Generating schema backup : "+db, options.Verbosity, Info)

	var args []string
	args = append(args, fmt.Sprintf("-h%s", options.HostName))
	args = append(args, fmt.Sprintf("-u%s", options.UserName))
	args = append(args, fmt.Sprintf("-p%s", options.Password))

	args = append(args, "--no-data")

	if options.AdditionalMySQLDumpArgs != "" {
		args = append(args, strings.Split(options.AdditionalMySQLDumpArgs, " ")...)
	}

	timestamp := strings.Replace(strings.Replace(options.ExecutionStartDate.Format("2006-01-02_15:04:05"), "-", "", -1), ":", "", -1)
	filename := path.Join(options.OutputDirectory, db, fmt.Sprintf("%s_%s_%s.sql", db, "SCHEMA", timestamp))
	_ = os.Mkdir(path.Dir(filename), os.ModePerm)

	args = append(args, fmt.Sprintf("-r%s", filename))

	args = append(args, db)

	printMessage("mysqldump is being executed with parameters : "+strings.Join(args, " "), options.Verbosity, Info)

	cmd := exec.Command(options.MySQLDumpPath, args...)
	cmdOut, _ := cmd.StdoutPipe()
	cmdErr, _ := cmd.StderrPipe()

	cmd.Start()

	output, _ := ioutil.ReadAll(cmdOut)
	err, _ := ioutil.ReadAll(cmdErr)
	cmd.Wait()

	printMessage("mysqldump output is : "+string(output), options.Verbosity, Info)

	if string(err) != "" {
		printMessage("mysqldump error is: "+string(err), options.Verbosity, Error)
		os.Exit(4)
	}

	// Compressing
	printMessage("Compressing table file : "+filename, options.Verbosity, Info)

	// set up the output file
	file, errcreate := os.Create(filename + ".tar.gz")

	if errcreate != nil {
		printMessage("error to create a compressed file: "+filename, options.Verbosity, Error)
		os.Exit(4)
	}

	defer file.Close()
	// set up the gzip writer
	gw := gzip.NewWriter(file)
	defer gw.Close()
	tw := tar.NewWriter(gw)
	defer tw.Close()

	if errcompress := Compress(tw, filename); errcompress != nil {
		printMessage("error to compress file: "+filename, options.Verbosity, Error)
		os.Exit(4)
	}

	printMessage("Schema backup successfull : "+db, options.Verbosity, Info)
}

func generateSingleFileDataBackup(options Options, db string) {
	printMessage("Generating single file data backup : "+db, options.Verbosity, Info)

	var args []string
	args = append(args, fmt.Sprintf("-h%s", options.HostName))
	args = append(args, fmt.Sprintf("-u%s", options.UserName))
	args = append(args, fmt.Sprintf("-p%s", options.Password))

	args = append(args, "--no-create-db")
	args = append(args, "--skip-triggers")
	args = append(args, "--no-create-info")

	if options.AdditionalMySQLDumpArgs != "" {
		args = append(args, strings.Split(options.AdditionalMySQLDumpArgs, " ")...)
	}

	timestamp := strings.Replace(strings.Replace(options.ExecutionStartDate.Format("2006-01-02_15:04:05"), "-", "", -1), ":", "", -1)
	filename := path.Join(options.OutputDirectory, db, fmt.Sprintf("%s_%s_%s.sql", db, "DATA", timestamp))
	_ = os.Mkdir(path.Dir(filename), os.ModePerm)

	args = append(args, fmt.Sprintf("-r%s", filename))

	args = append(args, db)

	printMessage("mysqldump is being executed with parameters : "+strings.Join(args, " "), options.Verbosity, Info)

	cmd := exec.Command(options.MySQLDumpPath, args...)
	cmdOut, _ := cmd.StdoutPipe()
	cmdErr, _ := cmd.StderrPipe()

	cmd.Start()

	output, _ := ioutil.ReadAll(cmdOut)
	err, _ := ioutil.ReadAll(cmdErr)
	cmd.Wait()

	printMessage("mysqldump output is : "+string(output), options.Verbosity, Info)

	if string(err) != "" {
		printMessage("mysqldump error is: "+string(err), options.Verbosity, Error)
		os.Exit(4)
	}

	// Compressing
	printMessage("Compressing table file : "+filename, options.Verbosity, Info)

	// set up the output file
	file, errcreate := os.Create(filename + ".tar.gz")

	if errcreate != nil {
		printMessage("error to create a compressed file: "+filename, options.Verbosity, Error)
		os.Exit(4)
	}

	defer file.Close()
	// set up the gzip writer
	gw := gzip.NewWriter(file)
	defer gw.Close()
	tw := tar.NewWriter(gw)
	defer tw.Close()

	if errcompress := Compress(tw, filename); errcompress != nil {
		printMessage("error to compress file: "+filename, options.Verbosity, Error)
		os.Exit(4)
	}

	printMessage("Single file data backup successfull : "+db, options.Verbosity, Info)
}

func generateSingleFileBackup(options Options, db string) {
	printMessage("Generating single file backup : "+db, options.Verbosity, Info)

	var args []string
	args = append(args, fmt.Sprintf("-h%s", options.HostName))
	args = append(args, fmt.Sprintf("-u%s", options.UserName))
	args = append(args, fmt.Sprintf("-p%s", options.Password))

	if options.AdditionalMySQLDumpArgs != "" {
		args = append(args, strings.Split(options.AdditionalMySQLDumpArgs, " ")...)
	}

	timestamp := strings.Replace(strings.Replace(options.ExecutionStartDate.Format("2006-01-02_15:04:05"), "-", "", -1), ":", "", -1)
	filename := path.Join(options.OutputDirectory, db, fmt.Sprintf("%s_%s_%s.sql", db, "ALL", timestamp))
	_ = os.Mkdir(path.Dir(filename), os.ModePerm)

	args = append(args, fmt.Sprintf("-r%s", filename))

	args = append(args, db)

	printMessage("mysqldump is being executed with parameters : "+strings.Join(args, " "), options.Verbosity, Info)

	cmd := exec.Command(options.MySQLDumpPath, args...)
	cmdOut, _ := cmd.StdoutPipe()
	cmdErr, _ := cmd.StderrPipe()

	cmd.Start()

	output, _ := ioutil.ReadAll(cmdOut)
	err, _ := ioutil.ReadAll(cmdErr)
	cmd.Wait()

	printMessage("mysqldump output is : "+string(output), options.Verbosity, Info)

	if string(err) != "" {
		printMessage("mysqldump error is: "+string(err), options.Verbosity, Error)
		os.Exit(4)
	}

	// Compressing
	printMessage("Compressing table file : "+filename, options.Verbosity, Info)

	// set up the output file
	file, errcreate := os.Create(filename + ".tar.gz")

	if errcreate != nil {
		printMessage("error to create a compressed file: "+filename, options.Verbosity, Error)
		os.Exit(4)
	}

	defer file.Close()
	// set up the gzip writer
	gw := gzip.NewWriter(file)
	defer gw.Close()
	tw := tar.NewWriter(gw)
	defer tw.Close()

	if errcompress := Compress(tw, filename); errcompress != nil {
		printMessage("error to compress file: "+filename, options.Verbosity, Error)
		os.Exit(4)
	}

	printMessage("Single file backup successfull : "+db, options.Verbosity, Info)
}

func getTotalRowCount(tables []Table) int {
	result := 0
	for _, table := range tables {
		result += table.RowCount
	}

	return result
}

// Compress compresses one or many files into a single tar.gz archive file
func Compress(tw *tar.Writer, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	if stat, err := file.Stat(); err == nil {
		// now lets create the header as needed for this file within the tarball
		header := new(tar.Header)
		header.Name = path
		header.Size = stat.Size()
		header.Mode = int64(stat.Mode())
		header.ModTime = stat.ModTime()
		// write the header to the tarball archive
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		// copy the file data to the tarball
		if _, err := io.Copy(tw, file); err != nil {
			return err
		}

		// Removing the original file after zipping it
		err = os.Remove(path)

		if err != nil {
			fmt.Println(err)
			return err
		}
	}
	return nil
}

// GetOptions creates Options type from Commandline arguments
func GetOptions() *Options {

	var hostname string
	flag.StringVar(&hostname, "hostname", "localhost", "Hostname of the mysql server to connect to")

	var bind string
	flag.StringVar(&bind, "bind", "3306", "Port of the mysql server to connect to")

	var username string
	flag.StringVar(&username, "username", "root", "username of the mysql server to connect to")

	var password string
	flag.StringVar(&password, "password", "1234", "password of the mysql server to connect to")

	var databases string
	flag.StringVar(&databases, "databases", "--all-databases", "List of databases as comma seperated values to dump. OBS: If not specified, --all-databases is the default")

	var excludeddatabases string
	flag.StringVar(&excludeddatabases, "excluded-databases", "", "List of databases excluded to be excluded. OBS: Only valid if -databases is not specified")

	var dbthreshold int
	flag.IntVar(&dbthreshold, "dbthreshold", 10000000, "Do not split mysqldumps, if total rowcount of tables in database is less than dbthreshold value for whole database")

	var tablethreshold int
	flag.IntVar(&tablethreshold, "tablethreshold", 5000000, "Do not split mysqldumps, if rowcount of table is less than dbthreshold value for table")

	var batchsize int
	flag.IntVar(&batchsize, "batchsize", 1000000, "Split mysqldumps in order to get each file contains batchsize number of records")

	var forcesplit bool
	flag.BoolVar(&forcesplit, "forcesplit", false, "Split schema and data dumps even if total rowcount of tables in database is less than dbthreshold value. if false one dump file will be created")

	var additionals string
	flag.StringVar(&additionals, "additionals", "", "Additional parameters that will be appended to mysqldump command")

	var verbosity int
	flag.IntVar(&verbosity, "verbosity", 2, "0 = only errors, 1 = important things, 2 = all")

	var mysqldumppath string
	flag.StringVar(&mysqldumppath, "mysqldump-path", "/usr/bin/mysqldump", "Absolute path for mysqldump executable.")

	var outputdir string
	flag.StringVar(&outputdir, "output-dir", "", "Default is the value of os.Getwd(). The backup files will be placed to output-dir /{DATABASE_NAME}/{DATABASE_NAME}_{TABLENAME|SCHEMA|DATA|ALL}_{TIMESTAMP}.sql")

	var dailyrotation int
	flag.IntVar(&dailyrotation, "daily-rotation", 5, "Number of backups on the daily rotation")

	var weeklyrotation int
	flag.IntVar(&weeklyrotation, "weekly-rotation", 2, "Number of backups on the weekly rotation")

	var montlyrotation int
	flag.IntVar(&montlyrotation, "montly-rotation", 1, "Number of backups on the montly rotation")

	var test bool
	flag.BoolVar(&test, "test", false, "test")

	flag.Parse()

	if outputdir == "" {
		dir, err := os.Getwd()
		if err != nil {
			printMessage(err.Error(), verbosity, Error)
		}

		outputdir = dir
	}

	defaultsProvidedByUser := true

	if _, err := os.Stat(mysqldumppath); os.IsNotExist(err) {
		printMessage("mysqldump binary can not be found, please specify correct value for mysqldump-path parameter", verbosity, Error)
		os.Exit(1)
	}

	opts := NewOptions(hostname, bind, username, password, databases, excludeddatabases, dbthreshold, tablethreshold, batchsize, forcesplit, additionals, verbosity, mysqldumppath, outputdir, defaultsProvidedByUser, dailyrotation, weeklyrotation, montlyrotation)
	stropts, _ := json.MarshalIndent(opts, "", "\t")
	printMessage("Running with parameters", verbosity, Info)
	printMessage(string(stropts), verbosity, Info)
	printMessage("Running on operating system : "+runtime.GOOS, verbosity, Info)

	if test {
		cmd := exec.Command(opts.MySQLDumpPath,
			`-h127.0.0.1`,
			`-uroot`,
			`-pXXXX`,
			`--no-create-db`,
			`--skip-triggers`,
			`--no-create-info`,
			`--single-transaction`,
			`--skip-extended-insert`,
			`--quick`,
			`--skip-add-locks`,
			`--default-character-set=utf8`,
			`--compress`,
			`mysql`,
			`--where="1=1 LIMIT 1000000, 1000000"`,
			`user`,
			`host`)

		cmdOut, _ := cmd.StdoutPipe()
		cmdErr, _ := cmd.StderrPipe()

		cmd.Start()

		output, _ := ioutil.ReadAll(cmdOut)
		err, _ := ioutil.ReadAll(cmdErr)

		cmd.Wait()

		printMessage("mysqldump output is : "+string(output), opts.Verbosity, Info)

		if string(err) != "" {
			printMessage("mysqldump error is: "+string(err), opts.Verbosity, Error)
			os.Exit(4)
		}

		os.Exit(4)
	}

	return opts
}

func printMessage(message string, verbosity int, messageType int) {
	colors := map[int]color.Attribute{Info: color.FgGreen, Warning: color.FgHiYellow, Error: color.FgHiRed}

	if verbosity == 2 {
		color.Set(colors[messageType])
		fmt.Println(message)
		color.Unset()
	} else if verbosity == 1 && messageType > 1 {
		color.Set(colors[messageType])
		fmt.Println(message)
		color.Unset()
	} else if verbosity == 0 && messageType > 2 {
		color.Set(colors[messageType])
		fmt.Println(message)
		color.Unset()
	}
}

func checkErr(err error) {
	if err != nil {
		color.Set(color.FgHiRed)
		panic(err)
		color.Unset()
	}
}
