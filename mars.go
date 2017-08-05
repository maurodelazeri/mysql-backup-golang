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
	"path/filepath"
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

	// Backups retentions validation
	BackupRotation(*options)

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

		t := time.Now()
		timestamp := strings.Replace(strings.Replace(options.ExecutionStartDate.Format("2006-01-02"), "-", "", -1), ":", "", -1)
		filename := path.Join(options.OutputDirectory, "daily", t.Format("2006-01-02"), db+"-"+options.ExecutionStartDate.Format("2006-01-02"), fmt.Sprintf("%s_%s%d_%s.sql", db, table.TableName, index, timestamp))
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

	t := time.Now()
	timestamp := strings.Replace(strings.Replace(options.ExecutionStartDate.Format("2006-01-02"), "-", "", -1), ":", "", -1)
	filename := path.Join(options.OutputDirectory, "daily", t.Format("2006-01-02"), db+"-"+options.ExecutionStartDate.Format("2006-01-02"), fmt.Sprintf("%s_%s_%s.sql", db, "SCHEMA", timestamp))
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

	t := time.Now()
	timestamp := strings.Replace(strings.Replace(options.ExecutionStartDate.Format("2006-01-02"), "-", "", -1), ":", "", -1)
	filename := path.Join(options.OutputDirectory, "daily", t.Format("2006-01-02"), db+"-"+options.ExecutionStartDate.Format("2006-01-02"), fmt.Sprintf("%s_%s_%s.sql", db, "DATA", timestamp))
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

	t := time.Now()
	timestamp := strings.Replace(strings.Replace(options.ExecutionStartDate.Format("2006-01-02"), "-", "", -1), ":", "", -1)
	filename := path.Join(options.OutputDirectory, "daily", t.Format("2006-01-02"), db+"-"+options.ExecutionStartDate.Format("2006-01-02"), fmt.Sprintf("%s_%s_%s.sql", db, "ALL", timestamp))
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

// Compress compresses files into tar.gz file
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

// ListFiles give a Array of files in a given path
func ListFiles(searchDir string) []string {
	fileList := []string{}
	filepath.Walk(searchDir, func(path string, f os.FileInfo, err error) error {
		if path != "daily" && path != "weekly" && path != "monthly" {
			fileList = append(fileList, path)
		}
		return nil
	})
	return fileList
}

// BackupRotation execute a rotation of file, daily,weekly and monthly
func BackupRotation(options Options) {

	t := time.Now()

	//month
	if options.MontlyRotation > 0 {
		month := ListFiles(options.OutputDirectory + "/monthly")
		if len(month) == 0 {
			CopyDir(options.OutputDirectory+"/daily/"+t.Format("2006-01-02"), options.OutputDirectory+"/monthly/"+t.Format("2006-01-02"))
		}

	}
	//week
	if options.WeeklyRotation > 0 {
		month := ListFiles(options.OutputDirectory + "/weekly")
		if len(month) == 0 {
		}

	}
	//day
	if options.DailyRotation > 0 {
		month := ListFiles(options.OutputDirectory + "/daily")
		if len(month) == 0 {
		}

	}
}

// CopyFile copies the contents of the file named src to the file named
// by dst. The file will be created if it does not already exist. If the
// destination file exists, all it's contents will be replaced by the contents
// of the source file. The file mode will be copied from the source and
// the copied data is synced/flushed to stable storage.
func CopyFile(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return
	}
	defer func() {
		if e := out.Close(); e != nil {
			err = e
		}
	}()

	_, err = io.Copy(out, in)
	if err != nil {
		return
	}

	err = out.Sync()
	if err != nil {
		return
	}

	si, err := os.Stat(src)
	if err != nil {
		return
	}
	err = os.Chmod(dst, si.Mode())
	if err != nil {
		return
	}

	return
}

// CopyDir recursively copies a directory tree, attempting to preserve permissions.
// Source directory must exist, destination directory must *not* exist.
// Symlinks are ignored and skipped.
func CopyDir(src string, dst string) (err error) {
	src = filepath.Clean(src)
	dst = filepath.Clean(dst)

	si, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !si.IsDir() {
		return fmt.Errorf("source is not a directory")
	}

	_, err = os.Stat(dst)
	if err != nil && !os.IsNotExist(err) {
		return
	}
	if err == nil {
		return fmt.Errorf("destination already exists")
	}

	err = os.MkdirAll(dst, si.Mode())
	if err != nil {
		return
	}

	entries, err := ioutil.ReadDir(src)
	if err != nil {
		return
	}

	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		if entry.IsDir() {
			err = CopyDir(srcPath, dstPath)
			if err != nil {
				return
			}
		} else {
			// Skip symlinks.
			if entry.Mode()&os.ModeSymlink != 0 {
				continue
			}

			err = CopyFile(srcPath, dstPath)
			if err != nil {
				return
			}
		}
	}

	return
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
	t := time.Now()
	os.MkdirAll(outputdir+"/daily/"+t.Format("2006-01-02"), os.ModePerm)
	os.MkdirAll(outputdir+"/weekly", os.ModePerm)
	os.MkdirAll(outputdir+"/monthly", os.ModePerm)

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
