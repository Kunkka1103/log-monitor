package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// LogEntry represents the structure of a log entry
type LogEntry struct {
	Server     string
	Program    string
	Date       string
	Time       string
	StatusCode string
	Duration   string
	IP         string
	Method     string
	APIPath    string
}

// ParseLogWithAWK uses awk to process a log line and returns a LogEntry
func ParseLogWithAWK(line, server, program string) (*LogEntry, error) {
	awkCmd := `awk '{print $2,$4,$6,$8,$10,$12,$13}'`
	cmd := exec.Command("sh", "-c", fmt.Sprintf("echo '%s' | %s", line, awkCmd))

	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	fields := strings.Fields(string(output))
	if len(fields) >= 7 {
		// 去掉 apiPath 两端的引号
		apiPath := strings.Trim(fields[6], "\"")

		return &LogEntry{
			Server:     server,
			Program:    program,
			Date:       fields[0],
			Time:       fields[1],
			StatusCode: fields[2],
			Duration:   fields[3],
			IP:         fields[4],
			Method:     fields[5],
			APIPath:    apiPath,
		}, nil
	}

	return nil, fmt.Errorf("failed to parse log line: %s", line)
}

// LongestMatch finds the longest matching API path in the list
func LongestMatch(apiPath string, apiList map[string]struct{}) string {
	longestMatch := ""
	for api := range apiList {
		if strings.HasPrefix(apiPath, api) && len(api) > len(longestMatch) {
			longestMatch = api
		}
	}
	return longestMatch
}

// InsertLogEntry inserts a log entry into the database
func InsertLogEntry(db *sql.DB, entries []*LogEntry) error {
	log.Printf("Inserting %d log entries", len(entries))
	query := `
		INSERT INTO oula_logs_record (server, program, date, time, status_code, duration, ip, method, api_path)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	for _, entry := range entries {
		_, err := db.Exec(query, entry.Server, entry.Program, entry.Date, entry.Time, entry.StatusCode, entry.Duration, entry.IP, entry.Method, entry.APIPath)
		if err != nil {
			log.Printf("Error inserting log entry: %v", err)
			return err
		}
	}
	return nil
}

// monitorLogs monitors the logs from supervisorctl and processes them
func monitorLogs(program string, db *sql.DB, apiList map[string]struct{}, server string) {
	log.Printf("Starting to monitor logs for program: %s", program)
	cmd := exec.Command("supervisorctl", "tail", "-f", program)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatalf("Error getting stdout for %s: %v", program, err)
	}

	if err := cmd.Start(); err != nil {
		log.Fatalf("Error starting command for %s: %v", program, err)
	}

	reader := bufio.NewReader(stdout)
	batchSize := 100
	entries := []*LogEntry{}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("Error reading stdout: %v", err)
		}

		if strings.Contains(line, "GIN") {
			log.Println("Found GIN log line")
			entry, err := ParseLogWithAWK(line, server, program)
			if err != nil {
				log.Printf("Error parsing log line with awk: %v", err)
				continue
			}
			// Find the longest matching APIPath
			matchedAPIPath := LongestMatch(entry.APIPath, apiList)
			if matchedAPIPath != "" {
				entry.APIPath = matchedAPIPath
				entries = append(entries, entry)

				// Insert in batch when batchSize is reached
				if len(entries) >= batchSize {
					err := InsertLogEntry(db, entries)
					if err != nil {
						log.Printf("Error inserting log entry: %v", err)
					} else {
						log.Println("Log entries inserted successfully")
					}
					entries = []*LogEntry{} // Reset the batch
				}
			} else {
				log.Printf("APIPath did not match: %s", entry.APIPath)
			}
		}
	}

	// Insert any remaining entries
	if len(entries) > 0 {
		err := InsertLogEntry(db, entries)
		if err != nil {
			log.Printf("Error inserting remaining log entries: %v", err)
		} else {
			log.Println("Remaining log entries inserted successfully")
		}
	}
}

// CleanOldLogs deletes logs older than 8 days from the database
func CleanOldLogs(db *sql.DB) {
	log.Println("Cleaning old logs older than 8 days")
	query := `DELETE FROM oula_logs_record WHERE date < NOW() - INTERVAL 8 DAY`
	_, err := db.Exec(query)
	if err != nil {
		log.Printf("Error cleaning old logs: %v", err)
	}
}

var dsn = flag.String("dsn", "", "Data Source Name for MySQL")
var programList = flag.String("programs", "", "Comma-separated list of programs to monitor")
var apiListFile = flag.String("apilist", "", "Path to the API list file")
var server = flag.String("server", "", "Servername")

func main() {
	// 提取参数
	flag.Parse()

	// 加载API列表
	apiList, err := LoadAPIList(*apiListFile)
	if err != nil {
		log.Fatalf("Error loading API list: %v", err)
	}

	// 连接数据库
	log.Printf("Connecting to database with DSN: %s", *dsn)
	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		log.Fatalf("Error connecting to the database: %v", err)
	}
	defer db.Close()

	// 定期清理旧数据，每天清理一次
	go func() {
		for {
			CleanOldLogs(db)
			time.Sleep(24 * time.Hour)
		}
	}()

	// 处理要监控的程序列表
	programs := strings.Split(*programList, ",")

	for _, program := range programs {
		go monitorLogs(program, db, apiList, *server)
	}

	// 保持主程序持续运行
	select {}
}

// LoadAPIList loads the APIPath from a file into a map for quick lookup
func LoadAPIList(filePath string) (map[string]struct{}, error) {
	log.Printf("Loading API list from file: %s", filePath)
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	apiList := make(map[string]struct{})
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			apiList[line] = struct{}{}
			log.Printf("Loaded API: %s", line)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("Error reading API list file: %v", err)
		return nil, err
	}
	return apiList, nil
}
