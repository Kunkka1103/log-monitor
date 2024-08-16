package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"

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
	APIPath   string
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
			APIPath:   apiPath,
		}, nil
	}

	return nil, fmt.Errorf("failed to parse log line: %s", line)
}

// InsertLogEntry inserts a log entry into the database
func InsertLogEntry(db *sql.DB, entry *LogEntry) error {
	log.Printf("Inserting log entry: %+v", entry)
	query := `
		INSERT INTO oula_logs_record (server,program,date, time, status_code, duration, ip, method, api_path)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
	_, err := db.Exec(query, entry.Server, entry.Program, entry.Date, entry.Time, entry.StatusCode, entry.Duration, entry.IP, entry.Method, entry.APIPath)
	if err != nil {
		log.Printf("Error inserting log entry: %v", err)
	}
	return err
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

	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "GIN") {
			log.Println("Found GIN log line")
			entry, err := ParseLogWithAWK(line, server, program)
			if err != nil {
				log.Printf("Error parsing log line with awk: %v", err)
				continue
			}
			// Check if APIPath is in the API list
			if _, exists := apiList[entry.APIPath]; exists {
				err := InsertLogEntry(db, entry)
				if err != nil {
					log.Printf("Error inserting log entry: %v", err)
				} else {
					log.Println("Log entry inserted successfully")
				}
			} else {
				log.Printf("APIPath did not match: %s", entry.APIPath)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading stdout: %v", err)
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
