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
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// LogEntry represents the structure of a log entry
type LogEntry struct {
	Date       string
	Time       string
	StatusCode string
	Duration   string
	IP         string
	Method     string
	Endpoint   string
}

// ProcessLog processes a single log line and returns a LogEntry if the endpoint matches the API list
func ProcessLog(line string, apiList map[string]struct{}) *LogEntry {
	parts := strings.Fields(line)
	if len(parts) >= 13 {
		endpoint := parts[11]
		if _, exists := apiList[endpoint]; exists {
			return &LogEntry{
				Date:       parts[1],
				Time:       parts[2],
				StatusCode: parts[4],
				Duration:   parts[6],
				IP:         parts[8],
				Method:     parts[10],
				Endpoint:   endpoint,
			}
		}
	}
	return nil
}

// InsertLogEntry inserts a log entry into the database
func InsertLogEntry(db *sql.DB, entry *LogEntry) error {
	query := `
		INSERT INTO logs (date, time, status_code, duration, ip, method, endpoint)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`
	_, err := db.Exec(query, entry.Date, entry.Time, entry.StatusCode, entry.Duration, entry.IP, entry.Method, entry.Endpoint)
	return err
}

// LoadAPIList loads the API endpoints from a file into a map for quick lookup
func LoadAPIList(filePath string) (map[string]struct{}, error) {
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
		}
	}
	return apiList, scanner.Err()
}

func monitorLogs(programName string, apiList map[string]struct{}, db *sql.DB) {
	cmd := exec.Command("supervisorctl", "tail", "-f", programName)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatalf("Error getting stdout for %s: %v", programName, err)
	}

	if err := cmd.Start(); err != nil {
		log.Fatalf("Error starting command for %s: %v", programName, err)
	}

	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "GIN") {
			entry := ProcessLog(line, apiList)
			if entry != nil {
				err := InsertLogEntry(db, entry)
				if err != nil {
					log.Printf("Error inserting log entry: %v", err)
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading stdout: %v", err)
	}
}

var dsn = flag.String("dsn", "user:password@tcp(127.0.0.1:3306)/your_database", "Data Source Name for MySQL")
var programList = flag.String("programs", "oula-api", "Comma-separated list of programs to monitor")
var apiListFile = flag.String("apilist", "api.list", "Path to the API list file")
var interval = flag.Duration("interval", 5*time.Minute, "Time interval for processing logs")

func main() {
	// 提取参数
	flag.Parse()

	// 加载API列表
	apiList, err := LoadAPIList(*apiListFile)
	if err != nil {
		log.Fatalf("Error loading API list: %v", err)
	}

	// 连接数据库
	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		log.Fatalf("Error connecting to the database: %v", err)
	}
	defer db.Close()

	// 处理要监控的程序列表
	programs := strings.Split(*programList, ",")

	for _, program := range programs {
		go monitorLogs(program, apiList, db)
	}

	// 使用 time.Tick 按照指定的时间间隔处理日志
	ticker := time.NewTicker(*interval)
	for range ticker.C {
		// 在这里可以处理每隔一段时间需要进行的日志分析或汇总任务
		fmt.Println("Processing logs at interval...")
	}

	// 保持主程序持续运行
	select {}
}