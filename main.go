package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"time"
)

var (
	writerSource = fmt.Sprintf("%s:%s@(%s:3306)/%s", os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD"), os.Getenv("WRITER_HOST"), os.Getenv("DB_NAME"))
	readerSource = fmt.Sprintf("%s:%s@(%s:3306)/%s", os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD"), os.Getenv("READER_HOST"), os.Getenv("DB_NAME"))

	writer *sql.DB
	reader *sql.DB
)

func init() {
	var err error = nil

	writer, err = sql.Open("mysql", writerSource)
	if err != nil {
		println("Writer Connection Error. - " + err.Error())
		os.Exit(1)
	}
	reader, err = sql.Open("mysql", readerSource)
	if err != nil {
		println("Writer Connection Error. - " + err.Error())
		os.Exit(1)
	}

	_, err = writer.ExecContext(context.Background(), "CREATE TABLE IF NOT EXISTS test (id INT PRIMARY KEY AUTO_INCREMENT)")
	if err != nil {
		println("Failed to create table. - " + err.Error())
		os.Exit(1)
	}
}

type Info struct {
	Version string
	Id      int
}

// getInfo MySQLのバージョンと test テーブルの最新のレコードの id 列の値を取得
func getInfo(ctx context.Context, db *sql.DB) (*Info, error) {
	selectSql := "SELECT version(), id from test order by id desc limit 1"
	c, _ := context.WithTimeout(ctx, 500*time.Millisecond)
	rows, err := db.QueryContext(c, selectSql)
	if err != nil {
		return nil, err
	}

	info := Info{}
	if rows.Next() {
		rows.Scan(&info.Version, &info.Id)
	}

	return &info, nil
}

// insert test テーブルに 1 件追加 (id 列は AUTO_INCREMENT で自動的に値が入る)
func insert(ctx context.Context, db *sql.DB) error {
	insertSql := "INSERT INTO test VALUES ()"
	c, _ := context.WithTimeout(ctx, 500*time.Millisecond)
	_, err := db.ExecContext(c, insertSql)
	return err
}

func main() {
	defer writer.Close()
	defer reader.Close()

	ctx := context.Background()

	for {
		err := insert(ctx, writer)
		insertResult := "INSERT SUCCEEDED"
		if err != nil {
			insertResult = "INSERT FAILED!!!"
		}
		time.Sleep(500 * time.Millisecond)

		winfo, err := getInfo(ctx, writer)
		if err != nil {
			winfo = &Info{
				Version: "FAILED",
				Id:      -1,
			}
		}
		rinfo, err := getInfo(ctx, reader)
		if err != nil {
			rinfo = &Info{
				Version: "FAILED",
				Id:      -1,
			}
		}

		fmt.Printf(
			"%s - %s | WRITER version: %s, id: %d | READER version: %s, id: %d\n",
			time.Now().Format("15:04:05"),
			insertResult,
			winfo.Version,
			winfo.Id,
			rinfo.Version,
			rinfo.Id,
		)

		time.Sleep(500 * time.Millisecond)
	}
}
