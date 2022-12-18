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
	writerSource = fmt.Sprintf("%s:%s@(%s:3306)/example", os.Getenv("WRITER_USER"), os.Getenv("WRITER_PASSWORD"), os.Getenv("WRITER_HOST"))
	readerSource = fmt.Sprintf("%s:%s@(%s:3306)/example", os.Getenv("READER_USER"), os.Getenv("READER_PASSWORD"), os.Getenv("READER_HOST"))
)

type Info struct {
	Version string
	Id      int
}

// getInfo MySQLのバージョンと test テーブルの最新のレコードの id 列の値を取得
func getInfo(ctx context.Context, db *sql.DB) (*Info, error) {
	selectSql := "SELECT version(), id from test order by id desc limit 1"
	rows, err := db.QueryContext(ctx, selectSql)
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
	_, err := db.ExecContext(ctx, insertSql)
	return err
}

func main() {
	writer, err := sql.Open("mysql", writerSource)
	reader, err := sql.Open("mysql", readerSource)

	if err != nil {
		println(err.Error())
		return
	}

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
		rinfo, err := getInfo(ctx, reader)

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
