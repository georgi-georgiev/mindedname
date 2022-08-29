package plunder

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4"
)

type PlunderPGXLogger struct {
}

func (l *PlunderPGXLogger) Log(ctx context.Context, level pgx.LogLevel, msg string, data map[string]interface{}) {

	key := ctx.Value("plunder")
	if key != nil {
		fmt.Println("plunder", key)
	}

	fmt.Println("level", level)
	fmt.Println("msg", msg)

	for k, v := range data {
		fmt.Println("k", k)
		fmt.Println("v", v)
	}
}

func AfterConnect(ctx context.Context, conn *pgx.Conn) error {
	key := ctx.Value("plunder")
	if key != nil {
		fmt.Println("after connect plunder", key)
	}
	return nil
}

func AfterRelease(conn *pgx.Conn) bool {
	return true
}
