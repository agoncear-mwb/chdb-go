package chdb

import (
	chdbpurego "github.com/chdb-io/chdb-go/chdb-purego"
)

// Query calls query_conn with a default in-memory session and default output format of "CSV" if not provided.
func Query(queryStr string, outputFormats ...string) (result chdbpurego.ChdbResult, err error) {
	outputFormat := "CSV" // Default value
	if len(outputFormats) > 0 {
		outputFormat = outputFormats[0]
	}
	tempSession, err := initConnection(":memory:")
	if err != nil {
		return nil, err
	}
	defer tempSession.Close()
	return tempSession.Query(queryStr, outputFormat)
}

func initConnection(connStr string) (result chdbpurego.ChdbConn, err error) {
	argv := []string{connStr}
	// Call NewConnection with the constructed arguments
	return chdbpurego.NewConnection(len(argv), argv)
}
