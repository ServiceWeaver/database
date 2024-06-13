package main

import (
	"fmt"
	"math/rand"
)

func createInsertQueries(queryCnt int, table string) []string {
	randStrL := func(minLen, maxLen int, charset string) string {
		length := rand.Intn(maxLen-minLen+1) + minLen
		result := make([]byte, length)
		for i := 0; i < length; i++ {
			result[i] = charset[rand.Intn(len(charset))]
		}
		return string(result)
	}
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	const passwordChar = "0123456789"

	queries := make([]string, queryCnt)
	for i := 0; i < queryCnt; i++ {
		queries[i] = fmt.Sprintf("INSERT INTO %s(username,password) VALUES ('%s','%s');", table, randStrL(9, 19, charset), randStrL(4, 10, passwordChar))
	}
	return queries
}

func createDeleteQueries(table string) []string {
	var queries []string

	query1 := fmt.Sprintf(`
	DELETE FROM %s
	WHERE username='abcde';
		`, table)
	queries = append(queries, query1)

	return queries
}

func createReadQueries(table string) []string {
	var queries []string
	// hard coded some queries
	query1 := fmt.Sprintf(`
	SELECT *
	FROM %s
	WHERE username = 'aaaa';
	`, table)
	queries = append(queries, query1)

	query2 := fmt.Sprintf(`
	SELECT *
	FROM %s
	WHERE LENGTH(password) = 8 AND username LIKE 'a%%';
	`, table)
	queries = append(queries, query2)

	query3 := fmt.Sprintf(`
	SELECT COUNT(*)
	FROM %s
	`, table)
	queries = append(queries, query3)

	query4 := fmt.Sprintf(`
	SELECT *
	FROM %s
	`, table)
	queries = append(queries, query4)
	return queries
}
