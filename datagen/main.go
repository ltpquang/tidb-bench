package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
)

var tables = []string{
	"user_no_partition",
	"user_hash_partition",
	"user_range_partition",
}

func main() {
	generateInsert()
	generateQueryRange()
	generateQuerySingle()
	log.Println("DONE")
}

func generateInsert() {
	id := make([]int, 0, 1000000)
	for i := 0; i < 1000000; i++ {
		id = append(id, i+1)
	}
	for _, table := range tables {
		file, err := os.Create(fmt.Sprintf("insert_sequently_%s.sql", table))
		if err != nil {
			panic(err)
		}
		for i := 0; i < 1000000; i++ {
			_, err := file.WriteString(fmt.Sprintf("INSERT INTO `%s` (`id`) VALUES(%d);\n", table, id[i]))
			if err != nil {
				panic(err)
			}
		}
	}

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(id), func(i, j int) { id[i], id[j] = id[j], id[i] })

	for _, table := range tables {
		file, err := os.Create(fmt.Sprintf("insert_randomly_%s.sql", table))
		if err != nil {
			panic(err)
		}
		for i := 0; i < 1000000; i++ {
			_, err := file.WriteString(fmt.Sprintf("INSERT INTO `%s` (`id`) VALUES(%d);\n", table, id[i]))
			if err != nil {
				panic(err)
			}
		}
	}
}

func generateQuerySingle() {
	id := make([]int, 0, 1000000)
	for i := 0; i < 1000000; i++ {
		id = append(id, i+1)
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(id), func(i, j int) { id[i], id[j] = id[j], id[i] })

	for _, table := range tables {
		file, err := os.Create(fmt.Sprintf("query_single_%s.sql", table))
		if err != nil {
			panic(err)
		}
		for i := 0; i < 10000; i++ {
			_, err := file.WriteString(fmt.Sprintf("SELECT * FROM `%s` WHERE id = %d;\n", table, id[i]))
			if err != nil {
				panic(err)
			}
		}
	}
}

func generateQueryRange() {
	id := make([]int, 0, 1000000)
	for i := 0; i < 1000000; i++ {
		id = append(id, i+1)
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(id), func(i, j int) { id[i], id[j] = id[j], id[i] })

	for _, table := range tables {
		file, err := os.Create(fmt.Sprintf("query_range_%s.sql", table))
		if err != nil {
			panic(err)
		}
		for i := 0; i < 10000; i++ {
			_, err := file.WriteString(fmt.Sprintf("SELECT * FROM `%s` WHERE id > %d AND id < %d;\n", table, id[i], id[i]+300))
			if err != nil {
				panic(err)
			}
		}
	}
}
