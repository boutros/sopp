package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/boutros/sopp"
)

const importBatchSize = 1000

func main() {
	log.SetFlags(0)
	log.SetPrefix("sopp: ")

	importF := flag.String("i", "", "import nt/ttl to db")
	baseURI := flag.String("base", "http://localhost/", "base URI")
	dump := flag.Bool("d", false, "dump database as turtle to standard out")

	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: sopp <flags> <database file>")
		flag.PrintDefaults()
	}

	flag.Parse()

	if len(flag.Args()) < 1 || *baseURI == "" {
		flag.Usage()
		os.Exit(1)
	}

	db, err := sopp.Open(flag.Args()[0], *baseURI)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if *importF != "" {
		rdf, err := os.Open(*importF)
		if err != nil {
			log.Fatal(err)
		}

		n, err := db.Import(rdf, importBatchSize)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("imported %d triples from %s", n, *importF)
	}

	if *dump {
		if err := db.Dump(os.Stdout); err != nil {
			log.Fatal(err)
		}
	}
}
