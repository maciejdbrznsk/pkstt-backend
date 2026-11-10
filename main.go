package main

import (
	"log"
	"path/filepath"
)

func main() {
	destPath := filepath.Join("C:\\Users\\maciej\\Downloads", "latest.zip")
	url := "https://kmrzeszow.live/gtfs-pks/latest.zip"

	if err := downloadZIP(destPath, url); err != nil {
		log.Fatalf("Error: %v", err)
	}

	log.Println("File downloaded succesfully")
}
