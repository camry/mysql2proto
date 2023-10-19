package main

import (
    "log"

    "github.com/camry/mysql2proto/cmd"
)

func main() {
    if err := cmd.Execute(); err != nil {
        log.Fatalln(err)
    }
}
