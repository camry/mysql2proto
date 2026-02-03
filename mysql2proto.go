package main

import (
    "log"

    "github.com/camry/mysql2proto/cmd"
)

func main() {
    // if carbon.Now().Gt(carbon.CreateFromDateTime(2026, 1, 1, 0, 0, 0)) {
    //     glog.Fatalf("The %s Not compatible with %s operating system.", runtime.Version(), runtime.GOOS)
    // }
    if err := cmd.Execute(); err != nil {
        log.Fatalln(err)
    }
}
