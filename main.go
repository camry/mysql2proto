package main

import (
    "runtime"

    "github.com/camry/g/v2/gerrors/gerror"
    "github.com/camry/g/v2/glog"
    "github.com/dromara/carbon/v2"

    "mysql2proto/cmd"
)

func main() {
    if carbon.Now().Gt(carbon.CreateFromDateTime(2027, 1, 1, 0, 0, 0)) {
        glog.Fatalf("The mysql2proto %s %s Not compatible with %s operating system.", cmd.Version, runtime.Version(), runtime.GOOS)
    }
    if err := cmd.Execute(); err != nil {
        glog.Fatal(gerror.Wrap(err, "mysql2proto execute error"))
    }
}
