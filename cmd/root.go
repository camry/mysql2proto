package cmd

import (
    "fmt"
    "os"
    "regexp"
    "strconv"
    "strings"
    "sync"

    "github.com/spf13/cobra"
    "gopkg.in/yaml.v3"
    "gorm.io/driver/mysql"
    "gorm.io/gorm"
    "gorm.io/gorm/logger"
)

const (
    Dsn         = "%s:%s@tcp(%s:%d)/information_schema?timeout=10s&parseTime=true&charset=%s"
    HostPattern = "^(.*)\\:(.*)\\@(.*)\\:(\\d+)$"
    DbPattern   = "^([A-Za-z0-9_]+)$"
)

func Execute() error {
    return rootCmd.Execute()
}

func init() {
    cobra.OnInitialize(initConfig)

    rootCmd.Flags().StringVarP(&server, "server", "s", "", "指定服务器。(格式: <user>:<password>@<host>:<port>)")
    rootCmd.Flags().StringVarP(&db, "db", "d", "", "指定数据库。")
    rootCmd.Flags().StringVarP(&cfgPath, "config", "c", "", "指定配置文件路径。")
    rootCmd.Flags().StringVarP(&out, "out", "o", "", "指定输出目录。")

    cobra.CheckErr(rootCmd.MarkFlagRequired("server"))
    cobra.CheckErr(rootCmd.MarkFlagRequired("db"))
}

func initConfig() {
}

type Config struct {
    Ignores []*IgnoreTable `yaml:"ignores"`
}

type IgnoreTable struct {
    Table   string   `yaml:"table"`
    Columns []string `yaml:"columns"`
}

var (
    wg sync.WaitGroup
    ch = make(chan bool, 16)

    server  string
    db      string
    cfgPath string
    out     string

    rootCmd = &cobra.Command{
        Use:     "mysql2proto",
        Short:   "MySQL convert to Proto.",
        Version: "v1.0.1",
        Run: func(cmd *cobra.Command, args []string) {
            serverMatched, err1 := regexp.MatchString(HostPattern, server)
            dbMatched, err2 := regexp.MatchString(DbPattern, db)
            cobra.CheckErr(err1)
            cobra.CheckErr(err2)
            if !serverMatched {
                cobra.CheckErr(fmt.Errorf("服务器 `%s` 格式错误。(正确格式: <user>:<password>@<host>:<port>)", server))
            }
            if !dbMatched {
                cobra.CheckErr(fmt.Errorf("数据库 `%s` 格式错误。", db))
            }

            var (
                serverUser = strings.Split(server[0:strings.LastIndex(server, "@")], ":")
                serverHost = strings.Split(server[strings.LastIndex(server, "@")+1:], ":")
                err        error
            )
            serverDbConfig := &DbConfig{
                User:     serverUser[0],
                Password: serverUser[1],
                Host:     serverHost[0],
                Charset:  "utf8mb4",
                Database: db,
            }
            serverDbConfig.Port, err = strconv.Atoi(serverHost[1])
            cobra.CheckErr(err)

            serverDb, err := gorm.Open(mysql.New(mysql.Config{
                DSN: fmt.Sprintf(Dsn,
                    serverDbConfig.User, serverDbConfig.Password,
                    serverDbConfig.Host, serverDbConfig.Port,
                    serverDbConfig.Charset,
                ),
            }), &gorm.Config{
                SkipDefaultTransaction: true,
                DisableAutomaticPing:   true,
                Logger:                 logger.Default.LogMode(logger.Silent),
            })
            cobra.CheckErr(err)

            var serverSchema Schema
            serverSchemaResult := serverDb.Table("SCHEMATA").Limit(1).Find(
                &serverSchema,
                "`SCHEMA_NAME` = ?", serverDbConfig.Database,
            )
            if serverSchemaResult.RowsAffected <= 0 {
                cobra.CheckErr(fmt.Errorf("数据库 `%s` 不存在。", serverDbConfig.Database))
            }

            var serverTableData []*Table
            serverTableResult := serverDb.Table("TABLES").Order("`TABLE_NAME` ASC").Find(
                &serverTableData,
                "`TABLE_SCHEMA` = ?", serverDbConfig.Database,
            )
            if serverTableResult.RowsAffected <= 0 {
                cobra.CheckErr(fmt.Errorf("数据库 `%s` 没有表。", serverDbConfig.Database))
            }

            // Load Ignore Config
            icMap := make(map[string]*IgnoreTable, 10)
            if cfgPath != "" {
                var ic *Config
                bytes, err := os.ReadFile(cfgPath)
                cobra.CheckErr(err)
                err = yaml.Unmarshal(bytes, &ic)
                cobra.CheckErr(err)

                for _, vv := range ic.Ignores {
                    icMap[vv.Table] = vv
                }
            }

            defer close(ch)
            for _, serverTable := range serverTableData {
                var ignoreTable = &IgnoreTable{}
                if v, ok := icMap[serverTable.TableName]; ok {
                    ignoreTable = v
                }
                isContinue := true
                if ignoreTable.Table == serverTable.TableName && len(ignoreTable.Columns) == 0 {
                    isContinue = false
                }
                if isContinue {
                    wg.Add(1)
                    go NewConverter(serverDbConfig, serverDb, serverTable, ignoreTable).Start()
                }
            }
            wg.Wait()
        },
    }
)
