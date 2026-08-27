package cmd

import (
    _ "embed"
    "fmt"
    "os"
    "regexp"
    "strconv"
    "strings"
    "sync"

    "github.com/camry/g/v2/gerrors/gerror"
    "github.com/samber/lo"
    "github.com/spf13/cobra"
    "gopkg.in/yaml.v3"
    "gorm.io/driver/mysql"
    "gorm.io/gorm"
    "gorm.io/gorm/logger"
)

const (
    Dsn         = "%s:%s@tcp(%s:%d)/%s?timeout=10s&parseTime=true&charset=%s"
    HostPattern = "^(.*)\\:(.*)\\@(.*)\\:(\\d+)$"
    DbPattern   = "^([A-Za-z0-9_]+)$"
)

var (
    //go:embed .version
    Version string
    //go:embed template/proto.tpl
    TemplateProto string
    //go:embed template/proto_json.tpl
    TemplateProtoJson string
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
    rootCmd.Flags().StringVarP(&pkg, "pkg", "p", "pb", "指定包名。")
    rootCmd.Flags().StringVarP(&goPkg, "go-pkg", "g", "", "指定 Go 包名。")

    cobra.CheckErr(rootCmd.MarkFlagRequired("server"))
    cobra.CheckErr(rootCmd.MarkFlagRequired("db"))
    cobra.CheckErr(rootCmd.MarkFlagRequired("config"))
    cobra.CheckErr(rootCmd.MarkFlagRequired("out"))
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
    pkg     string
    goPkg   string

    rootCmd = &cobra.Command{
        Use:     "mysql2proto",
        Short:   "mysql convert to proto.",
        Version: Version,
        Run: func(cmd *cobra.Command, args []string) {
            defer close(ch)
            serverMatched, err1 := regexp.MatchString(HostPattern, server)
            dbMatched, err2 := regexp.MatchString(DbPattern, db)
            cobra.CheckErr(err1)
            cobra.CheckErr(err2)
            if !serverMatched {
                cobra.CheckErr(gerror.Newf("服务器 `%s` 格式错误。(正确格式: <user>:<password>@<host>:<port>)", server))
            }
            if !dbMatched {
                cobra.CheckErr(gerror.Newf("数据库 `%s` 格式错误。", db))
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
                    "information_schema", serverDbConfig.Charset,
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
                cobra.CheckErr(gerror.Newf("数据库 `%s` 不存在。", serverDbConfig.Database))
            }

            var (
                serverTableData []*Table
                baseJsonList    []*BaseJson
                baseJsonMap     map[int32]*BaseJson
                jsonTableCount  int64
            )
            serverTableResult := serverDb.Table("TABLES").Order("`TABLE_NAME` ASC").Find(
                &serverTableData,
                "`TABLE_SCHEMA` = ?", serverDbConfig.Database,
            )
            if serverTableResult.RowsAffected <= 0 {
                cobra.CheckErr(gerror.Newf("数据库 `%s` 没有表。", serverDbConfig.Database))
            }

            // JSON PROTO Generate ...
            err = serverDb.Table("TABLES").Where("`TABLE_SCHEMA` = ? AND `TABLE_NAME` IN ?",
                serverDbConfig.Database,
                []string{"base_json", "base_json_proto", "base_json_proto_column"},
            ).Count(&jsonTableCount).Error
            cobra.CheckErr(err)

            if jsonTableCount > 0 {
                baseDb, err1 := gorm.Open(mysql.New(mysql.Config{
                    DSN: fmt.Sprintf(Dsn,
                        serverDbConfig.User, serverDbConfig.Password,
                        serverDbConfig.Host, serverDbConfig.Port,
                        serverDbConfig.Database, serverDbConfig.Charset,
                    ),
                }), &gorm.Config{
                    SkipDefaultTransaction: true,
                    DisableAutomaticPing:   true,
                    Logger:                 logger.Default.LogMode(logger.Silent),
                })
                cobra.CheckErr(err1)
                err1 = baseDb.Model(&BaseJson{}).Preload("BaseJsonProto").Preload("BaseJsonProto.BaseJsonProtoColumnList").Find(&baseJsonList).Error
                cobra.CheckErr(err1)
                baseJsonMap = lo.SliceToMap(baseJsonList, func(item *BaseJson) (int32, *BaseJson) {
                    if item.BaseJsonProto.IsProtoColumn {
                        item.BaseJsonProto.ProtoName = lo.PascalCase(item.BaseJsonProto.ProtoName)
                    }
                    return item.JsonID, item
                })
            }

            wg.Add(1)
            go NewConverterJson(baseJsonList).Start()

            // Load Ignore Config
            icMap := make(map[string]*IgnoreTable, 50)
            icMap["base_json"] = &IgnoreTable{
                Table: "base_json",
            }
            icMap["base_json_proto"] = &IgnoreTable{
                Table: "base_json_proto",
            }
            icMap["base_json_proto_column"] = &IgnoreTable{
                Table: "base_json_proto_column",
            }
            if cfgPath != "" {
                var ic *Config
                bytes, err1 := os.ReadFile(cfgPath)
                cobra.CheckErr(err1)
                err1 = yaml.Unmarshal(bytes, &ic)
                cobra.CheckErr(err1)

                for _, vv := range ic.Ignores {
                    icMap[vv.Table] = vv
                }
            }

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
                    go NewConverter(serverDbConfig, serverDb, serverTable, ignoreTable, baseJsonMap).Start()
                }
            }

            wg.Wait()
        },
    }
)
