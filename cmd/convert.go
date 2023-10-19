package cmd

import (
    "bytes"
    "fmt"
    "os"
    "strings"
    "text/template"

    "github.com/camry/g/gutil"
    "github.com/spf13/cobra"
    "golang.org/x/text/cases"
    "golang.org/x/text/language"
    "gorm.io/gorm"
)

type Converter struct {
    serverDbConfig       *DbConfig
    serverDb             *gorm.DB
    serverTable          *Table
    ignoreTable          *IgnoreTable
    serverTableColumns   []string
    serverTableColumnMap map[string]*MySQL2ProtoColumn
}

type MySQL2ProtoColumn struct {
    DataType      string
    ProtoDataType string
}

type ProtoTemplate struct {
    TableName    string
    ProtoColumns []ProtoColumn
}

type ProtoColumn struct {
    ColumnName string
    ColumnType string
    ColumnNum  int32
}

// NewConverter 新建转换器。
func NewConverter(serverDbConfig *DbConfig, serverDb *gorm.DB, serverTable *Table, ignoreTable *IgnoreTable) *Converter {
    return &Converter{
        serverDbConfig:       serverDbConfig,
        serverDb:             serverDb,
        serverTable:          serverTable,
        ignoreTable:          ignoreTable,
        serverTableColumnMap: make(map[string]*MySQL2ProtoColumn),
    }
}

// Start 启动。
func (c *Converter) Start() {
    defer wg.Done()
    ch <- true

    switch c.serverTable.TableType {
    case "BASE TABLE":
        c.create()
    case "VIEW":
        // glog.Warnf("表 `%s` 不支持 VIEW 转换。", c.serverTable.TableName)
    }

    <-ch
}

// create 创建 PROTO。
func (c *Converter) create() {
    var (
        serverColumnData []Column
    )

    serverTableColumnResult := c.serverDb.Table("COLUMNS").Order("`ORDINAL_POSITION` ASC").Find(
        &serverColumnData,
        "`TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?",
        c.serverDbConfig.Database, c.serverTable.TableName,
    )

    if serverTableColumnResult.RowsAffected > 0 {
        // COLUMNS ...
        for _, serverColumn := range serverColumnData {
            if gutil.InArray(serverColumn.ColumnName, c.ignoreTable.Columns) {
                continue
            }

            dataType := strings.ToUpper(serverColumn.DataType)
            protoDataType := c.getDataType(dataType)

            c.serverTableColumns = append(c.serverTableColumns, serverColumn.ColumnName)
            c.serverTableColumnMap[serverColumn.ColumnName] = &MySQL2ProtoColumn{
                DataType:      dataType,
                ProtoDataType: protoDataType,
            }
        }

        pt := ProtoTemplate{TableName: c.toCamelCase(c.serverTable.TableName)}
        for i, columnName := range c.serverTableColumns {
            if pc, ok := c.serverTableColumnMap[columnName]; ok {
                pt.ProtoColumns = append(pt.ProtoColumns, ProtoColumn{ColumnName: columnName, ColumnType: pc.ProtoDataType, ColumnNum: int32(i + 1)})
            }
        }

        tpl, err := template.ParseFiles("template/proto.tpl")
        cobra.CheckErr(err)
        var wr bytes.Buffer
        err1 := tpl.Execute(&wr, pt)
        cobra.CheckErr(err1)
        err2 := os.WriteFile(fmt.Sprintf("%s/%s.proto", out, c.serverTable.TableName), wr.Bytes(), 0644)
        cobra.CheckErr(err2)
    }
}

// getDataType 数据类型。
func (c *Converter) getDataType(dataType string) string {
    switch dataType {
    case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER":
        return "int32"
    case "BIGINT":
        return "int64"
    case "FLOAT", "DECIMAL":
        return "float32"
    case "DOUBLE":
        return "float64"
    case "DATE", "TIME", "YEAR", "DATETIME", "TIMESTAMP", "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT":
        return "string"
    case "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB":
        return "bytes"
    }
    return "string"
}

func (c *Converter) toCamelCase(s string) string {
    words := strings.Split(s, "_")
    for i := 0; i < len(words); i++ {
        words[i] = cases.Title(language.English).String(words[i])
    }
    return strings.Join(words, "")
}
