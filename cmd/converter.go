package cmd

import (
    "bytes"
    "fmt"
    "os"
    "strings"
    "text/template"

    "github.com/camry/g/v2/gtext/gregex"
    "github.com/samber/lo"
    "github.com/spf13/cast"
    "github.com/spf13/cobra"
    "gorm.io/gorm"
)

type Converter struct {
    serverDbConfig       *DbConfig
    serverDb             *gorm.DB
    serverTable          *Table
    ignoreTable          *IgnoreTable
    serverTableColumns   []string
    serverTableColumnMap map[string]*MySQL2ProtoColumn
    baseJsonMap          map[int32]*BaseJson
    isJsonColumn         bool
}

type MySQL2ProtoColumn struct {
    *Column
    ProtoDataType string
    ColumnComment string
}

type ProtoTemplate struct {
    PackageName   string
    GoPackageName string
    IsJsonColumn  bool
    Imports       []string
    TableName     string
    ProtoColumns  []ProtoColumn
}

type ProtoColumn struct {
    ColumnName    string
    ColumnType    string
    ColumnNum     int32
    ColumnExtend  string
    ColumnComment string
}

// NewConverter 新建转换器。
func NewConverter(serverDbConfig *DbConfig, serverDb *gorm.DB, serverTable *Table, ignoreTable *IgnoreTable, baseJsonMap map[int32]*BaseJson) *Converter {
    return &Converter{
        serverDbConfig:       serverDbConfig,
        serverDb:             serverDb,
        serverTable:          serverTable,
        ignoreTable:          ignoreTable,
        serverTableColumnMap: make(map[string]*MySQL2ProtoColumn),
        baseJsonMap:          baseJsonMap,
    }
}

// Start 启动。
func (x *Converter) Start() {
    defer wg.Done()
    ch <- true

    switch x.serverTable.TableType {
    case "BASE TABLE":
        x.generate()
    case "VIEW":
        // glog.Warnf("表 `%s` 不支持 VIEW 转换。", x.serverTable.TableName)
    }

    <-ch
}

// generate 创建 PROTO。
func (x *Converter) generate() {
    var (
        serverColumnData []*Column
    )

    serverTableColumnResult := x.serverDb.Table("COLUMNS").Order("`ORDINAL_POSITION` ASC").Find(
        &serverColumnData,
        "`TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?",
        x.serverDbConfig.Database, x.serverTable.TableName,
    )

    if serverTableColumnResult.RowsAffected > 0 {
        // COLUMNS ...
        for _, serverColumn := range serverColumnData {
            if lo.Contains(x.ignoreTable.Columns, serverColumn.ColumnName) {
                continue
            }
            serverColumn.DataType = strings.ToUpper(serverColumn.DataType)
            x.serverTableColumns = append(x.serverTableColumns, serverColumn.ColumnName)
            x.serverTableColumnMap[serverColumn.ColumnName] = &MySQL2ProtoColumn{
                Column:        serverColumn,
                ProtoDataType: x.getProtoDataType(serverColumn),
                ColumnComment: strings.ReplaceAll(serverColumn.ColumnComment, "\n", "; "),
            }
        }

        p := ProtoTemplate{
            PackageName:   pkg,
            GoPackageName: goPkg,
            IsJsonColumn:  x.isJsonColumn,
            TableName:     lo.CamelCase(x.serverTable.TableName),
        }
        if x.isJsonColumn {
            p.Imports = append(p.Imports, "google/protobuf/descriptor.proto")
            p.Imports = append(p.Imports, fmt.Sprintf("%s/base_json.proto", pkg))
        }
        for i, columnName := range x.serverTableColumns {
            if pc, ok := x.serverTableColumnMap[columnName]; ok {
                protoColumn := ProtoColumn{
                    ColumnName:    columnName,
                    ColumnType:    pc.ProtoDataType,
                    ColumnNum:     int32(i + 1),
                    ColumnComment: pc.ColumnComment,
                }
                if pc.DataType == "JSON" && pc.ProtoDataType != "string" {
                    protoColumn.ColumnExtend = " [(google.protobuf.FieldOptions.is_json) = true]"
                }
                p.ProtoColumns = append(p.ProtoColumns, protoColumn)
            }
        }

        tpl, err := template.ParseFiles("template/proto.tpl")
        cobra.CheckErr(err)
        var wr bytes.Buffer
        err = tpl.Execute(&wr, p)
        cobra.CheckErr(err)
        err = os.WriteFile(fmt.Sprintf("%s/%s.proto", out, x.serverTable.TableName), wr.Bytes(), 0644)
        cobra.CheckErr(err)
    }
}

// getProtoDataType PROTO 数据类型。
func (x *Converter) getProtoDataType(serverColumn *Column) string {
    switch serverColumn.DataType {
    case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER":
        return "int32"
    case "BIGINT":
        return "int64"
    case "FLOAT", "DECIMAL":
        return "float"
    case "DOUBLE":
        return "double"
    case "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB":
        return "bytes"
    case "JSON":
        matchList, err := gregex.MatchString("json_id=(\\d+)", serverColumn.ColumnComment)
        if err != nil {
            goto END
        }
        if len(matchList) > 1 {
            jsonId := cast.ToInt32(matchList[1])
            if baseJson, ok := x.baseJsonMap[jsonId]; ok {
                x.isJsonColumn = true
                if baseJson.JsonType == "Array" {
                    return "repeated " + lo.PascalCase(baseJson.JsonName)
                }
                return lo.PascalCase(baseJson.JsonName)
            }
        }
    }
END:
    return "string"
}
