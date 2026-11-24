package cmd

import (
    "bytes"
    "fmt"
    "os"
    "text/template"

    "github.com/spf13/cobra"
)

type BaseJson struct {
    JsonID        int32          `gorm:"column:json_id"`
    JsonType      string         `gorm:"column:json_type"`
    JsonFormat    string         `gorm:"column:json_format"`
    ProtoID       int32          `gorm:"column:proto_id"`
    BaseJsonProto *BaseJsonProto `gorm:"foreignKey:proto_id;references:proto_id"`
}

func (x *BaseJson) TableName() string {
    return "base_json"
}

type BaseJsonProto struct {
    ProtoID                 int32                  `gorm:"column:proto_id"`
    ProtoName               string                 `gorm:"column:proto_name"`
    IsProtoColumn           bool                   `gorm:"column:is_proto_column"`
    BaseJsonProtoColumnList []*BaseJsonProtoColumn `gorm:"foreignKey:proto_id;references:proto_id"`
}

func (x *BaseJsonProto) TableName() string {
    return "base_json_proto"
}

type BaseJsonProtoColumn struct {
    ProtoID        int32  `gorm:"column:proto_id"`
    ColumnPosition int32  `gorm:"column:column_position"`
    ColumnRule     string `gorm:"column:column_rule"`
    ColumnType     string `gorm:"column:column_type"`
    ColumnName     string `gorm:"column:column_name"`
    ColumnDesc     string `gorm:"column:column_desc"`
}

func (x *BaseJsonProtoColumn) TableName() string {
    return "base_json_proto_column"
}

type ProtoJsonTemplate struct {
    PackageName   string
    GoPackageName string
    ProtoMessages []*ProtoMessage
}

type ProtoMessage struct {
    ProtoName    string
    ProtoColumns []*ProtoColumn
}

type ConverterJson struct {
    baseJsonList []*BaseJson
}

// NewConverterJson 新建 JSON 转换器。
func NewConverterJson(baseJsonList []*BaseJson) *ConverterJson {
    return &ConverterJson{
        baseJsonList: baseJsonList,
    }
}

// Start 启动。
func (x *ConverterJson) Start() {
    defer wg.Done()
    ch <- true

    x.generate()

    <-ch
}

// generate 创建 PROTO。
func (x *ConverterJson) generate() {
    p := ProtoJsonTemplate{
        PackageName:   pkg,
        GoPackageName: goPkg,
    }
    protoMessageMap := make(map[int32]*ProtoMessage, len(x.baseJsonList))
    for _, baseJson := range x.baseJsonList {
        if !baseJson.BaseJsonProto.IsProtoColumn {
            continue
        }
        m := &ProtoMessage{
            ProtoName: baseJson.BaseJsonProto.ProtoName,
        }
        for _, baseJsonProto := range baseJson.BaseJsonProto.BaseJsonProtoColumnList {
            m.ProtoColumns = append(m.ProtoColumns, &ProtoColumn{
                ColumnName:    baseJsonProto.ColumnName,
                ColumnType:    baseJsonProto.ColumnType,
                ColumnNum:     baseJsonProto.ColumnPosition,
                ColumnComment: baseJsonProto.ColumnDesc,
            })
        }
        if _, ok := protoMessageMap[baseJson.BaseJsonProto.ProtoID]; !ok {
            protoMessageMap[baseJson.BaseJsonProto.ProtoID] = m
            p.ProtoMessages = append(p.ProtoMessages, m)
        }
    }
    tpl, err := template.New("TemplateProtoJson").Parse(TemplateProtoJson)
    cobra.CheckErr(err)
    var wr bytes.Buffer
    err = tpl.Execute(&wr, p)
    cobra.CheckErr(err)
    err = os.WriteFile(fmt.Sprintf("%s/%s_base_json.proto", out, pkg), wr.Bytes(), 0644)
    cobra.CheckErr(err)
}
