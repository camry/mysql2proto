# mysql2proto

mysql to proto

## 建表

```sql
CREATE TABLE `base_json` (
  `json_id` int NOT NULL COMMENT 'JSON ID',
  `json_name` varchar(20) COLLATE utf8mb4_general_ci NOT NULL COMMENT 'JSON 名称',
  `json_type` enum('Array','Object') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT 'JSON 类型（Array,Map）',
  `json_format` json NOT NULL COMMENT 'JSON 格式',
  `proto_id` int NOT NULL COMMENT 'PROTO ID',
  PRIMARY KEY (`json_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='JSON 配置';


```

```sql
CREATE TABLE `base_json_proto` (
  `proto_id` int NOT NULL COMMENT 'PROTO ID',
  `column_position` int NOT NULL COMMENT '字段位置',
  `column_rule` enum('singular','optional','repeated') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '字段规则',
  `column_type` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '字段类型',
  `column_name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '字段名称',
  `column_desc` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '字段描述',
  PRIMARY KEY (`proto_id`,`column_position`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='JSON PROTO 配置表';
```
