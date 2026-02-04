# mysql2proto

mysql to proto

## 建表

```mysql
CREATE TABLE `base_json` (
  `json_id` int NOT NULL COMMENT 'JSON ID',
  `json_type` enum('Array','Map','Object') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT 'JSON 类型（Array,Map,Object）',
  `json_format` json NOT NULL COMMENT 'JSON 格式',
  `proto_id` int NOT NULL COMMENT 'PROTO ID',
  `proto_name` varchar(50) COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT 'PROTO 名称',
  PRIMARY KEY (`json_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='JSON 配置';
```

```mysql
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

## 配置

在 json 字段注释中添加 `json_id=xxx`

### 示例

- fall_skill_list: 注释中添加了 json_id=1001
- fall_attr_list: 注释中添加了 json_id=1002

```mysql
CREATE TABLE `base_codex_fall` (
  `fall_id` int NOT NULL COMMENT '掉落ID',
  `fall_group` int NOT NULL COMMENT '掉落组号',
  `fall_order` int NOT NULL COMMENT '掉落序号',
  `fall_type` tinyint NOT NULL COMMENT '掉落种类（参考 `base_fall_type` 表）',
  `fall_type_id` int NOT NULL COMMENT '掉落种类编号',
  `fall_num` int NOT NULL DEFAULT '0' COMMENT '掉落数量',
  `fall_level` int NOT NULL DEFAULT '0' COMMENT '掉落等级',
  `fall_step_level` int NOT NULL COMMENT '掉落阶级',
  `fall_star_level` int NOT NULL COMMENT '掉落星级',
  `fall_skill_list` json DEFAULT NULL COMMENT '掉落技能列表 json_id=1001（格式：[{"skill_id":1001,"skill_level":1},...]）',
  `fall_attr_list` json DEFAULT NULL COMMENT '掉落属性列表 json_id=1002（格式：[{"attr_id":1,"attr_value":"1"},...]）',
  PRIMARY KEY (`fall_id`,`fall_group`,`fall_order`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='图鉴 - 徽章固定掉落配置表';
```

## 运行

```bash
mysql2proto --server dev:Zqa5Q5pYgvmT@192.168.110.112:3306 --db treasure_base  --config ./config/ignore.yaml --out ../../treasure/treasure-proto/cbpb --pkg cbpb --go-pkg gitlab.walrus.cs/treasure/treasure-pb/cbpb
```
