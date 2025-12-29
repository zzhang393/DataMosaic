# MCP 配置文件说明

本目录包含各个MCP（Model Context Protocol）模块的配置文件，用于定义数据质量约束规则和修复策略。

## 配置文件列表

### 1. format_rules.json
格式约束规则配置文件

**配置内容：**
- 字段格式规则定义
- 标准格式模式（正则表达式）
- 格式转换映射表
- 标准示例值

**支持的字段：**
- `季度`：中文季度格式（第一季度、第二季度等）
- `税额`：金额格式（亿元）

**使用场景：**
- 格式验证：检查字段值是否符合标准格式
- 格式修复：将非标准格式转换为标准格式
- 批量统一：确保同一字段在表格中使用统一格式

### 2. aggregation_rules.json
聚合约束规则配置文件

**配置内容：**
- 聚合关键字定义（总计、平均等）
- 计算类型配置（求和、乘法、百分比等）
- 字段修复规则
- 容差值设置

**支持的修复类型：**
- `amount_unit`：金额单位统一
- `quarter_format`：季度格式统一

**使用场景：**
- 聚合验证：检查总计、平均值等聚合计算的正确性
- 单位统一：确保同类数据使用相同的单位
- 格式一致性：保证聚合数据的格式一致

## 配置文件结构

### format_rules.json 结构
```json
{
  "field_format_rules": {
    "字段名": {
      "pattern": "正则表达式",
      "standard_examples": ["示例1", "示例2"],
      "description": "规则描述",
      "mapping": {
        "变体1": "标准格式",
        "变体2": "标准格式"
      },
      "conversion_rules": [
        {
          "pattern": "匹配模式",
          "template": "转换模板",
          "description": "转换说明"
        }
      ]
    }
  }
}
```

### aggregation_rules.json 结构
```json
{
  "aggregation_keywords": {
    "total": ["关键字列表"],
    "average": ["关键字列表"]
  },
  "calculation_types": {
    "类型名": {
      "description": "描述",
      "tolerance": 容差值
    }
  },
  "field_fix_rules": {
    "字段名": {
      "type": "修复类型",
      "target_format": "目标格式",
      "mapping": {},
      "conversion_rules": []
    }
  }
}
```

## 如何添加新的规则

### 添加新的格式规则

1. 在 `format_rules.json` 的 `field_format_rules` 中添加新字段：
```json
"新字段名": {
  "pattern": "^正则表达式$",
  "standard_examples": ["示例"],
  "description": "规则描述",
  "mapping": {
    "变体": "标准格式"
  }
}
```

2. 在 `format_mcp.py` 的 `FormatFixer.__init__()` 中添加对应的处理器：
```python
if field_name == '新字段名':
    self.format_normalizers[field_name] = self._normalize_新字段
```

3. 实现标准化方法：
```python
def _normalize_新字段(self, value: str) -> Optional[str]:
    # 实现转换逻辑
    pass
```

### 添加新的聚合规则

1. 在 `aggregation_rules.json` 中添加新的字段修复规则
2. 在 `AggregationFixer` 中添加对应的处理方法
3. 在 `_generate_aggregation_fix` 中添加新的修复类型判断

## 最佳实践

1. **配置优先**：所有规则应优先在JSON配置文件中定义，避免硬编码
2. **统一格式**：同一类型的字段应使用统一的标准格式
3. **渐进增强**：新增规则时保持向后兼容
4. **文档同步**：修改配置时同步更新本文档

## 注意事项

- 正则表达式中的特殊字符需要转义（使用 `\\` 而不是 `\`）
- 映射表的键应为小写（程序会自动转换为小写进行匹配）
- 容差值用于浮点数比较，避免精度问题
- 配置文件必须是有效的JSON格式，注意不要有尾随逗号

