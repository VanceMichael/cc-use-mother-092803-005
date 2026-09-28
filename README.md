# 地质灾害响应

这是一个面向风险区预警与转移回执的后端服务起始项目。

## 目录

`app/` 放置领域对象和服务入口，`tests/` 保存行为测试。

## 测试

```bash
python3 -m unittest discover -s tests
```

## 构建检查

```bash
python3 -m compileall app
```

## 使用

服务以本地 Python 模块运行，数据默认保存在调用方提供的 SQLite 文件中。
