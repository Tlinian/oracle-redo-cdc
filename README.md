# Oracle Redo 日志解析工具

## 项目简介

这是一个用于解析 Oracle Redo 日志的工具，能够解析 Redo 日志文件中的 DML（数据操作语言）和 DDL（数据定义语言）操作，并将其转换为结构化的事件数据。该工具支持通过配置文件进行灵活配置，可用于数据恢复、审计和分析等场景。

## 功能特性

- ✅ 解析 Oracle Redo 日志文件
- ✅ 支持 DML 操作解析（INSERT、UPDATE、DELETE）
- ✅ 支持 DDL 操作解析
- ✅ 事务提交事件解析
- ✅ 可配置起始 SCN（系统变更号）
- ✅ 支持文件模式和数据库模式两种解析方式
- ✅ 灵活的配置文件支持
- ✅ 命令行界面支持

## 技术栈

- **Java 17** - 开发语言
- **Gradle** - 构建工具
- **Picocli** - 命令行界面框架
- **Jackson** - JSON 处理库
- **Lombok** - 代码简化工具
- **Oracle JDBC** - 数据库连接驱动

## 快速开始

### 环境要求

- JDK 17 或更高版本
- Gradle 7.0 或更高版本
- Oracle 数据库（可选，仅用于数据库模式）

### 构建项目

```bash
# 克隆项目
git clone <repository-url>
cd oracle-redo-analysis

# 构建项目
./gradlew build

# 生成可执行 JAR 文件
./gradlew shadowJar
```

### 运行工具

```bash
# 使用命令行选项运行
java -jar build/libs/redo-tool-0.1.0.jar --parse-path=src\main\resources\application.properties

# 查看帮助信息
java -jar build/libs/redo-tool-0.1.0.jar --help
```

## 配置说明

工具使用配置文件来指定解析参数，配置文件采用 properties 格式。以下是支持的配置项：

| 配置项 | 说明 | 示例值 |
|--------|------|--------|
| redo.file.path | Redo 日志文件路径 | /path/to/redo.log |
| start.scn | 起始 SCN（0 表示从开始解析） | 1234567890 |
| url | 数据库连接 URL（仅用于数据库模式） | jdbc:oracle:thin:@localhost:1521:ORCL |
| user | 数据库用户名（仅用于数据库模式） | sys |
| password | 数据库密码（仅用于数据库模式） | password |
| database | 数据库名称（仅用于数据库模式） | ORCL |
| schema.list | 要解析的模式列表（仅用于数据库模式） | SCOTT,HR |
| miner.mode | 解析模式（file 或 database） | file |
| origin.path | 原始路径（可选） | /path/to/origin |
| target.path | 目标路径（可选） | /path/to/target |

示例配置文件 `src\main\resources\application.properties`：

```properties
redo.file.path=/path/to/redo.log
start.scn=0
url=jdbc:oracle:thin:@localhost:1521:ORCL
user=sys
password=password
database=ORCL
schema.list=SCOTT
miner.mode=file
```

## 使用示例

### 解析单个 Redo 日志文件

```bash
java -jar redo-tool-0.1.0.jar --parse-path=src\main\resources\application.properties --limit-records=100
```

### 从指定 SCN 开始解析

在配置文件中设置 `start.scn=1234567890`，然后运行：

```bash
java -jar redo-tool-0.1.0.jar --parse-path=src\main\resources\application.properties
```

## 项目结构

```
oracle-redo-analysis/
├── src/
│   ├── main/
│   │   ├── java/com/example/redo/
│   │   │   ├── App.java              # 应用程序入口
│   │   │                                                                                                                                                                                                                                                                                                                                                   ├── App.java              # 应用程序入口
│   │   │   ├── ConvertLlbRedoRecord.java  # LLB Redo 记录转换工具
│   │   ├── ConvertRedoRecord.java      # Redo 记录转换工具
│   │   ├── RedoClient.java           # Redo 客户端
│   │   │   ├── config/               # 配置相关
│   │   │   │   └── Config.java       # 配置类
│   │   │   ├── deserialize/         # 反序列化模块
│   │   │   │   ├── CommitEvent.java  # 提交事件
│   │   │   │   ├── DdlEvent.java      # DDL 事件
│   │   │   │   ├── DeleteEvent.java   # 删除事件
│   │   │   │   ├── Deserializer.java   # 反序列化接口
│   │   │   │   ├── DmlEvent.java     # DML 事件
│   │   │   │   ├── EventType.java     # 事件类型
│   │   │   │   ├── InsertEvent.java   # 插入事件
│   │   │   │   ├── PrintDeserializer.java # 打印反序列化器
│   │   │   │   ├── RBA.java          # 重做块地址
│   │   │   │   ├── RecordDeserializer.java # 记录反序列化器
│   │   │   │   ├── RedoEvent.java    # Redo 事件
│   │   │   │   └── UpdateEvent.java   # 更新事件
│   │   │   ├── metadata/            # 元数据管理
│   │   │   │   ├── MetadataManager.java # 元数据管理器
│   │   │   │   ├── TableMetadata.java   # 表元数据
│   │   │   │   └── ColumnMetadataFactory.java # 列元数据工厂
│   │   │   ├── model/               # 数据模型
│   │   │   │   ├── BlockHeader.java  # 块头
│   │   │   │   ├── RedoRecord.java    # Redo 记录
│   │   │   │   ├── RedoChange.java    # Redo 变更
│   │   │   │   └── decoration/       # 装饰器
│   │   │   └── origin/              # 原始数据模型
│   │   │   ├── parser/               # 解析器
│   │   │   │   ├── RedoParser.java    # Redo 解析器
│   │   │   │   ├── RedoMiner.java     # Redo 挖掘器
│   │   │   │   └── RecordConvertor.java # 记录转换器
│   │   │   └── util/                # 工具类
│   │   │       └── BinaryUtil.java    # 二进制工具
│   │   └── resources/               # 资源文件
│   └── test/                        # 测试代码
├── build.gradle                     # Gradle 构建脚本
├── settings.gradle                  # Gradle 设置
└── test-config.properties           # 测试配置文件
```

## 核心模块说明

1. **App** - 应用程序入口，处理命令行参数和程序流程
2. **Config** - 配置管理，从配置文件读取参数
3. **RedoParser** - 核心解析器，负责解析 Redo 日志文件
4. **Deserializer** - 反序列化模块，将原始数据转换为结构化事件
5. **MetadataManager** - 元数据管理器，处理表和列的元数据
6. **RedoClient** - Redo 客户端，支持数据库模式的解析

## 构建和运行

### 构建命令

```bash
# 构建项目
./gradlew build

# 运行测试
./gradlew test

# 生成可执行 JAR
./gradlew shadowJar
```

### 运行命令


```bash
# 基本运行
java -jar build/libs/redo-tool-0.1.0.jar --parse-path=src\main\resources\application.properties

# 限制解析记录数
java -jar build/libs/redo-tool-0.1.0.jar --parse-path=src\main\resources\application.properties --limit-records=500
```

## 注意事项

1. 确保 JDK 版本为 17 或更高
2. 解析大型 Redo 日志文件时可能需要较长时间和较多内存
3. 数据库模式需要正确配置 Oracle 数据库连接信息
4. 起始 SCN 设置为 0 表示解析整个文件
5. 建议先测试小文件以确保配置正确

## 许可证

本项目采用 MIT 许可证，详见 [LICENSE](LICENSE) 文件。

## 贡献

欢迎提交 Issue 和 Pull Request！

## 联系方式

此项目只是为了学习和研究 Oracle Redo 日志解析，如有问题或建议，请通过以下方式联系：

- 邮箱：[2629731238@qq.com]
- GitHub：[项目地址](https://github.com/yourusername/oracle-redo-analysis)