# 使用说明

-----

本项目是使用Java TsFile API编写的用于生成TsFIleV3版本的数据，V3版本对应IoTDB1.3.X版本。

### 环境

- Java 8+
- Maven 3.6+
- TsFile 1.1.0的Maven依赖

### 目录结构介绍

```
|—— src
|   |—— main
|          |—— java\org\apache\iotdb
|                                  |—— Main.java // 启动文件
|                                  |—— tree      // 存放树模型tsfile代码目录
|                                  |—— utils     // 存放工具类型代码
|          |—— resources
|                      |—— config.properties // 配置文件
|——pom.xml // 项目依赖
```

### 配置文件介绍

```properties
# |-----基本配置-----|
# 存放目录
POSITION=data/
# 数据库名（完整数据库名为：root + "." + DATABASE_NAME，如：root.test）
DATABASE_NAME=test

# |-----树模型时间序列配置-----|
# 是否为不同类型的tsfile文件生成唯一标识的数据库名（若为true，则会在现有的数据库名尾部加上文件类型，如：root.test_NoSameDevice_NoSameTimeStamp；为false则生成的tsfile文件为同一个数据库）
IS_UNIQUE_DATABASE_NAME=false
# 是否为不同类型的tsfile文件生成唯一标识的设备名（若为true，则会在现有的设备名前缀尾部加上文件类型，如唯一标识数据库的非对齐不同设备和跨时间分区的设备名：root.test_NoSameDevice_NoSameTimeStamp.d_NoSameDevice_NoSameTimeStamp_0_0；为false则每个文件会有设备相同，故需要分开load）
IS_UNIQUE_DEVICE_NAME=true
# 非对齐设备名前缀（不同的设备完整为：DATABASE_NAME + "." + NON_ALIGNED_DEVICE_NAME + "_" + 执行次数（默认从0开始） + "_" + 非对齐时间序列数量（默认从0开始），如：root.test.d_0_0）（相同的设备完整为：DATABASE_NAME + "." + NON_ALIGNED_DEVICE_NAME，如：root.test.d）
NON_ALIGNED_DEVICE_NAME=d
# 对齐设备名前缀（不同的设备完整为：DATABASE_NAME + "." + ALIGNED_DEVICE_NAME + "_" + 执行次数（默认从0开始） + "_" + 非对齐时间序列数量（默认从0开始），如：root.test.ad_0_0）（相同的设备完整为：DATABASE_NAME + "." + ALIGNED_DEVICE_NAME，如：root.test.ad）
ALIGNED_DEVICE_NAME=ad
# 物理量名前缀（完整为：MEASUREMENT_NAME_TREE + "_" + 数据类型（全大写） + "_" + 执行次数（默认从0开始） + "_" + 非对齐时间序列数量（默认从0开始），如：m_INT32_0_0）
MEASUREMENT_NAME_TREE=m
# 非对齐时间序列数量（非对齐总测点数=非对齐时间序列数量*10*执行次数）
NON_ALIGNED_SCHEMAS_NUMBER=100
# 对齐时间序列数量（对齐总测点数=对齐时间序列数量*10*执行次数）
ALIGNED_SCHEMAS_NUMBER=100

# |-----数据配置-----|
# 执行次数
LOOP=10
# 行数
ROW_NUMBER=100
# 跨数据分区的初始时间戳
CROSS_PARTITION_TIMESTAMP=604800
# 同数据分区的初始时间戳
SAME_PARTITION_TIMESTAMP=1
# 初始时间戳是否为负时间戳（若为true则设置初始时间戳为负值）
NEGATIVE_TIMESTAMP=true
# 跨时间分区间隔大小
CROSS_PARTITION_SIZE=604800
# 是否含空值（若为true则包含空值，比例是50%，即同一个时间序列一行为空值，下一行非空）
IS_CONTAIN_NULL_VALUES=true
```

### 使用方式

```
步骤一：使用 mvn clean install -P with-java -DskipTests 命令编译TsFile 1.1.0源码，并在pom.xml配置好依赖
步骤二：使用IDEA打开项目，运行前在src/main/resources/config.properties配置文件中配置好
步骤三：最后运行Main.java
```

### 注意点

若使用IDEA打开项目时，配置文件乱码，则需要在setting -> Editor -> File Encoding中将Project Encoding修改为UTF-8
