# 编译安装

1. 确认在编译的服务器上已经安装```go1.15.5```版本，若未安装，可使用以下命令进行安装：

```shell
# 以下命令仅适用于ubuntu系统
# 1.拉取资源文件
wget go1.15.5.linux-amd64.tar.gz
# 2.解压并安装
rm -rf /usr/local/go && tar -C /usr/local -xzf go1.15.6.linux-amd64.tar.gz
# 3.导出环境变量
export PATH=$PATH:/usr/local/go/bin
# 或者将以上导出变量命令加入$HOME/.profile 或 /etc/profile
```

2. 使用如下命令编译安装

```shell
make all
sudo make install
```

# 使用

1. 配置文件

   配置文件目前分为computers(所有参与文件转移的服务器)、tasks(转移任务)、singlethreadmbps(单线程限速MB/s)

   格式以下图为例：

   

   ![](https://markdown-pub.oss-cn-shanghai.aliyuncs.com/blog/5eomp.png)

2. 部署方式

   整个数据流程走向为：源服务器——>中转服务器(可以是源服务器自身)——>目标服务器

   1. 工具需要一个带宽较大的中转服务器，配置进默认为/home/$USER/mv_sectors.yaml文件中（也可以用--path指定配置文件路径）。
   2. 工具部署在中转服务器上
   3. 配置对应的源服务器信息和目标服务器信息

3. 运行方式

   配置完配置文件后使用如下命令启动程序：

   - 拷贝cache文件：

   ```shell
   nohup move_sectors run --Cache(-C/-c) >> ~/move_sectors.log &
   # 或者指定配置文件
   nohup move_sectors run --Cache(-C/-c) --path configPath >> ~/move_sectors.log &
   ```

   - 拷贝sealed文件

   ```shell
   nohup move_sectors run --Sealed(-S/-s) >> ~/move_sectors.log &
   # 或者指定配置文件
   nohup move_sectors run --Sealed(-S/-s) --path configPath >> ~/move_sectors.log &
   ```

   - 拷贝unsealed文件

   ```shell
   nohup move_sectors run --UnSealed(-U/-u) >> ~/move_sectors.log &
   # 或者指定配置文件
   nohup move_sectors run --UnSealed(-U/-u) --path configPath >> ~/move_sectors.log &
   ```
   
   - 指定sector拷贝
   
   ```shell
   # 首先将需要拷贝的sectorID以一行一个的形式写入文件，使用--SectorListFile(-SF/-sf)指定该文件路径
   # 以sealed文件为例：
   nohup move_sectors run --Sealed(-S/-s) --SectorListFile(-SF/-sf) $FILEPATH >> ~/move_sectors.log &
   # 或者指定配置文件
   nohup move_sectors run --Sealed(-S/-s) --SectorListFile(-SF/-sf) $FILEPATH --path configPath >> ~/move_sectors.log &
   ```
   
   - 使用以下环境变量可以打印详细日志
   
   ```shell
   export GOLOG_LOG_LEVEL=DEBUG
   ```
   
   - 如果要跳过大小不对的文件，让程序继续运行
   
   ```shell
   # 在命令行中添加--SkipSourceError
   ```
   
   
   
   