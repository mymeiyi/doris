# 目标

 FE 减少对 meta_service 的 get_version 和 get_tablet_stats 接口的调用

# get_version 优化
## 现状
- 哪些对象有 version：table 和 partition
- meta-service中哪些操作会更新 version

  - table
    - Key: table_version_key
    - Val: int 
    - 修改操作 (调用update_table_version方法)
      - commitPartition / dropPartition / commitIndex
      - 提交事务(普通事务；大事务)
  - partition
    - Key: partition_version_key
    - Val: message VersionPB
    - 修改操作
      - preparePartition
      - 提交事务(普通事务；大事务)
        - ms 提交成功返回 partition version，fe 更新partition.setCachedVisibleVersion
        - 大事务 partition version kv 还没改，但返回的version 已改

  - 结论
    - 导入会更新 table 和 partition version
    - schema change(创建删除partition) 会更新 table verison
- fe 内存中的 version
  - table
    - 属性: 没有存储visible version
    - 属性: cachedTableVersion. 通过 session变量 cloud_table_version_cache_ttl_ms，决定是否使用cache version.
  - partition
    - 属性: visibleVersion
    - 属性: 没有存储 cached version, 当前的 visibleVersion 就是 cache. 通过session变量 cloud_partition_version_cache_ttl_ms，决定是否使用cache version.
 - fe中查询怎么获取 version
   - 在 ScanNode.setVisibleVersionForOlapScanNodes 中获取version

## 优化思路
- FE 主动更新 version
  - commit_txn 后更新 fe 内存中的 table 和 partition version
    - 如果 enable_stream_load_commit_txn_on_be = true (默认 false)，可能由 be 提交事务。这个 https://github.com/apache/doris/pull/59754 新增了当 be 提交事务成功后，转发给 master fe的逻辑，复用这个机制，fe 也可以知道be发起的事务提交。
  - schema change (及增删 partition) 后，主动更新内存中的 table version
  - follower fe 怎么更新 version
    - master FE 更新 version 后，给 followers 发 rpc（version map发过去，follower 不需要从 ms 拉）
- 由于version是主动更新的，内存中的 version 极大概率是对的
  - 因此，默认使用 version cache（cache 超时可以设置一个超大的值；或者Long.max 默认不会超时?）
  - 另外，可以提供 session 变量支持强制从 ms 获取
- version 的校正机制
  - 由于更新 partition version 一定会更新 table version。启动一个后台线程，定期从 ms 获取 table version。如果与内存中的 table version 不一致，则全量获取这个 table 的partition version。
  - 在更新可能失败的路径（比如提交事务 rpc 超时等），主动触发一次校正
- 实现中：
  - commit_txn 先更新 partition_version，再更新 table_version (适配上面的校正机制)
  - 需要对 partition version 做一致性的更新。否则可能有这种问题：
    1. 查询1 获取 partiton1 的cache version1
    2. 导入更新 partition1 的cache version为2，partition2 的cache version为2
    3. 查询1 获取 partiton2 的cache version2
- 另外，尽量保证未来新加或改的代码，引入非预期的 rpc 调用：默认使用cache
- 可能存在的问题
  - 数据可见性是否有问题
  - master FE 和 follower FE 版本一致性是否有问题

## 长期思路
- fe 等节点启动超长连接到 ms，ms 在数据变更后，主动通知相关的节点更新（依赖快照的operation log？）


# get_tablet_stats 优化
## 现状
- Tablet stats 信息是：message TabletStatsPB
- 哪些操作会更新 tablet stats
  - 导入
    - 导入如果是大事务提交，commit_txn返回时，tablet stats还没有更新
  - Compaction
  - schema change (新tablet)
  - commit_restore_job
- fe 怎么获取 tablet stats
  - 通过 CloudTabletStatMgr 线程，每 tablet_stat_update_interval_second = 60 秒，从 ms 拉取全部 tablet stats 
  - 如果 tablet 多的话，可能需要几十分钟才能完成一轮拉取
- Fe replica 用到 6 个字段；同时，根据 replica 信息更新 table 统计信息
  - replica.setDataSize(stat.getDataSize());
  - replica.setRowsetCount(stat.getNumRowsets());
  - replica.setSegmentCount(stat.getNumSegments());
  - replica.setRowCount(stat.getNumRows());
  - replica.setLocalInvertedIndexSize(stat.getIndexSize());
  - replica.setLocalSegmentSize(stat.getSegmentSize());
- 目前存在的问题
  - 一些功能依赖正确的统计信息，获取很慢
  - 有些 tablet 的统计信息没有变更过，重复获取，对ms压力较大

## 优化思路
- 基本的思路是获取变更的统计信息。目前导入，compaction，schema change，restore会更新统计信息。
  - 导入，schema change ，restore完成后，master fe 记录哪些partition数据变更 (Partition粒度就可以，不需要 tablet粒度)
  - compaction 由 be 提交，需要 be 通知 master fe
    ```
    be -> commit compaction job -> ms
    ms -> commit job success -> be

    新增的 rpc：
    be -> send_stats_to_fe_async(复用commit_txn给fe的通知机制，里面增加tablet id) -> fe

    fe -> get tablet stats -> ms
    ms -> return tablet stats -> fe

    新增的 rpc：
    fe -> tablet stats -> follower fes
    ```

- follower fe 怎么更新统计信息
  - master fe 把拉取的统计信息同步给其它 fe，各个 fe 更新数据
    - 每个 tablet 有 6 个数值
- 统计信息的校正机制
    - 每个 tablet 记录 lastGetTabletStatsTime 和 inverval，下一次获取的时间间隔 [1 min，5 min，10 min，30 min，2 h，6 h，12 h，3 d，无穷]
    - 主动更新：
      - 获取 tablet stats，设置 lastGetTabletStatsTime 为当前时间，inverval = 1min
    - 校正线程：
      - lastGetTabletStatsTime + inverval > now()，更新 lastGetTabletStatsTime 为当前时间，比较统计信息是否变更：
        1. 没有变更，inverval 选下一个
        2. 有变更，inverval = 1min
      - 这种策略，保证一个tablet在8次无效获取后，不再获取
- 收益
  - 对于写入频繁的Partition，stats的获取更及时
  - 对于写入不频繁的Partition，stats变化不大，可以减少较多没有必要的获取
 - 可能存在的问题
   - 对master fe依赖过重
   - master fe和follower fe统计信息不一致的问题
   - 统计信息不准确的问题  
     - 导入，compaction，schema change，restore等操作没有正确通知 fe
     - fe 之间同步统计信息失败

## 长期思路

# FE rpc限流

# FE 统计信息和version周期持久化