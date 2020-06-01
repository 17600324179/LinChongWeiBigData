1.com.spark_streaming.checkpoint.UvCheckpoint
    实时从kafka的topic中读取数据，并统计数据的次数，将数据checkpoint到hdfs上
2.com.spark_streaming.offset.UVCheckPointOffsetToMysql
    实时从kafka的topic中读取数据，并统计数据的次数，将数据checkpoint到hdfs上,手动维护offset到mysql中

实际上，当手动维护offset的时候就不需要再进行checkpoint操作了。因为为了保证数据的exactly once准确一次性
    ，写入mysql的时候会带着 topic、partition、groupId、offset、result结果 等字段