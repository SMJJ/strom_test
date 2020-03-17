package com.kafka;

import com.google.common.collect.Maps;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Map;
import java.util.UUID;

public class KafkaTopology {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        // kafka使用的zk地址
        BrokerHosts hosts = new ZkHosts("192.168.199.102:2181");

        // kafka存储数据的topic名称
        String topic = "project_topic";

        // 指定zk中的一个根目录，存储的是KafkaSpout读取数据的位置信息(offset)
        String zkRoot = "/" + topic;

        String id = UUID.randomUUID().toString();
        SpoutConfig spoutConfig = new SpoutConfig(hosts,topic,zkRoot,id);
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        String SPOUT_ID = KafkaSpout.class.getSimpleName();
        builder.setSpout(SPOUT_ID,kafkaSpout);

        String BOLT_ID = LogProcessBolt.class.getSimpleName();
        builder.setBolt(BOLT_ID,new LogProcessBolt()).shuffleGrouping(SPOUT_ID);

        //JDBC配置参数
        Map hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://localhost/storm");
        hikariConfigMap.put("dataSource.user","root");
        hikariConfigMap.put("dataSource.password","root");
        ConnectionProvider connectionProvider;
        connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);
        //表名
        String tableName = "stat_tmp";
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);

        JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                .withInsertQuery("insert into stat_tmp values(?,?,?)")
                .withQueryTimeoutSecs(30);

        builder.setBolt("JdbcInsertBolt",userPersistanceBolt).shuffleGrouping(BOLT_ID);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology(KafkaTopology.class.getSimpleName(),new Config(),builder.createTopology());

//        try {
//            StormSubmitter.submitTopology(KafkaTopology.class.getSimpleName(),
//                    new Config(),builder.createTopology());
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }
}
