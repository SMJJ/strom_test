package com.liaomj;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

public class ClusterSumStormTaskTopology {

    /**
     * Spout需要继承BaseRichSpout
     * 数据源需要产生数据并发射
     */
    public static class DataSourceSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;

        /**
         * 初始化方法，只会调用一次
         *
         * @param conf      配置参数
         * @param context   上下文
         * @param collector 数据发射器
         */
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        int number = 0;

        /**
         * 产生数据，在生产上肯定是小事队列
         * 这个方法是一个死循环
         */
        @Override
        public void nextTuple() {
            this.collector.emit(new Values(++number));
            System.out.println("Spout:" + number);
            Utils.sleep(1000);
        }

        /**
         * 声明输出字段
         *
         * @param declarer
         */
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("num"));
        }
    }

    /**
     * 数据的累计求和Blot：接收数据并处理
     */
    public static class SumBolt extends BaseRichBolt {

        /**
         * 初始化方法，只会调用一次
         *
         * @param stormConf
         * @param context
         * @param collector
         */
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        }

        int sum = 0;

        /**
         * 死循环，获取Spout发送过来的数据
         *
         * @param input
         */
        @Override
        public void execute(Tuple input) {
            Integer value = input.getIntegerByField("num");
            sum += value;
            System.out.println("Blot:sum = [" + sum + "]");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static void main(String[] args) {
            // TopologyBuilder根据Spout和Bolt捞构建出Topology
            // Storm中任何一个作业都是通过Topology的方式进行提交的
            // Topology中需要制定Spout和Bolt的执行顺序
            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("DataSourceSpout",new DataSourceSpout(),2);
            builder.setBolt("SumBolt",new SumBolt(),2).setNumTasks(4).shuffleGrouping("DataSourceSpout");

            // 创建集群模式运行
            String topName = ClusterSumStormTaskTopology.class.getSimpleName();
        try {
            Config config = new Config();
            config.setNumWorkers(2);
            config.setNumAckers(0);
            StormSubmitter.submitTopology(topName,config,builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
