package com.kafka;

import clojure.lang.IFn;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class LogProcessBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            // 固定写法，获得一个字节数组
            byte[] binaryByField = input.getBinaryByField("bytes");
            String value = new String(binaryByField);

            String[] splits = value.split("\t");
            String phone = splits[0];
            String[] temp = splits[1].split(",");
            String longitude = temp[0];
            String latitude = temp[1];
            long time = Long.parseLong(DataUtils.getTime(splits[2]));
            System.out.println(phone+","+longitude+","+latitude+","+time);

            collector.emit(new Values(time, Double.parseDouble(latitude), Double.parseDouble(longitude)));

            this.collector.ack(input);
        }catch (Exception e){
            this.collector.fail(input);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time","latitude","longitude"));
    }
}
