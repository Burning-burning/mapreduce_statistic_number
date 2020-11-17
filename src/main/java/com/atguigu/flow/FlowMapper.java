package com.atguigu.flow;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable,Text,Text, FlowBean> {
    private Text phoneNumber = new Text();
    private FlowBean flowBean = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");

        String phone  = fields[1];
        this.phoneNumber.set(phone);
        long upFlow  = Long.parseLong(fields[fields.length-3]);
        long downFlow = Long.parseLong(fields[fields.length-2]);
        this.flowBean.set(upFlow,downFlow);
        context.write(phoneNumber,flowBean);
    }
}
