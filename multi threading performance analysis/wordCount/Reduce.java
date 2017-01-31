package com.mapreduce.wordcount;

/**
 * Created by amogh-hadoop on 1/27/17.
 */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, IntWritable, Text, LongWritable> {

    @Override
    public void reduce(Text key,
                       Iterable<IntWritable> values,
                       Context context)
            throws IOException, InterruptedException {
        long count = 0;
        for (IntWritable value : values) {
            count++;
        }

        context.write(key, new LongWritable(count));
    }
}

