package top.jiangnanmax.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author jiangnanmax
 * @email jiangnanmax@gmail.com
 * @description WordCountMapper
 * @date 2021/4/20
 **/

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
        Text text = new Text();
        IntWritable intWritable = new IntWritable(1);
        for (String word : words) {
            text.set(word);
            context.write(new Text(word), intWritable);
        }
    }
}
