package www.itcast.tld.map_reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.stream.Stream;

/**
 * @createUser: 张鹏
 * @createTime: 2020/12/4
 * @descripton:
 **/
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    IntWritable intWritable = new IntWritable(1);
    Text key = new Text();
    protected void map(LongWritable begin, Text text, Context context)  {

        Stream.of(text.toString().split(" ")).forEach(str -> {
            try {
                key.set(str);
                context.write(key,intWritable);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}
