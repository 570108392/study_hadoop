package www.itcast.tld.map_reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.stream.Stream;

/**
 * @createUser: 张鹏
 * @createTime: 2020/12/4
 * @descripton:
 **/
public class WordCountMapper extends Mapper<Text, IntWritable,Text, IntWritable> {
    IntWritable intWritable = new IntWritable(1);
    Text tes = new Text();
    protected void map(Text key, IntWritable value, Mapper<Text, IntWritable,Text, IntWritable>.Context context)  {

        Stream.of(key.toString().split(" ")).forEach(str -> {
            try {
                context.write(new Text(str),intWritable);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}
