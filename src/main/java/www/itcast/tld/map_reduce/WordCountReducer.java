package www.itcast.tld.map_reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @createUser: 张鹏
 * @createTime: 2020/12/4
 * @descripton:
 **/
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private  IntWritable v = new IntWritable();
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values){
            sum += value.get();
        }
        v.set(sum);
        context.write(key,v);
    }
}
