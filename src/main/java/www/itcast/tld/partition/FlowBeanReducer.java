package www.itcast.tld.partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @createUser: 张鹏
 * @createTime: 2020/12/4
 * @descripton:
 **/
public class FlowBeanReducer extends Reducer<FlowBean, NullWritable, FlowBean, NullWritable> {

    protected void reduce(FlowBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable value : values){
            context.write(key,NullWritable.get());
        }
    }
}
