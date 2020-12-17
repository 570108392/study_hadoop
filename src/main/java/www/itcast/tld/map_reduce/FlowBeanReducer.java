package www.itcast.tld.map_reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import www.itcast.tld.bean.FlowBean;

import java.io.IOException;

/**
 * @createUser: 张鹏
 * @createTime: 2020/12/4
 * @descripton:
 **/
public class FlowBeanReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private  FlowBean v = new FlowBean();
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (FlowBean value : values){
            sum += value.getSumFlow();
        }
        context.write(key,v.setSumFlow(sum));
    }
}
