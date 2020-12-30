package www.itcast.tld.out_put_format;

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

    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        for (FlowBean value : values){
            context.write(key,value);
        }

    }
}
