package www.itcast.tld.out_put_format;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import www.itcast.tld.bean.FlowBean;

import java.io.IOException;

/**
 * @createUser: 张鹏
 * @createTime: 2020/12/4
 * @descripton:
 **/
public class FlowBeanMapper extends Mapper<LongWritable, Text,Text, FlowBean> {
    protected void map(LongWritable begin, Text text, Context context)  {

        String[] split = text.toString().split("\t");
        Text key =  new Text(split[1]);
        Long value1 =  Long.parseLong(split[split.length -3]);
        Long value2 =  Long.parseLong(split[split.length -2]);
        try {
            context.write(key,new FlowBean(value1,value2));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
