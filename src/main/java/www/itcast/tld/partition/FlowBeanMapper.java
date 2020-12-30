package www.itcast.tld.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @createUser: 张鹏
 * @createTime: 2020/12/4
 * @descripton:
 **/
public class FlowBeanMapper extends Mapper<LongWritable, Text,FlowBean, NullWritable> {
    protected void map(LongWritable begin, Text text, Context context)  {

        String[] split = text.toString().split("\t");
        Long value1 =  Long.parseLong(split[split.length -3]);
        Long value2 =  Long.parseLong(split[split.length -2]);
        try {
            context.write(new FlowBean(split[1],value1,value2),NullWritable.get());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
