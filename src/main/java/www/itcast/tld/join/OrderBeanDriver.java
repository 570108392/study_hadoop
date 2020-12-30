package www.itcast.tld.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import www.itcast.tld.bean.FlowBean;
import www.itcast.tld.out_put_format.FlowBeanMapper;
import www.itcast.tld.out_put_format.FlowBeanOutputFormat;
import www.itcast.tld.out_put_format.FlowBeanReducer;

import java.io.IOException;

/**
 * @createUser: 张鹏
 * @createTime: 2020/12/4
 * @descripton:
 **/
public class OrderBeanDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 设置jar加载路径
        job.setJarByClass(OrderBeanDriver.class);

        // 3 设置map和reduce类
        job.setMapperClass(OrderBeanMapper.class);
        job.setReducerClass(OrderBeanReduce.class);

        // 4 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderBean.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("E:\\hadoop-2.9.2\\test\\order.txt"),new Path("E:\\hadoop-2.9.2\\test\\pd.txt"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\hadoop-2.9.2\\test\\order_pd"));

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }

}
