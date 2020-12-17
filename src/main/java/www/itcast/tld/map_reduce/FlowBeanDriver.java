package www.itcast.tld.map_reduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import www.itcast.tld.bean.FlowBean;

import java.io.IOException;

/**
 * @createUser: 张鹏
 * @createTime: 2020/12/4
 * @descripton:
 **/
public class FlowBeanDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

//        args[0] = "E:\\hadoop-2.9.2\\test\\wordcount.txt";
//        args[1] = "E:\\hadoop-2.9.2\\test\\count.txt";
        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 设置jar加载路径
        job.setJarByClass(FlowBeanDriver.class);

        // 3 设置map和reduce类
        job.setMapperClass(FlowBeanMapper.class);
        job.setReducerClass(FlowBeanReducer.class);

        // 4 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("E:\\hadoop-2.9.2\\test\\phone_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\hadoop-2.9.2\\test\\phone_data"));

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }

}
