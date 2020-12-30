package www.itcast.tld.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.regex.Pattern;


/**
 * @createUser: 张鹏
 * @createTime: 2020/12/24
 * @descripton:
 **/
public class OrderBeanMapper extends Mapper<LongWritable, Text,Text, OrderBean> {

    private String fileName;
    @Override
    protected void setup(Context context) {
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        fileName = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        OrderBean orderBean = fileName.equals("order.txt")
                ? OrderBean.builder().id(split[0]).pid(split[1]).amount(Integer.parseInt(split[2])).pname("").build()
                : OrderBean.builder().id("").pid(split[0]).amount(0).pname(split[1]).build();
        context.write(new Text(orderBean.getPid()),orderBean);
    }

    public static boolean isInteger(String str) {
        Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
        return pattern.matcher(str).matches();
    }
}
