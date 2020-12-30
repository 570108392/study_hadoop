package www.itcast.tld.cache;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import javax.print.DocFlavor;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;


/**
 * @createUser: 张鹏
 * @createTime: 2020/12/24
 * @descripton:
 **/
public class OrderBeanMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {

    Map<String, String> pdMap = new HashMap<>();
    @Override
    protected void setup(Context context) throws IOException {
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath();

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path),"UTF-8"));
        String line;
        while(StringUtils.isNotEmpty(line = reader.readLine())){

            // 2 切割
            String[] fields = line.split("\t");

            // 3 缓存数据到集合
            pdMap.put(fields[0], fields[1]);
        }

        // 4 关流
        reader.close();

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split("\t");

        OrderBean build = OrderBean.builder()
                .id(fields[0])
                .pid(fields[1])
                .pname(pdMap.get(fields[1]))
                .amount(Integer.parseInt(fields[2])).build();

        context.write(build, NullWritable.get());

    }
}
