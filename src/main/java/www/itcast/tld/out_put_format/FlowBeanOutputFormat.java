package www.itcast.tld.out_put_format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @createUser: 张鹏
 * @createTime: 2020/12/23
 * @descripton:
 **/
public class FlowBeanOutputFormat<Text, FlowBean> extends FileOutputFormat<Text, FlowBean> {

    public FlowBeanOutputFormat() {
    }

    @Override
    public RecordWriter<Text, FlowBean> getRecordWriter(TaskAttemptContext job) {
        return new FlowBeanOutputFormat.FlowBeanRecordWriter(job) ;
    }


    protected static class FlowBeanRecordWriter<Text, FlowBean> extends RecordWriter<Text, FlowBean> {
        public  static String SEPERATOR = "mapreduce.output.textoutputformat.separator";
        private static final byte[] NEWLINE;
        FSDataOutputStream out = null;
        FSDataOutputStream out1 = null;
        FSDataOutputStream out2 = null;
        FSDataOutputStream out3 = null;
        private  byte[] keyValueSeparator;
        public FlowBeanRecordWriter(TaskAttemptContext job) {

            // 1 获取文件系统
            FileSystem fs;
            try {
                fs = FileSystem.get(job.getConfiguration());
                keyValueSeparator = job.getConfiguration().get(SEPERATOR, "\t").getBytes(StandardCharsets.UTF_8);
                // 3 创建输出流
                out = fs.create(new Path("E:\\hadoop-2.9.2\\test\\phone_data_0.txt"));
                out1 = fs.create(new Path("E:\\hadoop-2.9.2\\test\\phone_data_1.txt"));
                out2 = fs.create(new Path("E:\\hadoop-2.9.2\\test\\phone_data_2.txt"));
                out3 = fs.create(new Path("E:\\hadoop-2.9.2\\test\\phone_data_3.txt"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void write(Text key, FlowBean value) throws IOException {

            if (key.toString().startsWith("13")) {
                write(key,value,out);
            } else if (key.toString().startsWith("15")){
                write(key,value,out1);
            }else if (key.toString().startsWith("18")){
                write(key,value,out2);
            }else {
                write(key,value,out3);
            }
        }

        private void writeObject(Object o,FSDataOutputStream out) throws IOException {
            if (o instanceof org.apache.hadoop.io.Text) {
                org.apache.hadoop.io.Text to = (org.apache.hadoop.io.Text)o;
                out.write(to.getBytes(), 0, to.getLength());
            } else {
                out.write(o.toString().getBytes(StandardCharsets.UTF_8));
            }

        }

        public synchronized void write(Text key, FlowBean value,FSDataOutputStream out) throws IOException {
            boolean nullKey = key == null || key instanceof NullWritable;
            boolean nullValue = value == null || value instanceof NullWritable;
            if (!nullKey || !nullValue) {
                if (!nullKey) {
                    this.writeObject(key,out);
                }

                if (!nullKey && !nullValue) {
                    out.write(keyValueSeparator);
                }

                if (!nullValue) {
                    this.writeObject(value,out);
                }

                out.write(NEWLINE);
            }
        }

        @Override
        public void close(TaskAttemptContext context){
            IOUtils.closeStream(out);
            IOUtils.closeStream(out1);
            IOUtils.closeStream(out2);
            IOUtils.closeStream(out3);
        }
        static {
            NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);
        }
    }

}
