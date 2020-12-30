package www.itcast.tld.partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @createUser: 张鹏
 * @createTime: 2020/12/21
 * @descripton:  HashPartitioner 重写指定手动partition
 **/
public class FlowBeanPartition extends Partitioner<FlowBean, NullWritable> {
    @Override
    public int getPartition(FlowBean flowBean, NullWritable nullWritable, int i) {
        String mobile = flowBean.getMobile().substring(0,2);
        int partition;
        switch(mobile){
            case "13" :
                partition = 1;
                break;
            case "15" :
                partition = 2;
                break;
            case "18" :
                partition = 3;
                break;
            default :
                partition = 0;
                break;
        }
        return partition;
    }

}
