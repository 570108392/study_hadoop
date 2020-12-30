package www.itcast.tld.partition;


import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @createUser: 张鹏
 * @createTime: 2020/12/17
 * @descripton:
 *  实现自定义序列化 有实现mapTask 进行排序
 * interface WritableComparable<T> extends Writable, Comparable<T>
 **/
@Data
@Accessors(chain = true)
public class FlowBean implements WritableComparable<FlowBean> {

    private long upFlow;
    private long downFlow;
    private long sumFlow;
    private String mobile;


    public FlowBean(String mobile,long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.mobile = mobile;
        this.sumFlow = upFlow + downFlow;
    }
    public FlowBean() {
        super();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(mobile);
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        mobile = dataInput.readUTF();
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();

    }

    @Override
    public String toString() {
        return mobile +"      "+ upFlow  +"      "+ downFlow +"      "+ sumFlow+"      "  ;
    }

    @Override
    public int compareTo(FlowBean o) {
        if (this.sumFlow - o.getSumFlow() > 0)
            return -1;
        else if (this.sumFlow -o.getSumFlow() < 0)
            return 1;
        else
            return  0;
    }
}
