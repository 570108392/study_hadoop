package www.itcast.tld.bean;


import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @createUser: 张鹏
 * @createTime: 2020/12/17
 * @descripton:
 **/
@Data
@Accessors(chain = true)
public class FlowBean implements Writable {

    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public FlowBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }
    public FlowBean() {
        super();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();

    }

    @Override
    public String toString() {
        return "FlowBean{" +
                "upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", sumFlow=" + sumFlow +
                '}';
    }
}
