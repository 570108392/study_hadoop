package www.itcast.tld.cache;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @createUser: 张鹏
 * @createTime: 2020/12/24
 * @descripton:
 **/
@Data
@Accessors(chain = true)
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderBean implements WritableComparable<OrderBean> {

    private String pid;
    private String id;
    private int amount;
    private String pname;
    @Override
    public int compareTo(OrderBean o) {
        return o.getAmount() - this.amount;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(pid);
        dataOutput.writeUTF(id);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pname);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        pid = dataInput.readUTF();
        id = dataInput.readUTF();
        amount = dataInput.readInt();
        pname = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return id + "   " + pname +"    " + amount;
    }
}
