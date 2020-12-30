package www.itcast.tld.join;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @createUser: 张鹏
 * @createTime: 2020/12/24
 * @descripton:
 **/
public class OrderBeanReduce extends Reducer<Text,OrderBean,OrderBean ,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<OrderBean> values, Context context){

        String pname = null;
        ArrayList<OrderBean> orderBeans = new ArrayList<>();
        for (OrderBean orderBean : values){
            if (!StringUtils.isEmpty(orderBean.getPname()))
                pname = orderBean.getPname();
            else {
                OrderBean build = OrderBean.builder().id(orderBean.getId()).pid(orderBean.getPid()).amount(orderBean.getAmount()).build();
                orderBeans.add(build);
            }
        }

        String finalPname = pname;
        orderBeans.forEach(orderBean -> {
            try {
                orderBean = orderBean.setPname(finalPname);
                context.write(orderBean,NullWritable.get());
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

    }
}
