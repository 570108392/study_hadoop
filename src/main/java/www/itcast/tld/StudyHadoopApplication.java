package www.itcast.tld;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.net.URI;

@SpringBootApplication
public class StudyHadoopApplication {

    public static void main(String[] args) throws Exception {
        // 1获取文件系统
        Configuration configuration = new Configuration();
//        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),configuration);

        // 2 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while(listFiles.hasNext()){
            LocatedFileStatus status = listFiles.next();

            // 输出详情
            // 文件名称
            System.out.println(status.getPath().getName());
            // 长度
            System.out.println(status.getLen());
            // 权限
            System.out.println(status.getPermission());
            // 分组
            System.out.println(status.getGroup());

            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations) {

                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();

                for (String host : hosts) {
                    System.out.println(host);
                }
            }

            System.out.println("-----------班长的分割线----------");
        }

        // 3 关闭资源
        fs.close();
    }

}
