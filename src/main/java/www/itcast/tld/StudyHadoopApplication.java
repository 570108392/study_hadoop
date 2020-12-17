package www.itcast.tld;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.*;
import java.net.URI;
import java.net.URL;

@SpringBootApplication
public class StudyHadoopApplication {

    public static void main(String[] args) throws Exception {
//        fileUpload();
//        fileDownload();
//        mvDir();
//        fileUploadByIo();
//        fileDownloadByIo();
//        fileDownloadByIoAndLength();
        fileDownloadByIoAndBegin();
    }


    /**
     * 初始化文件系统
     * @return
     * @throws Exception
     */
    private static FileSystem init()throws Exception{
        Configuration cfg = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),cfg,"root");
        return fs;
    }

    /**
     * 关闭
     * @param outputStream
     * @param inputStream
     * @param fs
     * @throws Exception
     */
    private static void destory(OutputStream outputStream,InputStream inputStream,FileSystem fs)throws Exception{
        IOUtils.closeStream(outputStream);
        IOUtils.closeStream(inputStream);
        fsClose(fs);
    }

    /**
     * 关闭文件流
     * @param fs
     * @throws Exception
     */
    private static void fsClose(FileSystem fs)throws Exception{
        fs.close();
    }

    /**
     * dfs 文件上传测试
     */
    private static void fileUpload() throws Exception{
        FileSystem fs = init();
        fs.copyFromLocalFile(true,new Path("E:\\hadoop-2.9.2\\LICENSE.txt"),new Path("/second.txt"));
        fsClose(fs);
    }

    /**
     * dfs 下载文件测试
     */
    private static void fileDownload() throws Exception {
        FileSystem fs = init();
        fs.copyToLocalFile(true,new Path("/second.txt"),new Path("E:\\hadoop-2.9.2\\test\\second.txt"));
        fsClose(fs);
    }
    /**
     * dfs 删除文件夹
     */
    private static void delDir() throws Exception {
        FileSystem fs = init();
        fs.delete(new Path("/second.txt"),true);
        fsClose(fs);
    }
    /**
     * dfs 修改文件夹名称
     */
    private static void mvDir() throws Exception {
        FileSystem fs = init();
        fs.rename(new Path("/rmsecond.txt"),new Path("/second.txt"));
        fsClose(fs);
    }

    /**
     * dfs 文件流式上传
     */
    private static void fileUploadByIo() throws Exception {
        FileSystem fs = init();
        FSDataOutputStream outputStream = fs.create(new Path("/jdk-8u191-windows-x64.exe"), true);
        InputStream inputStream = new FileInputStream("E:\\hadoop-2.9.2\\test\\jdk-8u191-windows-x64.exe");
        IOUtils.copyBytes(inputStream,outputStream,fs.getConf());
        destory(outputStream,inputStream,fs);
    }
    /**
     * dfs 文件流式下载
     */
    private static void fileDownloadByIo() throws Exception {
        FileSystem fs = init();
        FSDataInputStream inputStream = fs.open(new Path("/jdk-8u191-windows-x64.exe"));
        OutputStream outputStream = new FileOutputStream("E:\\hadoop-2.9.2\\test\\jdk-8u191-windows.exe");
        IOUtils.copyBytes(inputStream,outputStream,fs.getConf());
        destory(outputStream,inputStream,fs);
    }
    /**
     * dfs 文件流式下载下载钱N文件
     */
    private static void fileDownloadByIoAndLength() throws Exception {
        FileSystem fs = init();
        FSDataInputStream inputStream = fs.open(new Path("/jdk-8u191-windows-x64.exe"));
        OutputStream outputStream = new FileOutputStream("E:\\hadoop-2.9.2\\test\\jdk-191.exe");

        byte[] buf = new byte[1024];
        for (int i = 0; i < 1024 * 128; i++){
            inputStream.read(buf);
            outputStream.write(buf);
        }
        destory(outputStream,inputStream,fs);
    }

    /**
     * dfs 文件流式从指定角标位置开始下载
     */
    private static void fileDownloadByIoAndBegin() throws Exception {
        FileSystem fs = init();
        FSDataInputStream inputStream = fs.open(new Path("/jdk-8u191-windows-x64.exe"));
        //从指定角标位置开始下载
        inputStream.seek(1024 * 1024 * 128);
        OutputStream outputStream = new FileOutputStream("E:\\hadoop-2.9.2\\test\\jdk-end.exe");
        IOUtils.copyBytes(inputStream,outputStream,fs.getConf());
        destory(outputStream,inputStream,fs);
    }
}
