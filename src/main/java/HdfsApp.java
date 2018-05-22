import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

/**
 * HDFS基础操作
 * Created by dell on 2018/5/21.
 */
public class HdfsApp {
    public static final String HDFS_PATH = "hdfs://39.105.95.154:8020";
    Configuration configuration = null;
    FileSystem fileSystem = null;

    @Before
    public void setup() throws Exception {
        configuration = new Configuration();
        configuration.set("dfs.client.use.datanode.hostname", "true");//让可以使用主机名传参数
        configuration.set("fs.defaultFS", "hdfs://iz2zef94dnmkl8kf3l63r9z:8020");//主机名访问
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "root");
    }

    @After
    public void tearDown() throws Exception {
        configuration = null;
        fileSystem = null;
    }

    //创建文件夹--( hadoop fs -ls / ) 命令查看
    @Test
    public void mkdirDir() throws Exception {
        fileSystem.mkdirs(new Path("/hdfsapi/test "));
    }

    //创建文件
    @Test
    public void create() throws Exception {
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        output.write("hello hadoop".getBytes());
        output.flush();
        output.close();
    }

    //查看文件内容
    @Test
    public void cat() throws Exception {
        FSDataInputStream in = new FSDataInputStream(new FileInputStream(new File("/hdfsapi/test/a.txt")));
        IOUtils.copyBytes(in, System.out, 1024);
        in.close();
    }

    //文件重命名
    @Test
    public void rename() throws Exception {
        fileSystem.rename(new Path("/hdfsapi/test/a.txt"), new Path("/hdfsapi/test/b.txt"));
    }

    //上传文件到HDFS
    @Test
    public void copyFromLocalFileWithProgress() throws Exception {
        InputStream in = new BufferedInputStream(new FileInputStream(new File("E:\\hadoop-2.6.0-cdh5.7.0.tar.gz")));
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/hadoop.tar.gz"),
                new Progressable() {
                    public void progress() {
                        System.out.println("->");
                    }
                }
        );
        IOUtils.copyBytes(in, output, 4096);
    }

    //下载hdfs文件
    @Test
    public void copyToLocalFile() throws Exception {
        Path localPath = new Path("E:\\h.txt");
        Path hdfsPath = new Path("/hdfsapi/test/hello.txt");
        fileSystem.copyToLocalFile(hdfsPath, localPath);
    }
    //展示文件
    @Test
    public void list() throws Exception {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/hdfsapi/test/"));
        for (FileStatus file : fileStatuses) {
            String isDir = file.isDirectory() ? "文件夹" : "文件";
            short replication = file.getReplication();
            long len = file.getLen();
            String path = file.getPath().toString();

            System.out.println(isDir + "\t" + replication + "\t" + len + "\t" + path);
        }
    }
    //删除文件
    @Test
    public void delete() throws Exception {
        fileSystem.delete(new Path("/"), true);
    }

}
