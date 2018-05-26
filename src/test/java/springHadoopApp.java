import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;


/**
 * 通过spring对HDFS进行访问
 * Created by dell on 2018/5/26.
 */
public class springHadoopApp {

    private ApplicationContext ctx;
    private FileSystem fileSystem;

    @Before
    public void setup() {
        ctx = new ClassPathXmlApplicationContext("beans.xml");
        fileSystem = (FileSystem) ctx.getBean("fileSystem");
    }

    @After
    public void tearDown() {
        ctx = null;
        fileSystem = null;
    }

    //创建文件夹
    @Test
    public void testMkdir() throws Exception{
        fileSystem.mkdirs(new Path("/springhdfs/"));
    }

    @Test
    public void cat() throws Exception{
        FSDataInputStream in =fileSystem.open(new Path("/springhdfs/hello.txt"));
        IOUtils.copyBytes(in,System.out,1024);
        in.close();
    }
}
