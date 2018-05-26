import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsShell;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by dell on 2018/5/26.
 */
@SpringBootApplication
public class springBootHDFSApp implements CommandLineRunner{
    @Autowired
    FsShell fsShell;

    public void run(String...strings) throws  Exception{
        for(FileStatus fileStatus:fsShell.lsr("/springhdfs")){
            System.out.println(fileStatus);
        }
    }

    public static void main(String[] args){
        SpringApplication.run(springBootHDFSApp.class,args);
    }

}
