import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by dell on 2018/5/21.
 */
public class MapReduceApp {
    //Map--将文件中的词进行拆分
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String[] words=line.split(" ");
            for(String word:words){
                context.write(new Text(word),new LongWritable(1));
            }
        }
    }

    //Reducer--拆分后统计
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for(LongWritable value:values){
                sum+=value.get();
            }
            context.write(key,new LongWritable(sum));
        }
    }

    //Partitioner
    //按照内容划分不同的reducer进行处理
    public static class MyPartitioner extends Partitioner<Text,LongWritable>{
        @Override
        public int getPartition(Text text, LongWritable longWritable, int i) {
            return 0;
        }
    }


    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        Job job= Job.getInstance(configuration,"wordCount");
        job.setJarByClass(MapReduceApp.class);
        //FileInputFormat注意路径
        FileInputFormat.addInputPath(job, new Path(args[0]));//设置输入路径

        //对output路径文件进行判断
        Path output =  new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if(fileSystem.exists(output)){
            fileSystem.delete(output,true);

        }

        //设置map参数
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置reduce参数
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置combinerApp
        //job.setCombinerClass(MyReducer.class);
        //设置Partioner
        //job.setPartitionerClass(MyPartitioner.class);
        //job.setNumReduceTasks(4);

        //设置作业输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true)?0:1);

    }
}
