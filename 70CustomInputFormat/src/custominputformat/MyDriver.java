package custominputformat;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class MyDriver {

  public static void main(String[] args) 
                  throws IOException, ClassNotFoundException, InterruptedException {
        
    Job job = new Job();
    job.setJarByClass(MyDriver.class);
    job.setJobName("CustomTest");
    job.setNumReduceTasks(0);
    job.setMapperClass(MyMapper.class);
    job.setInputFormatClass(Server1Inputformat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class); 
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.waitForCompletion(true);
  }
}