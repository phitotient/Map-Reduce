package WcPartitioner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMap extends Mapper<LongWritable, Text, Text, IntWritable>{
	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			String lineParts[] = line.split(" ");
			for(String SingleWord : lineParts)
			{
				Text OutKey = new Text(SingleWord);
				IntWritable One = new IntWritable(1);
				context.write(OutKey, One);				
			}
		}
	}
