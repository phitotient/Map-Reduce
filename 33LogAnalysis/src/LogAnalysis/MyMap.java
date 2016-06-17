package LogAnalysis;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMap extends Mapper<LongWritable, Text, Text, Text>{
	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String Record = value.toString();
			String RecordParts[] = Record.split(" ");
			String RecordType = RecordParts[3].substring(1, RecordParts[3].length()-1);
			Text OutPutKey = new Text(RecordType);
			Text OutPutValue = value;
			context.write(OutPutKey,OutPutValue);
		}
	}
