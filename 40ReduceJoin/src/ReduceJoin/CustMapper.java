package ReduceJoin;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CustMapper extends Mapper<LongWritable, Text, Text, Text>{
	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			//4000001,Kristina,Chung,55,Pilot
			String Record = value.toString();
			String RecordParts[] = Record.split(",");
			String Uid = "Uid:"+RecordParts[0];
			String Name = "Name:"+RecordParts[1];
			Text OutPutKey = new Text(Uid);
			Text OutPutValue = new Text(Name);
			context.write(OutPutKey,OutPutValue);
		}
	}
