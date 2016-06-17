package SearchTxnbyArg;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMap extends Mapper<LongWritable, Text, NullWritable, Text>{
	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String Txn = value.toString();
			String TxnParts[] = Txn.split(",");
			String Uid = TxnParts[2];
			String Uid2Search = context.getConfiguration().get("Uid2Search");
			if(Uid.equals(Uid2Search))
			{
				context.write(null, value);	
			}			
		}
	}
