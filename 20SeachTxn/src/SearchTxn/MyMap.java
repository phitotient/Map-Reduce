package SearchTxn;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMap extends Mapper<LongWritable, Text, NullWritable, Text>{
	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String Txn = value.toString();
			String TxnParts[] = Txn.split(",");
			Double Amt = Double.parseDouble(TxnParts[3]);
			String Uid = TxnParts[2];			
			if(Uid.equals("4000010") && Amt>100)
			{
				context.write(null, value);	
			}			
		}
	}
