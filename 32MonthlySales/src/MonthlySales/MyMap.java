package MonthlySales;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMap extends Mapper<LongWritable, Text, IntWritable, DoubleWritable>{
	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String Txn = value.toString();
			String TxnParts[] = Txn.split(",");
			String Date = TxnParts[1];
			String DateParts[] = Date.split("-");
			
			int Month = Integer.parseInt(DateParts[0]);
			Double Amt = Double.parseDouble(TxnParts[3]);
			context.write(new IntWritable(Month), new DoubleWritable(Amt));
		}
	}
