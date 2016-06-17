package MonthlyReport;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMap extends Mapper<LongWritable, Text, Text, Text>{
	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String Txn = value.toString();
			String TxnParts[] = Txn.split(",");
			String Date = TxnParts[1];
			String DateParts[] = Date.split("-");
			String Month = DateParts[0];
			context.write(new Text(Month), value);
		}
	}
