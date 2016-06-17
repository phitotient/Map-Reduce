package ReduceJoin;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyJoinReducer extends Reducer<Text, Text, Text, NullWritable> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		String CustName = "";
		double CustTotal = 0.0;
		int TxnCount = 0;		
		for (Text SingleVal : values) {
			String ValueParts[] = SingleVal.toString().split(":");
			if (ValueParts[0].equals("Amt")) {
				TxnCount++;
				CustTotal += Double.parseDouble(ValueParts[1]);
			} 
			else if (ValueParts[0].equals("Name")) {
				CustName = ValueParts[1];
			}
		}
		String OutputString = CustName+" did a total of "+TxnCount+" transaction with total spendings of INR."+CustTotal;
		context.write(new Text(OutputString), null);
	}
}
