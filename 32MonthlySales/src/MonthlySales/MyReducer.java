package MonthlySales;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<IntWritable,DoubleWritable,NullWritable,Text> {
	
	public void reduce(IntWritable key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
		
		int month = key.get();
		int sum = 0;
		int count = 0;
		Text Result = new Text();
				
		for (DoubleWritable value : values) {			
			sum += value.get();
			count++;
		}
		
		switch (month) {
		case 1:
			Result.set("Total no. of Transaction in January is "+count+" with a total sales of INR."+sum);			
			break;
		case 2:
			Result.set("Total no. of Transaction in February is "+count+" with a total sales of INR."+sum);			
			break;
		case 3:
			Result.set("Total no. of Transaction in March is "+count+" with a total sales of INR."+sum);			
			break;
		case 4:
			Result.set("Total no. of Transaction in April is "+count+" with a total sales of INR."+sum);			
			break;
		case 5:
			Result.set("Total no. of Transaction in May is "+count+" with a total sales of INR."+sum);			
			break;
		case 6:
			Result.set("Total no. of Transaction in June is "+count+" with a total sales of INR."+sum);			
			break;
		case 7:
			Result.set("Total no. of Transaction in July is "+count+" with a total sales of INR."+sum);			
			break;
		case 8:
			Result.set("Total no. of Transaction in August is "+count+" with a total sales of INR."+sum);			
			break;
		case 9:
			Result.set("Total no. of Transaction in September is "+count+" with a total sales of INR."+sum);			
			break;
		case 10:
			Result.set("Total no. of Transaction in October is "+count+" with a total sales of INR."+sum);			
			break;
		case 11:
			Result.set("Total no. of Transaction in November is "+count+" with a total sales of INR."+sum);			
			break;
		case 12:
			Result.set("Total no. of Transaction in December is "+count+" with a total sales of INR."+sum);			
			break;
		default:
			break;
		}
		context.write(null, Result);
	}
}
