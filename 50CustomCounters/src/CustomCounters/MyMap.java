package CustomCounters;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMap extends
		Mapper<LongWritable, Text, NullWritable, NullWritable> {
	
	public static enum MyDebug {
		Loop1, If1, If2, If3
	};

	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		String line = value.toString();
		String lineParts[] = line.split(" ");
		for (String SingleWord : lineParts) {
			context.getCounter(MyDebug.Loop1).increment(1);

			if (SingleWord.startsWith("a")) {
				context.getCounter(MyDebug.If1).increment(1);
			}
			if (SingleWord.startsWith("b")) {
				context.getCounter(MyDebug.If2).increment(1);
			}
			if (SingleWord.startsWith("c")) {
				context.getCounter(MyDebug.If3).increment(1);
			}
		}
		context.write(null, null);
	}

}
