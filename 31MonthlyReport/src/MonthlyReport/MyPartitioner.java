package MonthlyReport;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, Text> {

		public int getPartition(Text key, Text value, int numPartitions) {

			int Month = Integer.parseInt(key.toString());
			return Month-1;			
		}

	}
