package WcPartitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, IntWritable> {

		public int getPartition(Text key, IntWritable value, int numPartitions) {

			String myKey = key.toString().toLowerCase();

			if (myKey.startsWith("a") ) {
				return 0;
			}
			else if (myKey.startsWith("n")) {
				return 1;
			}
			else if (myKey.startsWith("k")) {
				return 2;
			}				
			else {
				return 3;
			}
		}

	}
