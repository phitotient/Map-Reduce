package MapSideJoin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMap extends Mapper<LongWritable,Text, Text, NullWritable> {
    
	private Map<String, String> CustDetails = new HashMap<String, String>();		
	
	protected void setup(Context context) throws java.io.IOException, InterruptedException{
		
		Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());		
		for (Path SinglePath : files) {
			if (SinglePath.getName().equals("custs.dat")) 
			{
				BufferedReader reader = new BufferedReader(new FileReader(SinglePath.toString()));
				String line = reader.readLine();
				while(line != null) 
				{
					String[] lineParts = line.split(",");
					String CustUid = lineParts[0];
					String CustName = lineParts[1]+" "+lineParts[2];
					CustDetails.put(CustUid, CustName);
					line = reader.readLine();
				}
				reader.close();
			}
		}
		if (CustDetails.isEmpty()) 
		{
			throw new IOException("Unable To Load Customer Data.");
		}
	}
	
    protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException 
    {    	   	
    	String Record = value.toString();
		String RecordParts[] = Record.split(",");
		
		String Uid = RecordParts[2];
		String Amt = RecordParts[3];
		String Name =  CustDetails.get(RecordParts[2]);
		
		Text OutPutUid = new Text("Uid:"+Uid);
		Text OutPutAmt = new Text("Amt:"+Amt);
		Text OutPutName = new Text("Name:"+Name);
		
		Text OutPutKey = new Text("{"+OutPutName+";"+OutPutUid+";"+OutPutAmt+";}");
  	  	context.write(OutPutKey,null);
    }  
}

