package FileSystemOps;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MyDriver {

	public static void main(String[] args) throws Exception {

		//Get Hadoop FileSystem
		FileSystem hdfs = FileSystem.get(new Configuration());

		//Get Hadoop Home Directory
		Path homeDir = hdfs.getHomeDirectory();
		System.out.println(homeDir);

		//Get Hadoop Working Directory
		Path workingDir = hdfs.getWorkingDirectory();
		System.out.println(workingDir);

		//Create New Path
		Path newFolderPath = new Path("/MyDataFolder");

		//Delete If Path Exists
		if (hdfs.exists(newFolderPath)) {
			hdfs.delete(newFolderPath, true); 
		}

		//Create New Directory
		hdfs.mkdirs(newFolderPath);

		//Perform CopyFromLocal in Java 
		Path localFilePath = new Path("/home/cloudera/Desktop/biginput.txt");
		Path hdfsFilePath = new Path(newFolderPath + "/biginput.txt");
		hdfs.copyFromLocalFile(localFilePath, hdfsFilePath);

		//Perform CopyToLocal in Java 
		localFilePath = new Path("/home/cloudera/Desktop/mydataapi/");
		hdfs.copyToLocalFile(hdfsFilePath, localFilePath);

		//Create New File 
		Path newFilePath = new Path(newFolderPath + "/newFile.txt");
		hdfs.createNewFile(newFilePath);

		//Add Data to File
		StringBuilder sb = new StringBuilder();
		for (int i = 1; i <= 5; i++) 
		{
			sb.append("Data");
			sb.append(i);
			sb.append("\n");
		}
		byte[] byt = sb.toString().getBytes();
		FSDataOutputStream fsOutStream = hdfs.create(newFilePath);
		fsOutStream.write(byt);
		fsOutStream.close();
		
		//Read Data From File on HDFS
		BufferedReader bfr = new BufferedReader(new InputStreamReader(hdfs.open(newFilePath)));
		String str = null;
		while ((str = bfr.readLine()) != null) 
		{
			System.out.println(str);
		}
	}

}
