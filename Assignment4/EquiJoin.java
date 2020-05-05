package com.code.dezyre;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
//import org.apache.hadoop.mapreduce.lib.*;
import org.apache.hadoop.mapred.Reducer;
//import org.apache.hadoop.mapred.jobcontrol.Job;

import java.lang.String;

public class EquiJoin{
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
    {
        private Text dataTuples = new Text();
        private Text byKeyJoin = new Text();
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
        {  
            String line = value.toString();
            String[] val = line.split(",");
            //String table = val[0];
            String keyjoin = val[1];   
            byKeyJoin.set(keyjoin);
            dataTuples.set(line);
            output.collect(byKeyJoin,dataTuples);   
        } 
    }
    
    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>
    {  
        public void reduce(Text key, Iterator <Text> values,OutputCollector <Text, Text> output, Reporter reporter) throws IOException 
        { 
            List<String> first = new ArrayList<String>();
            List<String> second = new ArrayList<String>();
            List<String> theseWrite = new ArrayList<String>();
            
            
            String firstName = "";
            Text result = new Text();
            String res = new String();
            boolean checkBit = true;  
            while(values.hasNext())
            {
                String value = values.next().toString();
                String[] valueSplit = value.split(",");
                if (checkBit == true) 
                {
                    firstName = valueSplit[0];
		            checkBit = false;
                }
                if (firstName == valueSplit[0] ) {
                    first.add(value);
                }
                else 
                {
                    second.add(value);
                }
                theseWrite.add(value);   
            }
	    Collections.reverse(theseWrite);
            Text temp = new Text("");
	    if ( first.size() == 0 || second.size() == 0)
	    {
	    	key.clear();
	    }
	    else
            {
            	for (int i =0; i<theseWrite.size(); i++) 
            	{
                	for (int j=i+1; j<theseWrite.size(); j++) 
                	{
                    		res = theseWrite.get(i) + ", " + theseWrite.get(j);
                    		result.set(res);
                    		output.collect(temp,result);              
                	}  
                
            	}  
            }
        }

    

    public static void main(String[] args) throws Exception 
	    {         
    	if (args.length < 2) {
    	    System.err.println("Not enough arguments received.");
    	    return;
    	}

			JobConf conf = new JobConf(EquiJoin.class);
			conf.setJobName("EquiJoin");
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			//conf.set("mapred.textoutputformat.separator"," ");
			conf.setMapperClass(Map.class);
			conf.setReducerClass(Reduce.class);
			
			//Path input_path = new Path("inputMolife.txt");
			//Path output_path = new Path("outputfileMolife.txt");
	        FileInputFormat.setInputPaths(conf, new Path(args[0]));
	        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	        
	        System.out.println(args[0]);
	        System.out.println(args[1]);
	        JobClient.runJob(conf);
	        
	    	
	    }
    }
}
