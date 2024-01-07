package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		// Get words from the input line
    		String[] words = value.toString().split("\\s+");
    		
    		for (int i=0; i<words.length-1; i++) {
    			String biGram = words[i] + " " + words[i+1];
    			biGram = biGram.toLowerCase();
    			
    			/**
    			 * Cleaning (optional for this lab exercise)
    			 * matches the bigram only if composed by alphanumeric characters
    			 */
    			if (biGram.matches("[a-z0-9]+ [a-z0-9]+"))
    				context.write(new Text(biGram), new IntWritable(1));
    		}
    }
}
