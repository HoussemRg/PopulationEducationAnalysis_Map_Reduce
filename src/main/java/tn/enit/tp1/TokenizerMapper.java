package tn.enit.tp1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TokenizerMapper
        extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text category = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        // Check if the required field exists
        if (fields.length > 4) {
            int educationNum = Integer.parseInt(fields[4].trim()); // Field for education level

            // Categorize as "Instruits" or "Non instruits"
            if (educationNum >= 13) {
                category.set("Instruits");
            } else {
                category.set("Non instruits");
            }

            // Emit the category with a value of 1
            context.write(category, one);
        }
    }
}
