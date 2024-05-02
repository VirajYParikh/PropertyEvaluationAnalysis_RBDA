
package rbda_proj_sc;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RodentInfestationProfilingMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private static final int[] columnsToProcess = {0, 6, 8};

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] columns = value.toString().split(","); // Split CSV line into columns

        // Define descriptive column names
        String[] columnNames = {"Inspection Type", "Unknown", "Unknown", "Unknown", "Unknown", "Unknown", "Borough", "Unknown", "Inspection Result"};

        // Iterate over selected columns
        for (int columnIndex : columnsToProcess) {
            if (columnIndex >= 0 && columnIndex < columns.length) {
                String columnName = columnNames[columnIndex];
                if (columnIndex == 0){
                    String category = columns[columnIndex].split("\\s+")[1].trim();
                    context.write(new Text(columnName + " : " + category), one);
                } else {
                    String category = columns[columnIndex].trim();
                    context.write(new Text(columnName + " : " + category), one);
                }
            }
        }
    }
}


