import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataProfilingMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private String profiling_column;
    private String[] headers;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        profiling_column = context.getConfiguration().get("profiling_column");
        headers = context.getConfiguration().getStrings("csv_headers");
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] columns = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

        // traverse each column
        for (int i = 0; i < columns.length; i++) {
            if (headers[i].equals(profiling_column)) {
                context.write(new Text(columns[i]), new IntWritable(1));
                break;
            }
        }

    }
}
