import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import javax.naming.Context;

public class FilterMapper extends Mapper<LongWritable, Text, Text, Text> {
    private SimpleDateFormat originalFormat = new SimpleDateFormat("MM/dd/yyyy");
    private SimpleDateFormat targetFormat = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String columns = value.toString();
        String[] fields = columns.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

        try {
            int boroIdIndex = 3;
            int boroughIndex = 4;
            int postcodeIndex = 10;
            int classIndex = 15;
            int currentStatusIdIndex = 27;
            int currentStatus = 28;
            int currentStatusDateIndex = 29;
            int novTypeIndex = 30;
            int violationStatusIndex = 31;

            String formattedDate = targetFormat.format(originalFormat.parse(fields[currentStatusDateIndex]));

            String outputValue = String.join(",",
                fields[boroIdIndex],
                fields[boroughIndex],
                fields[postcodeIndex],
                fields[classIndex],
                fields[currentStatusIdIndex],
                fields[currentStatus],
                formattedDate,
                fields[novTypeIndex],
                fields[violationStatusIndex]);

            //Text outputValue = new Text(fields[boroIdIndex] + "," + fields[boroughIndex] + "," + fields[postcodeIndex]+ "," + fields[classIndex]+ "," + fields[currentStatusIdIndex]+ "," + formattedDate + "," + fields[novTypeIndex]+ "," + fields[violationStatusIndex]);

            context.write(new Text(""), new Text(outputValue));
        } catch (ParseException e) {
            // Handle date parsing error if necessary
            context.write(new Text("Error"), new Text("Invalid date format"));
        }
    }
}