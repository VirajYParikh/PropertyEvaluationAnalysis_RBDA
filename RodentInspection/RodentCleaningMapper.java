package rbda_proj_sc;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class RodentCleaningMapper
    extends Mapper<LongWritable, Text, NullWritable, Text> {

  private static final SimpleDateFormat INPUT_DATE_FORMAT = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
  private static final SimpleDateFormat OUTPUT_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String row = value.toString();
    String[] columns = row.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

    // Verifying if the data entered is correct
    if (columns.length < 25) {
      return;
    }

    // Feeding the data into the mapper
    String inspection_type = columns[0];
    String job_ticket = columns[1];
    String job_progress = columns[3];
    String zip_code = columns[10];
    String latitude = columns[13];
    String longitude = columns[14];
    String borough = columns[15];
    String inspection_date = extractDate(columns[16]);
    String result = columns[17];
    String approved_date = extractDate(columns[18]);

    // Removing all the data which does not have zipcode, job_tickets or inspection
    // status.
    if (zip_code.isEmpty() || zip_code.equals("0") || job_ticket.isEmpty() || result.isEmpty()) {
      return;
    }

    // Writing the output to the context
    String output = inspection_type + "," + job_ticket + "," + job_progress + "," + zip_code + "," + latitude + ","
        + longitude + "," + borough + "," + inspection_date + "," + result + "," + approved_date;
    context.write(NullWritable.get(), new Text(output));
  }

  private String extractDate(String dateString) {
    try {
      Date date = INPUT_DATE_FORMAT.parse(dateString);
      return OUTPUT_DATE_FORMAT.format(date);
    } catch (ParseException e) {
      // Handle parse exception (e.g., if the date format is invalid)
      e.printStackTrace();
      return ""; // Return empty string if unable to parse
    }
  }
}
