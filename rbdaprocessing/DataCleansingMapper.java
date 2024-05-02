import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DataCleansingMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    private String[] headers;
    private String[] column_not_include;
    private SimpleDateFormat inputFormat = new SimpleDateFormat("mm/dd/yyyy");
    private SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-mm-dd");

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        headers = context.getConfiguration().getStrings("csv_headers");
        column_not_include = context.getConfiguration().getStrings("csv_column_not_include");
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] columns = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

        StringBuilder newRecord = new StringBuilder();
        String zipcode = "";

        // traverse each column
        for (int i = 0; i < columns.length; i++) {
            // if usefull column, append it
            if (!checkCountain(headers[i])) {
                // if not last column, add a comma
                if (i < columns.length - 1) {
                    if (i == 1 || i == 3){
                        try {
                            Date date = inputFormat.parse(columns[i]);
                            String newDateString = outputFormat.format(date);
                            newRecord.append(newDateString);
                        }catch (ParseException e){
                            e.printStackTrace();
                        }
                    }else {
                        newRecord.append(columns[i]);
                    }
                    if (i < columns.length - 2){
                        newRecord.append(",");
                    }
                }else{
                    zipcode = columns[i];
                }
            }
        }

        context.write(new Text(zipcode), new Text(newRecord.toString()));
    }

    private boolean checkCountain(String columnName) {
        for (String column : column_not_include) {
            if (column.equals(columnName)) {
                return true;
            }
        }
        return false;
    }
}