    import org.apache.hadoop.io.LongWritable;
    import org.apache.hadoop.io.Text;
    import org.apache.hadoop.mapreduce.Mapper;
    import java.io.IOException;
    import java.util.HashMap;
    import java.util.Map;

    public class PropertyMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Map<String, String> boroughNames;
        
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            boroughNames = new HashMap<>();
            boroughNames.put("1", "Manhattan");
            boroughNames.put("2", "Bronx");
            boroughNames.put("3", "Brooklyn");
            boroughNames.put("4", "Queens");
            boroughNames.put("5", "Staten Island");
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] parts = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

            if (parts.length > 77) {  // Checking that the required columns exist in the row
                String zipCode = parts[77].trim();
                String curmktTot=cleanNumericField(parts[57].trim());  // CURMKTTOT is at index 57
                String curactTot=cleanNumericField(parts[59].trim()); //  CURACTTOT is at index 59
                String boro = parts[1].trim(); // BORO is at index 1
                String yr=parts[7].trim();
                String year = parts[7].trim()+"-01-01"; //  YEAR is at index 7
                String streetName=parts[76].trim(); //street name
                String houseNoHi=parts[75].trim();   //houseno_hi
                String houseNoLo=parts[74].trim();    //houseno_lo
                
                String boroName = boroughNames.getOrDefault(boro, "Unknown");           

                // Skipping rows with missing or nonsensical data
                if (boro.isEmpty() || curmktTot.isEmpty() || curactTot.isEmpty() || streetName.isEmpty() || houseNoHi.isEmpty() || houseNoLo.isEmpty() || !curmktTot.matches("\\d+") || !curactTot.matches("\\d+") || yr.equals("2025") || zipCode.isEmpty() || !zipCode.matches("\\d+") || zipCode.charAt(0)=='0') {
                    return; 
                }


                Text mapOutputValue = new Text(boroName + "," + year + "," + curmktTot + "," + curactTot + "," +  streetName + "," +houseNoHi + "," + houseNoLo);

                // Emit borough with concatenated values for further analysis
                context.write(new Text(zipCode), mapOutputValue);
            }
        }

    private String cleanNumericField(String input) {
        return input.replaceAll("[^0-9]", "");  // Removing non-numeric characters
    }

    }


