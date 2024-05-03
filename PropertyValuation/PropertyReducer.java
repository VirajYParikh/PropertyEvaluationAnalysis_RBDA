    import org.apache.hadoop.io.Text;
    import org.apache.hadoop.mapreduce.Reducer;

    import java.io.IOException;
    import java.util.HashMap;
    import java.util.Map;

    public class PropertyReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Long[]> yearData = new HashMap<>();
            for (Text value : values) {
                String[] parts = value.toString().split(",");
                String yearKey = parts[0] + "," + parts[1] + "," + parts[4] + "," + parts[5] + "," + parts[6]; // Borough, Year, StreetName, HouseNo_HI, HouseNoLO
                long mkt = parseLong(parts[2]);
                long act = parseLong(parts[3]);
    

                Long[] sums = yearData.getOrDefault(yearKey, new Long[]{0L, 0L, 0L}); // {sumMkt, sumAct, count}
                sums[0] += mkt;
                sums[1] += act;
                sums[2]++;
                yearData.put(yearKey, sums);
            }

            // Emitting each year's data for the current ZIP code
            for (Map.Entry<String, Long[]> entry : yearData.entrySet()) {
                Long[] val = entry.getValue();
                if (val[2] > 0) {
                    long avgMkt = val[0] / val[2];
                    long avgAct = val[1] / val[2];
                    context.write(key, new Text(entry.getKey() + "," + avgMkt + "," + avgAct));
            }
        }
        }
        private long parseLong(String value) {
            String trimmedValue = value.trim(); // Trimming the value to remove leading/trailing whitespace
            try {
                return Long.parseLong(trimmedValue);
            } catch (NumberFormatException e) {
                throw new NumberFormatException("Failed to parse '" + trimmedValue + "' as long.");
            }
        }
    }
