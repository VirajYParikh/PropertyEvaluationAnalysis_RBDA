package rbda_proj_sc;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.lang3.ObjectUtils.Null;
import org.apache.hadoop.io.NullWritable;


public class RodentCleaningReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
  public void reduce(Text key, Iterable<Text> cleaned, Context context) throws IOException, InterruptedException {  
      for (Text row: cleaned) {
        context.write(NullWritable.get(), row);
      }
  }
}
