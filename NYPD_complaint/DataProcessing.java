import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

public class DataProcessing {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: DataProcessing <input path> <cleansing_output path> <profiling_output path>");
            System.exit(-1);
        }

        Path inputPath = new Path(args[0]);
        Path cleansingOutputPath = new Path(args[1]);

        String csv_headers = "CMPLNT_NUM,CMPLNT_FR_DT,CMPLNT_FR_TM,CMPLNT_TO_DT,CMPLNT_TO_TM,ADDR_PCT_CD,RPT_DT,KY_CD,OFNS_DESC,PD_CD,PD_DESC,CRM_ATPT_CPTD_CD,LAW_CAT_CD,BORO_NM,LOC_OF_OCCUR_DESC,PREM_TYP_DESC,JURIS_DESC,JURISDICTION_CODE,PARKS_NM,HADEVELOPT,HOUSING_PSA,X_COORD_CD,Y_COORD_CD,SUSP_AGE_GROUP,SUSP_RACE,SUSP_SEX,TRANSIT_DISTRICT,Latitude,Longitude,Lat_Lon,PATROL_BORO,STATION_NAME,VIC_AGE_GROUP,VIC_RACE,VIC_SEX,Zip Codes";
        String[] headers = csv_headers.split(",");

        String column_not_include = "CMPLNT_NUM,ADDR_PCT_CD,RPT_DT,KY_CD,OFNS_DESC,PD_CD,PD_DESC,JURIS_DESC,JURISDICTION_CODE,PARKS_NM,HADEVELOPT,HOUSING_PSA,X_COORD_CD,Y_COORD_CD,TRANSIT_DISTRICT,Latitude,Longitude,Lat_Lon,PATROL_BORO,STATION_NAME";
        String[] dropped_columns = column_not_include.split(",");

        String column_category = "OFNS_DESC,PD_DESC,CRM_ATPT_CPTD_CD,LAW_CAT_CD,BORO_NM,PREM_TYP_DESC,SUSP_AGE_GROUP,SUSP_RACE,SUSP_SEX,VIC_AGE_GROUP,VIC_RACE,VIC_SEX,Zip Codes";
        String[] categories = column_category.split(",");

        // data cleansing job
        Configuration conf = new Configuration();
        conf.setStrings("csv_headers", headers);
        conf.setStrings("csv_column_not_include", dropped_columns);
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf);
        job.setJarByClass(DataProcessing.class);
        job.setJobName("Data Cleansing");

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, cleansingOutputPath);

        job.setMapperClass(DataCleansingMapper.class);
        job.setReducerClass(DataCleansingReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

//        // data profiling job
//        for (int i = 0; i < categories.length; i++) {
//            Path profilingPath = new Path(args[2] + "_" + categories[i]);
//
//            Configuration profiling_conf = new Configuration();
//            profiling_conf.set("profiling_column", categories[i]);
//            profiling_conf.setStrings("csv_headers", headers);
//
//            Job profiling_job = Job.getInstance(profiling_conf);
//            profiling_job.setJarByClass(DataProcessing.class);
//            profiling_job.setJobName("Data profiling");
//
//            FileInputFormat.addInputPath(profiling_job, inputPath);
//            FileOutputFormat.setOutputPath(profiling_job, profilingPath);
//
//            profiling_job.setMapperClass(DataProfilingMapper.class);
//            profiling_job.setCombinerClass(DataProfilingReducer.class);
//            profiling_job.setReducerClass(DataProfilingReducer.class);
//            profiling_job.setNumReduceTasks(1);
//
//            profiling_job.setOutputKeyClass(Text.class);
//            profiling_job.setOutputValueClass(IntWritable.class);
//
//            profiling_job.waitForCompletion(true);
//        }
    }
}