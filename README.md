# nyc-fhv-test


1. Explain your rationale for your approach to this task.
   I first looked into the dataset and the API and decided to go with using the requests libarary in python to extract the data and then save the data into a file then put it into s3 with the idea that I would be querying the data from the file using athena, but before I can do that I had to create the IAM roles and policies that allowed the lambda to access the s3 bucket, execute the lambda, and also add partitions to the athena table. I chose lambda for running the python because it is serverless and can handle rather light workloads for this task and I knew for this task it wouldn't require more than the 15 minute timeout. When it came to the format of the file I chose parquet because it is smaller in terms of the storage and also it is cheaper to query on Athena. So that is why in the lambda I convert the json into parquet and then put the file into s3. Also on Athena I had to make sure the point the output location in a separate location as the partitioned parquet files. Finally when it came to the daily schedule for running the lambda daily, I added an eventbridge rule for 21:00 UTC time using this cron expression: cron(0 21 ? * * *), which is 2pm pst or 5pm est when the files are expected to have been "updated" based on the column "last_date_updated". So in summary, used a lambda function to run python to extract the data using the provided API, then loaded the data into S3 as a parquet file plus add the new partition so that the new daily data can be queryable on Athena. Also I created the table in Athena pointing to the location in s3. Afterwards I was able to query the table in Athena. 
2. What else would you do if you had more time?
   If I had more time, I would try to compile a cloudformation template to provide all the resources involved in running the solution, which would be the IAM role+policies, the S3 bucket, lambda function, and the athena workgroup to tie it all together. Also when it came to being able to do transformations so that it is an upsert rather than a full "insert" every time, I was thinking of using the initial S3 location as a staging location with the final table being an Apache Iceberg table to make the actual upsert possible using Athena prepared statements. Granted this was supposed to take 1~2 hours and I was limited to free tier so there was only so much that could be done and delivered. So that is why only the lambda function written in python is being provided. Even with the lambda function I would have liked to added checks to whether or not the add partition worked and also check for errors in the code. 




Provide a SQL query that will answer a question of your choosing about the data. (Examples: What is the median
age of vehicles? What are stats by region of vehicle manufacturer?) I noticed that the vehicle_year had 2023 and 2024 as values and although it wouldn't have made a difference in getting the median it is cleaner to see that those cars are <1 years old so I set them to 0. Using approx_percentile() at .5 gives the closest to the median years. 

SELECT 
    approx_percentile(case when cast(vehicle_year as integer) >= 2023 then 0 else (2023 - cast(vehicle_year as integer)) end, 0.5) AS median_value
FROM 
    "nyc_fhv"."raw";


