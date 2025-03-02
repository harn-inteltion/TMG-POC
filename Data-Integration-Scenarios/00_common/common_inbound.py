# Databricks notebook source
dbutils.widgets.text("proc_name", "", "Process Name")
dbutils.widgets.text("proc_run_id", "", "Process Run ID")
dbutils.widgets.text("start_dt", "", "Start Data Date")
dbutils.widgets.text("end_dt", "", "End Data Date")
dbutils.widgets.text("run_mode", "", "Run Mode (N/R)")
dbutils.widgets.text("data_dt", "", "Data Date")

proc_name = dbutils.widgets.get("proc_name")
proc_run_id = dbutils.widgets.get("proc_run_id")
start_dt = dbutils.widgets.get("start_dt")
end_dt = dbutils.widgets.get("end_dt")
run_mode = dbutils.widgets.get("run_mode")
proc_name = dbutils.widgets.get("proc_name")
data_dt = dbutils.widgets.get("data_dt")

# COMMAND ----------

# MAGIC %run ./common_function

# COMMAND ----------

def get_inbound_config(process_name,_data_dt):
    try:
        df_config = spark.sql(f"SELECT src_system,src_file_name,src_delimeter,src_volume_name,trgt_tbl_name FROM cntrlfw.cntrl_config_proc where proc_name='{process_name}' and is_active=1")
        df_row=df_config.first() 

        if df_config.count()==0:    
            raise ValueError(f"ERROR: Invalid process name!:{process_name}, please re-check on config table.")
        else:
            src_system=df_row['src_system']
            file_name=df_row['src_file_name']
            delimiter=df_row['src_delimeter']
            volume_name=df_row['src_volume_name']
            target_table_name=df_row['trgt_tbl_name']
            if process_name == "INB_POC":
            # Append _data_dt and ".csv" to file_name if process_name is "POC"
                file_name = f"{file_name}_{_data_dt}.csv"

        if src_system is None or src_system.strip()=="":
            raise SystemError(f"Invalid config source system!, please re-check on config table.")

        if file_name is None or file_name.strip()=="":
            raise SystemError(f"Invalid config file name!, please re-check on config table.")

        if target_table_name is None or target_table_name.strip()=="":
            raise SystemError(f"Invalid target_table_name!, please re-check on config table.")

        if delimiter is None or delimiter.strip()=="":
            raise SystemError(f"Invalid delimiter!, please re-check on config table.")  

        if volume_name is None or volume_name.strip()=="":
            raise SystemError(f"Invalid volume_name!, please re-check on config table.")   

        return {"src_system": src_system, "file_name": file_name,"delimiter": delimiter,"volume_name": volume_name,"target_table_name": target_table_name}
        
    except Exception as err:
        raise SystemError(err)  

# COMMAND ----------

def check_data_file(file_path):
  try:
      file_list=dbutils.fs.ls(file_path)
      if len(file_list) > 0:
        return True
  except Exception as err:
      raise SystemError(f"ERROR: Not found data file, {file_path}")

# COMMAND ----------

def read_data_from_file(file_path,_delimiter,_schema):
  try:
    if _delimiter == "FIX":
      return spark.read.format("parquet").load(file_path).selectExpr(*_schema)
    else:
      if _schema == None:
        return spark.read.option("header", "true") \
          .option("sep", _delimiter) \
          .option("ignoreLeadingWhiteSpace", "true") \
          .option("ignoreTrailingWhiteSpace", "true") \
          .csv(file_path)
      else:
        return spark.read.csv(file_path, header=True, sep=_delimiter, schema=_schema) 
  except Exception as err:
    raise SystemError("ERROR: Unable to read data file, {err}")

# COMMAND ----------

def get_ctrl_file_record_count(file_name,ctrl_file_path):
  if file_name.startswith("POC"):
    # if file name is POC_XXXX.csv then using TMG's given control file
    df_ctrl = spark.read.csv(ctrl_file_path, header=True)
    df_filtered = df_ctrl.filter(col("FILENAME") == file_name)
    ctrl_val = df_filtered.select("FILENAME").collect()[0][0]  # Collect as list of rows
    ctrl_dt = ctrl_val.split("_")[1].split(".")[0]  # Extract 'date'
    ctrl_cnt = int(df_filtered.select("CNT_REC").collect()[0][0])
  else: 
    # else using normal control file
    df_ctrl = spark.read.csv(ctrl_file_path, header=True)
    df_filtered = df_ctrl.filter(col("FILENAME") == file_name)
    ctrl_val = df_filtered.select("FILENAME").collect()[0][0]  # Collect as list of rows
    ctrl_dt = ctrl_file_path.split("_")[-1].split(".")[0]
    ctrl_cnt = int(df_filtered.select("CNT_REC").collect()[0][0])
  return {"ctrl_dt": ctrl_dt, "ctrl_cnt": ctrl_cnt}




# COMMAND ----------

def parse_fixed_width(df, schema):
    try:
        start = 0
        parsed_cols = []
        for field in schema.fields:
            length = int(field.metadata.get("length", 0))
            parsed_cols.append(
                trim(substring(col("value"), start + 1, length)).alias(field.name)
            )
            start += length
        return df.select(*parsed_cols)
    except Exception as err:
      raise SystemError(f"ERROR: Unable to parse data file , {err}")

# COMMAND ----------

def check_data_date(data_dt,ctrl_dt):
    if data_dt != ctrl_dt :
        raise ValueError("ERROR: Date from parameter does not match the control file, {} != {}".format(data_dt,ctrl_dt))
    else :
        return("INFO: Date form parameter {}, Date from Control file {}".format(data_dt, ctrl_dt))

# COMMAND ----------

def check_record_count(data_cnt, ctrl_cnt):
    if data_cnt != ctrl_cnt :
        raise ValueError("ERROR: Number of records in file does not match the control file, {} != {}".format(data_cnt,ctrl_cnt))
    else :
        return("INFO: Count records from file {}, Control file record count {}".format(data_cnt, ctrl_cnt))

# COMMAND ----------

def insert_inbound_file_log(_proc_name,_proc_run_id,_proc_status,_proc_file_name,_total_record):
    try:
        _strInsertLog="""insert into cntrlfw.cntrl_inbound_file_log (proc_name,proc_run_id,proc_status,file_name,total_record,start_proc_dtm)
                    values('{}','{}','{}','{}',{},current_timestamp());
                    """.format(_proc_name,_proc_run_id,_proc_status,_proc_file_name,_total_record)
        _dfLog = spark.sql(_strInsertLog)  
    except Exception as err:
        raise

# COMMAND ----------

def update_inbound_file_log(_proc_name,_proc_run_id,_proc_status,_proc_file_name,_total_record,_proc_error_message):
    try:
        if _proc_error_message is None: 
            _proc_error_message="null"
        else:
                _proc_error_message=f"'{_proc_error_message}'"

        _strUpdateLog ="""update cntrlfw.cntrl_inbound_file_log set proc_status='{}',error_message={},total_record={},end_proc_dtm=current_timestamp()
                        where proc_name='{}' and proc_run_id='{}' and proc_status='Processing' and file_name='{}'
                        """.format(_proc_status,_proc_error_message,_total_record,_proc_name,_proc_run_id,_proc_file_name) 
        _df_status = spark.sql(_strUpdateLog)
    except Exception as err:
        raise

# COMMAND ----------

def move_processed_file_to_archived(_data_file,_control_file,_data_dt):
        thai_tz = pytz.timezone('Asia/Bangkok')
        timestamp = datetime.now(thai_tz).strftime('%Y%m%d%H%M%S')

        _archived_data_file = f'{"/".join(_data_file.split("/")[:-1])}/archived/{timestamp}/{_data_file.split("/")[-1].split(".")[0]}_{_data_dt}_{timestamp}.{_data_file.split("/")[-1].split(".")[-1]}'

        _archived_control_file=f'{"/".join(_control_file.split("/")[:-1])}/archived/{timestamp}/{_control_file.split("/")[-1].split(".")[0]}_{_data_dt}_{timestamp}.{_control_file.split("/")[-1].split(".")[-1]}'

        # Step 1: Copy the files to the archived folder
        dbutils.fs.cp(_data_file, _archived_data_file, True)
        # dbutils.fs.cp(_control_file, _archived_control_file, True)

        # Step 2: Delete the original files after copying
        dbutils.fs.rm(_data_file)
        # dbutils.fs.rm(_control_file)

# COMMAND ----------

def run_inbound_processed(_data_dt,ctrl_file_path):
  try:  
    total_record=0
    _ingest_time=datetime.now()
    # insert processing status to inbound file log
    insert_inbound_file_log(proc_name,proc_run_id,"Processing",data_file,total_record)   

    # check data file on landing zone
    check_data_file(data_file)

    # read data from file to dataframe
    parsed_df=read_data_from_file(data_file,delimiter,schema)
    total_record=parsed_df.count()

    # check data date with control file
    ctrl_val = get_ctrl_file_record_count(file_name,ctrl_file_path)
    ctrl_cnt = ctrl_val['ctrl_cnt']
    ctrl_dt = ctrl_val['ctrl_dt']
    check_data_date(_data_dt,ctrl_dt)

    # check total record with control file
    check_record_count(total_record,ctrl_cnt)    

    # write data to target table

    if proc_name=='INB_POC':
        parsed_df.write \
            .mode("overwrite")\
            .option("replaceWhere", f"BUSINESSDAYDATE = to_date('{_data_dt}','yyyyMMdd')")\
            .saveAsTable(f"{target_table_name}")
    elif proc_name=='INB_IRIS':
        parsed_df.write.mode("overwrite").saveAsTable(f"{target_table_name}")
    else:
        parsed_df = parsed_df.withColumn("DATA_DT", to_date(lit(_data_dt), 'yyyyMMdd'))\
                        .withColumn("PROC_RUN_ID",lit(proc_run_id))\
                        .withColumn("INS_PROC_DTM",lit(_ingest_time))                
        parsed_df.write\
        .mode("overwrite")\
        .partitionBy("DATA_DT")\
        .option("replaceWhere", f"DATA_DT = to_date('{_data_dt}','yyyyMMdd')")\
        .saveAsTable(f"{target_table_name}")

    # update processed status to inbound file log 
    update_inbound_file_log(proc_name,proc_run_id,"Success",data_file,total_record,None)

    # move processed file to archived folder
    move_processed_file_to_archived(data_file,ctrl_file_path,_data_dt)

  except Exception as err:
      update_inbound_file_log(proc_name,proc_run_id,"Failed",data_file,total_record,err)
      raise   

# COMMAND ----------

def run_inbound_processed_multiple(_data_dt,_data_file):
  try:  
    total_record=0
    _ingest_time=datetime.now()
    # insert processing status to inbound file log
    insert_inbound_file_log(proc_name,proc_run_id,"Processing",data_file,total_record)   

    #prep file list based on data_dt
    selected_files = []
    files = dbutils.fs.ls(_data_file)
    pattern = r"run-(\d+)-part"
    
    for file in files:
        # Extract the epoch timestamp from the file name using regex
        match = re.search(pattern, file.name)
        if match:
            # Convert the extracted string to an integer (epoch in milliseconds)
            epoch_millis = int(match.group(1))
            # Convert milliseconds to seconds, then create a UTC datetime object
            file_date = datetime.utcfromtimestamp(epoch_millis / 1000).strftime("%Y%m%d")
            # Compare with the target date
            if file_date == _data_dt:
                selected_files.append(file.path)    
    
    # Read the selected files if any are found
    if selected_files:
        df = spark.read.parquet(*selected_files)
    else:
        raise SystemError("No files found for date", _data_dt)
    
    total_record=df.count()

    parsed_df = df.withColumn("DATA_DT", to_date(lit(_data_dt), 'yyyyMMdd'))\
                    .withColumn("PROC_RUN_ID",lit(proc_run_id))\
                    .withColumn("INS_PROC_DTM",lit(_ingest_time))                
    parsed_df.write\
    .mode("overwrite")\
    .partitionBy("DATA_DT")\
    .option("replaceWhere", f"DATA_DT = to_date('{_data_dt}','yyyyMMdd')")\
    .saveAsTable(f"{target_table_name}")

    # update processed status to inbound file log 
    update_inbound_file_log(proc_name,proc_run_id,"Success",data_file,total_record,None)


    thai_tz = pytz.timezone('Asia/Bangkok')
    timestamp = datetime.now(thai_tz).strftime('%Y%m%d%H%M%S')
 
    for inserted_file in selected_files:
        inserted_file_name = inserted_file.split('/')[-1]
        _archived_data_file = f'{_data_file}archived/{timestamp}/{inserted_file_name}'
        dbutils.fs.cp(inserted_file, _archived_data_file, True)
        dbutils.fs.rm(inserted_file)

  except Exception as err:
      update_inbound_file_log(proc_name,proc_run_id,"Failed",data_file,total_record,err)
      raise   

# COMMAND ----------

# Main process and throw error for a WorkflowException.
batch_status=set_process_success()
try:    
    print(f"=> start process. : {proc_name}")
    
    # Get config process value
    _data_dt=data_dt.replace("-","") 
    config_val=get_inbound_config(proc_name,_data_dt)
    target_table_name=config_val['target_table_name']
    delimiter=config_val['delimiter']
    file_name=config_val['file_name']
    volume_name=config_val['volume_name']
    print(config_val)
       
    print(f"_data_dt :{_data_dt}")
    
    if volume_name=='poc_raw':
        data_file=f's3://tmg-poc-awsdb-apse1-stack-97db8-bucket/unity-catalog/catalog/poc_raw/{file_name}'
    
        if file_name.startswith("POC"):
            control_file = 's3://tmg-poc-awsdb-apse1-stack-97db8-bucket/unity-catalog/catalog/poc_cntrlfw/control_file/POC_RAW_COUNT_REC.csv'
        else:
            control_file = f's3://tmg-poc-awsdb-apse1-stack-97db8-bucket/unity-catalog/catalog/poc_cntrlfw/control_file/{file_name.split(".")[0]}_ctrl_{_data_dt}.csv'
        print(f"data_file :{data_file}")
        print(f"control_file :{control_file}")
        
        print(f"get_file_schema :{file_name}")
        schema = get_file_schema(file_name)
        
        print(f"run_inbound_processed :{data_dt}")
        run_inbound_processed(_data_dt,control_file)    
        print(f"=> end process. : {proc_name}")
    else:
        data_file=f's3://tmg-poc-awsdb-apse1-stack-97db8-bucket/unity-catalog/catalog/poc_raw/{volume_name}/'
        print(f"data_file :{data_file}* where data_dt = {data_dt}")
        print(f"run_inbound_processed :{data_dt}")
        run_inbound_processed_multiple(_data_dt,data_file)
        print(f"=> end process. : {proc_name}")


except Exception as err:
    batch_status=set_process_failed(err)

dbutils.notebook.exit(batch_status)  

# COMMAND ----------

# MAGIC %md
# MAGIC ----
