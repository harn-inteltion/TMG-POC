# Databricks notebook source
dbutils.widgets.text("proc_name", "", "Process Name")
dbutils.widgets.text("proc_run_id", "", "Process Run ID")
dbutils.widgets.text("data_dt", "", "Data Date")

proc_name = dbutils.widgets.get("proc_name")
proc_run_id = dbutils.widgets.get("proc_run_id")
data_dt = dbutils.widgets.get("data_dt")

# COMMAND ----------

# MAGIC %run ./common_function

# COMMAND ----------

class transform:
    # class attribute
    _catalog=None
    _proc_name=None
    _proc_run_id=None
    _script_file=None
    _transform_script=None
    _data_dt=None

    # initial class attribute from workflow parameter
    def __init__(self,_catalog,_proc_name,_proc_run_id,_data_dt):  
        self._proc_name=convert_empty_to_none(_proc_name)
        self._proc_run_id=convert_empty_to_none(_proc_run_id)
        self._catalog=convert_empty_to_none(_catalog)  
        self._data_dt=convert_empty_to_none(_data_dt)      

    # set working catalog   
    def set_working_catalog(self):
        spark.sql(f"use catalog {self._catalog}")

    # check and set inbound system to class properties
    def get_transform_script(self):
        try:
            _df_config = spark.sql(f"SELECT script_path FROM cntrlfw.cntrl_config_proc WHERE proc_name='{self._proc_name}' AND is_active=1")
            _df_config_row=_df_config.first() 

            if _df_config.count()==0:    
                raise SystemError(f"Invalid provided script!:{self._proc_name}, please re-check on config table.")
            else:
                self._script_file=_df_config_row['script_path']

            if self._script_file is None:
                raise SystemError(f"Invalid provided script!:{self._script_file}, please re-check on config table.")
            else:
                with open(self._script_file) as scriptFile:
                    self._transform_script = scriptFile.read()

            if self._transform_script is None or self._transform_script.strip()=="":
                raise SystemError(f"Invalid transform script!:{self._transform_script}, please re-check script on {self._script_file}.")
            else:
                pass
            
        except Exception as err:
            raise SystemError(err)        


    def run_multiple_transform_script(self):
        sql_arr = self._transform_script.split(";")
        #print(sql_arr)
        for sql in sql_arr:
            if len(sql.strip()) > 0:
                _transform_script=(sql.replace("${data_dt}",self._data_dt)).replace("${proc_run_id}",proc_run_id)
                #print(_transform_script)
                df_source=spark.sql(_transform_script) 

    def run_process(self):
        try:
            #print("=> 1. set working catalog.")
            self.set_working_catalog()
            print("=> 2. get transform script.")
            self.get_transform_script()
            print("=> 3. run transform script.")
            self.run_multiple_transform_script()
            print("=> 4. finised transform.")
        except Exception as err:
            raise SystemError(err)

# COMMAND ----------

# Main process and throw error for a WorkflowException.
batch_status=set_process_success()
try:    
    print(f"=> start process. : {proc_name}")
    objprocess=transform("poc_stg",proc_name,proc_run_id,data_dt)
    objprocess.run_process()  
    print(f"=> end process. : {proc_name}")
except Exception as err:
    batch_status=set_process_failed(err)

dbutils.notebook.exit(batch_status)  

