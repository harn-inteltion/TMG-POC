# Databricks notebook source
dbutils.widgets.text("proc_name", "", "Process Name")
dbutils.widgets.text("proc_run_id", "", "Process Run ID")
dbutils.widgets.text("start_dt", "", "Start Data Date")
dbutils.widgets.text("end_dt", "", "End Data Date")
dbutils.widgets.text("run_mode", "", "Run Mode (N/R)")

proc_name = dbutils.widgets.get("proc_name")
proc_run_id = dbutils.widgets.get("proc_run_id")
start_dt = dbutils.widgets.get("start_dt")
end_dt = dbutils.widgets.get("end_dt")
run_mode = dbutils.widgets.get("run_mode")

# COMMAND ----------

# MAGIC %run ./common_function

# COMMAND ----------

class process :
    # class attribute
    proc_status = None
    proc_notebook_path = None
    proc_message = None
    running_dt = None
    run_start_dt = None
    run_end_dt = None
    loop_no = 0

    _proc_sub_message=None
    _proc_sub_status=None

    # class private attribute
    __run_mode="N"
    __running_dt=None
    __end_proc_dtm=None
    __proc_sub_message=None

    # initial class attribute from workflow parameter
    def __init__(self,_proc_name,_proc_run_id,_run_mode,_run_start_dt,_run_end_dt):  
        self.proc_name=convert_empty_to_none(_proc_name)
        self.proc_run_id=convert_empty_to_none(_proc_run_id)
        self.run_mode=convert_empty_to_none(_run_mode)
        self.run_start_dt = convert_empty_to_none(_run_start_dt)
        self.run_end_dt = convert_empty_to_none(_run_end_dt)

    def set_working_catalog_and_schema(self,_catalog,_schema):
        spark.sql(f"use catalog {_catalog}")
        spark.sql(f"USE {_schema}")

    def validate_wf_parameter(self):
        if self.run_mode=="R" and (self.run_start_dt is None or self.run_end_dt is None):
            raise SystemError("if run_mode='R' ,run_start_dt and run_end_dt must be specified")   

        elif self.run_start_dt is not None and (not self.run_mode=="R" or self.run_end_dt is None):
            raise SystemError("if specified run_start_dt ,run_mode must be 'R' and run_end_date must be specified")

        elif self.run_end_dt is not None and (not self.run_mode=="R" or self.run_start_dt is None):
            raise SystemError("if specified run_end_dt ,run_mode must be 'R' and run_start_dt must be specified")

        if self.run_start_dt is not None:  
            try:
                _vali=datetime.strptime(self.run_start_dt, '%Y-%m-%d').date()
            except Exception as err:
                raise SystemError("param run_start_dt must be specify as format yyyy-MM-dd")

        if self.run_end_dt is not None:  
            try:
                _vali=datetime.strptime(self.run_end_dt, '%Y-%m-%d').date()
            except Exception as err:
                raise SystemError("param run_end_dt must be specify as format yyyy-MM-dd")

        if self.run_mode=="R" and self.run_end_dt<self.run_start_dt:
            raise SystemError("param run_end_dt must be equal or more than run_start_dt")

        if not self.run_mode=="R" and not self.run_mode=="N" and self.run_mode is not None:  
            raise SystemError("param run_mode must be R, N or empty")
        
        return None

    def cal_process_date(self):

        wf_confg = spark.sql(f"SELECT * FROM cntrlfw.cntrl_config_proc where proc_name='{self.proc_name}' and is_active=1")
        wf_status = spark.sql(f"SELECT * FROM cntrlfw.cntrl_proc_status where proc_name='{self.proc_name}' ")

        df_config=wf_confg.first()
        df_status=wf_status.first()

        if wf_confg.count()==0:    
            raise SystemError(f"processing {self.proc_name} is not exist or not active!")
        elif df_status and df_status['proc_status']=="Running":
            raise SystemError(f"processing {self.proc_name} is currently running!")

        try:
            if self.run_mode is None:
                self.run_mode="N" 
            
            _strQuery="""
                merge into cntrlfw.cntrl_proc_status as target
                using (
                    select cf.proc_name
                    ,adt.proc_run_id
                    ,adt.proc_status
                    ,adt.run_mode
                    ,ifnull(adt.running_dt,
                                    case when adt.start_dt is not null then adt.start_dt
                                        else
                                            case  when cf.data_freq='D' then 
                                                        case  when adt.param_start_dt !='None' then adt.param_start_dt
                                                            else date_add(adt.end_dt,-(cf.run_period-1)) end
                                                    when cf.data_freq='M' then 
                                                        case when adt.param_start_dt !='None' then last_day(adt.param_start_dt) 
                                                            else last_day(add_months(adt.end_dt,-(cf.run_period-1))) end
                                                    end 
                                end
                    ) as running_dt
                    ,case when adt.start_dt is not null then adt.start_dt 
                        else
                                case when cf.data_freq='D' then 
                                        case when adt.param_start_dt !='None' then adt.param_start_dt
                                        else date_add(adt.end_dt,-(cf.run_period-1)) 
                                        end
                                    when  cf.data_freq='M' then 
                                        case when adt.param_start_dt !='None' then last_day(adt.param_start_dt) 
                                        else last_day(add_months(adt.end_dt,-(cf.run_period-1))) 
                                        end
                                end 
                        end as start_dt
                    ,adt.end_dt
                    ,dateadd(hour , 7, current_timestamp()) start_proc_dtm
                    ,dateadd(hour , 7, current_timestamp()) ins_proc_dtm
                    from  (
                    select prm.proc_run_id
                        ,cfp.proc_name
                        ,prm.proc_status
                        ,case when ifnull(ps.proc_status,'')=prm.status_failed then ps.run_mode else prm.run_mode  end run_mode
                        ,case when ifnull(ps.proc_status,'')=prm.status_failed then ps.running_dt else null end running_dt
                        ,case when ifnull(ps.proc_status,'')=prm.status_failed then ps.end_dt
                            else  
                                case when cfp.data_freq='D' then 
                                        case when prm.param_end_dt !='None' then prm.param_end_dt
                                        else date_add(date(dateadd(hour , 7, current_timestamp())),-cfp.data_back_day) 
                                        end   
                                    when cfp.data_freq='M' then  
                                        case when prm.param_end_dt !='None' then last_day(prm.param_end_dt)
                                        else last_day(add_months(date(dateadd(hour , 7, current_timestamp())),-cfp.data_back_day)) 
                                        end 
                                    end 
                            end as end_dt
                        ,case when ifnull(ps.proc_status,'')=prm.status_failed then ps.start_dt else null end start_dt
                        ,prm.param_start_dt
                        ,prm.param_end_dt
                    from (select  '{}' proc_name
                                ,'{}' proc_run_id
                                ,'{}' proc_status
                                ,'{}' run_mode
                                ,'{}' param_start_dt
                                ,'{}' param_end_dt 
                                ,'Failed' status_failed
                            ) prm 
                    inner join cntrlfw.cntrl_config_proc cfp on prm.proc_name=cfp.proc_name
                    left join cntrlfw.cntrl_proc_status ps on cfp.proc_name=ps.proc_name
                    where cfp.proc_name=prm.proc_name and cfp.is_active=1
                    ) adt 
                    inner join cntrlfw.cntrl_config_proc cf on adt.proc_name=cf.proc_name
                    where cf.is_active=1                  
                ) as source
                on target.proc_name = source.proc_name
                when matched then
                update set target.proc_run_id=source.proc_run_id
                        ,target.proc_status=source.proc_status
                        ,target.run_mode=source.run_mode
                        ,target.running_dt=source.running_dt
                        ,target.start_dt=source.start_dt
                        ,target.end_dt=source.end_dt
                        ,target.start_proc_dtm=source.start_proc_dtm
                        ,target.upd_proc_dtm=source.ins_proc_dtm
                when not matched then
                insert (proc_name, proc_run_id, proc_status,run_mode,running_dt,start_dt,end_dt,start_proc_dtm,ins_proc_dtm)
                values (source.proc_name, source.proc_run_id, source.proc_status,source.run_mode,source.running_dt,source.start_dt,source.end_dt,source.start_proc_dtm,source.ins_proc_dtm);
                """.format(self.proc_name,self.proc_run_id,"Running",self.run_mode,self.run_start_dt,self.run_end_dt)
            _df = spark.sql(_strQuery)            
        except Exception as e:
            raise 

    def get_process_date(self):
        try:
            _strQuery="""select st.proc_name
                            ,st.proc_run_id
                            ,st.proc_status
                            ,st.run_mode   
                            ,case when cf.data_freq='D' then date_add(st.running_dt,prm.loop_no)  
                                when cf.data_freq='M' then last_day(add_months(st.running_dt,prm.loop_no)) 
                            end running_dt
                            ,st.start_dt
                            ,st.end_dt
                            ,cf.notebook_path
                        from cntrlfw.cntrl_proc_status st                    
                        inner join cntrlfw.cntrl_config_proc cf on st.proc_name=cf.proc_name
                        inner join (select  {} loop_no
                                    ) prm 
                                    on 1=1
                        where st.proc_name='{}' and st.proc_status='{}'
                    """.format(self.loop_no,self.proc_name,"Running")
            _df = spark.sql(_strQuery) 
    
            if _df.count()>0:
                _df_first=_df.first()  
                self.proc_status = _df_first['proc_status']
                self.proc_notebook_path = _df_first['notebook_path']
                self.run_mode=_df_first['run_mode']
                self.running_dt = _df_first['running_dt']
                self.run_start_dt = _df_first['start_dt']
                self.run_end_dt = _df_first['end_dt']

                if (self.running_dt<=self.run_end_dt):

                    # update ctl_proc_status
                    if self.loop_no==1:
                        _strUpdate ="""update cntrlfw.cntrl_proc_status set proc_status='{}',running_dt='{}',upd_proc_dtm=dateadd(hour , 7, current_timestamp())
                                            where proc_name='{}'
                                    """.format("Running",self.running_dt,self.proc_name)
                        _dfstatus = spark.sql(_strUpdate)

                    # insert ctl_proc_log
                    _strInsertHist="""insert into cntrlfw.cntrl_proc_log (proc_name,proc_run_id,proc_status,run_mode,running_dt,start_dt,end_dt,start_proc_dtm)
                        values('{}','{}','{}','{}','{}','{}','{}',dateadd(hour , 7, current_timestamp()));
                        """.format(self.proc_name,self.proc_run_id,"Running",self.run_mode,self.running_dt,self.run_start_dt,self.run_end_dt)
                    _dfLog = spark.sql(_strInsertHist)  
                else:
                    self.loop_no=-1 # end process

        except Exception as e:
            raise 

    def update_proc_log(self):
        if convert_empty_to_none(self._proc_sub_message) is None:                    
            self.__proc_sub_message='null'
        else:
            _new_error_msg=re.sub(r'[^\w\s/:#]', "", str(self._proc_sub_message))
            self.__proc_sub_message=f"'{_new_error_msg}'"

        _strUpdate ="""update cntrlfw.cntrl_proc_log set proc_status='{}',end_proc_dtm=dateadd(hour , 7, current_timestamp()),error_message={}
                    where proc_name='{}' and proc_run_id='{}' and running_dt='{}' and proc_status='Running'
               """.format(self._proc_sub_status,self.__proc_sub_message,self.proc_name,self.proc_run_id,self.running_dt)
        process_df = spark.sql(_strUpdate)

    def update_proc_status(self):
        if convert_empty_to_none(self.running_dt) is None:                    
            self.__running_dt='null'
            self.__end_proc_dtm='dateadd(hour , 7, current_timestamp())'
        else:
            self.__running_dt=f"'{self.running_dt}'"
            self.__end_proc_dtm='null'

        _strUpdate ="""update cntrlfw.cntrl_proc_status set proc_status='{}',running_dt={},end_proc_dtm={},upd_proc_dtm=dateadd(hour , 7, current_timestamp())
                    where proc_name='{}'
               """.format(self.proc_status,self.__running_dt,self.__end_proc_dtm,self.proc_name)
        _df = spark.sql(_strUpdate)

    def run_process(self):
        try:          
            json_run_result=dbutils.notebook.run(self.proc_notebook_path, 3600,{"proc_name": self.proc_name,"proc_run_id": self.proc_run_id, "data_dt": self.running_dt})
            result_list =json.loads(json_run_result)
            self._proc_sub_status=result_list['status']
            self._proc_sub_message=result_list['message']
        except Exception as e:
            self._proc_sub_status="Failed"
            self._proc_sub_message=e


# COMMAND ----------

# main process
try:
    print(f"=> start process. : {proc_name}")
    print(f"=> 1. assign wf parameter.")
    obj_process=process(proc_name,proc_run_id,run_mode,start_dt,end_dt)

    print(f"=> 2. set working catalog and schema.")
    obj_process.set_working_catalog_and_schema("poc_stg","cntrlfw")

    print(f"=> 3. validate workflow parameter.")
    is_param_valid= obj_process.validate_wf_parameter()
    
    if is_param_valid is None : 

        print(f"=> 4. calcuate process date.")
        obj_process.cal_process_date()  
        
        print(f"=> 5. loop running start.")
        while  obj_process.loop_no!=-1:             
            obj_process.get_process_date()
            print(f"=> 5.1 running_dt: {obj_process.running_dt}.")

            if obj_process.loop_no==-1:                        
                obj_process.running_dt=None
                obj_process.proc_status="Success"
                print(f"=> 5.2 loop running finished : exit loop.")        
                break #exit loop
            else:
                print(f"=> 5.3 run transform process.")  
                obj_process.run_process()
                print(f"=> 5.4 update process log.") 
                obj_process.update_proc_log()  

                if obj_process._proc_sub_status=="Failed":
                    obj_process.proc_status="Failed"
                    obj_process.proc_message=obj_process._proc_sub_message
                    print(f"=> 5.5 exit loop: process failed.")  
                    break #exit loop   

            obj_process.loop_no=1 #keep running

        print(f"=> 6. exit loop : update process status.")  
        obj_process.update_proc_status()   
        if  obj_process.proc_status=="Failed":
            #raise cmn_function.CustomException(f'Process Failed! :{obj_process.proc_message}')
            raise SystemError(f'Process Failed! :{obj_process.proc_message}')
       
        print(f"=> end process. : {proc_name}")

except Exception as err:
        raise
