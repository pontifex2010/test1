create or replace PROCEDURE EXPORT_ESQUEMA AS 
comanda varchar2(200);
source_schema varchar2(50);
ind NUMBER;
h1 NUMBER;
percent_done NUMBER;
job_state VARCHAR2(30);
le ku$_LogEntry;
js ku$_JobStatus;
jd ku$_JobDesc;
sts ku$_Status;
fecha VARCHAR2(80);
q varchar2(1) := chr(39); -- single quote


BEGIN

/* definir esquema origen: source_schema */
/* utilitzo SYS_CONTEXT:  http://download.oracle.com/docs/cd/B12037_01/server.101/b10759/functions150.htm */
comanda := 'SELECT SYS_CONTEXT (''USERENV'',''SESSION_USER'') FROM DUAL';
execute immediate comanda into source_schema;

/* definir canal export, job i fitxers */
--fecha := to_char(sysdate,'YYYYMMDDHH24MISS_');
h1 := DBMS_DATAPUMP.OPEN('EXPORT','SCHEMA', NULL,'EXPORT_' || source_schema,'LATEST');
DBMS_DATAPUMP.ADD_FILE(handle => h1, filename => source_schema || '.dmp', directory => 'DATA_PUMP_DIR', reusefile => 1);
DBMS_DATAPUMP.ADD_FILE(handle => h1, filename => source_schema ||'_export.log', directory => 'DATA_PUMP_LOG', filetype => 3);


/* make any data copied consistent with respect to now */
dbms_datapump.set_parameter(h1, 'FLASHBACK_SCN', sys.dbms_flashback.get_system_change_number);

/* restringir a esquema que volem exportar */
/* DBMS_DATAPUMP.METADATA_FILTER(h1,'SCHEMA_EXPR','IN (''source_schema'')'); */
DBMS_DATAPUMP.METADATA_FILTER (h1, 'SCHEMA_LIST', q||source_schema||q );

/* set parallelism */
dbms_datapump.set_parallel(h1, 4);

/* començar job */
DBMS_DATAPUMP.START_JOB(h1);


/* control estat job */
percent_done := 0;
job_state := 'UNDEFINED';
while (job_state != 'COMPLETED') and (job_state != 'STOPPED') loop
dbms_datapump.get_status(h1, dbms_datapump.ku$_status_job_error + dbms_datapump.ku$_status_job_status + dbms_datapump.ku$_status_wip,-1,job_state,sts);

js := sts.job_status;
if js.percent_done != percent_done then
dbms_output.put_line('** Porcentaje Completado = ' || to_char(js.percent_done));
percent_done := js.percent_done;
end if;

if (bitand(sts.mask,dbms_datapump.ku$_status_wip) != 0) then
le := sts.wip;
else
if (bitand(sts.mask,dbms_datapump.ku$_status_job_error) != 0) then
le := sts.error;
else
le := null;
end if;
end if;
if le is not null then
ind := le.FIRST;
while ind is not null loop
dbms_output.put_line(le(ind).LogText);

ind := le.NEXT(ind);
end loop;
end if;
end loop;

dbms_output.put_line('Job Completado');
dbms_output.put_line('Estado final del Job = ' || job_state);

/* tancar canal export */
dbms_datapump.detach(h1);

END EXPORT_ESQUEMA;
