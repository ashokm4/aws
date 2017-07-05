#!/user/bin/python
import MySQLdb as mysql
import boto3
import os, sys
import commands
client = boto3.client('s3')
#FILE_LIST=response['Contents'][1]['Key']
#mysql -u dbadmin -p -h semtest-clu.cluster-cmt97mkp9ktk.us-west-2.rds.amazonaws.com
dDir = <WORK DIRECTORY>
s3bucket = <S3 BUCKET HAVING MYSQL DUMP>
foldername = <FOLDER TO TEMPORARILY COPY THE DUMP"
destfolder=os.path.join(dDir,foldername)
cmd = '/usr/local/bin/s3cmd sync  s3://' + s3bucket + '/ ' + dDir+foldername + '/'
print cmd
os.system(cmd)
DB_HOST = <RDS ENDPOINT>
DB_USER = <DB USER>
DB_USER_PASSWORD =  '<DB PASSWORD>
COUNTFILE=<TABLE RCORD COUNT FILE>
fw=open(COUNTFILE,'w')


cnx = mysql.connect(user=DB_USER, passwd=DB_USER_PASSWORD,db='information_schema',host=DB_HOST)
cur = cnx.cursor()

def get_record_count(DB):
    cnx2 = mysql.connect(user=DB_USER, passwd=DB_USER_PASSWORD,db=DB,host=DB_HOST)
    cur1 = cnx2.cursor()

    fw.write('\n')
    fw.write("Record count for %s \n" %(DB))
    fw.write('\n')
    stmt='select table_name from information_schema.tables where table_schema=' + '\'' + DB + '\''
    cur1.execute(stmt)
    row=cur1.fetchone()
    while row is not None:
          tname=str(row[0])
          stmt2 = 'select count(*) from ' +  tname
          cur2=cnx2.cursor()
          cur2.execute(stmt2)
          count=cur2.fetchone()[0]
          fw.write("%s: %s\n" %(tname,count))
          cur2.close()
          row=cur1.fetchone()
    cur1.close()


for DUMP_FILE in os.listdir(destfolder):
     DUMP_FILE_FULL=os.path.join(destfolder,DUMP_FILE)
     k=DUMP_FILE.split('.')
     source_schema=k[0].lower()
     if source_schema == 'recordcount_ue1' or source_schema == 'recordcount_uw2' or source_schema == 'recordcount':
             continue
     elif source_schema == 'titanconfig':
             dest_schema= source_schema
     else:
             dest_schema='titan_' + source_schema

     dropdbcmd = 'drop database ' + dest_schema
     createdbcmd = 'create database ' + dest_schema
     dumpimpcmd = 'gunzip -c ' + DUMP_FILE_FULL + '| mysql  -u ' + DB_USER + ' --password=' + DB_USER_PASSWORD  + ' ' + dest_schema +  ' -h ' + DB_HOST
     try:
       cur.execute(dropdbcmd)
       cur.execute(createdbcmd)
       status,import_output=commands.getstatusoutput(dumpimpcmd)
       print import_output
       get_record_count(dest_schema)
     except:
       print "inside except block"
       e = sys.exc_info()[0]
       print( "Error: %s" % e )
       print "end of except block"
cur.close()
fw.close()

