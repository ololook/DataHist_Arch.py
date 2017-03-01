#encoding: utf-8
__author__ ='zhangyuanxiang'
from optparse import OptionParser
import os
import cx_Oracle
import sys
import datetime
from datetime import timedelta
from datetime import datetime
reload(sys )
sys.setdefaultencoding('utf8')
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.AL32UTF8'
def get_cli_options():
    parser = OptionParser(usage="usage: python %prog [options]",description="""Oracle history data archive""")
    parser.add_option("-H", "--f_dsn",dest="f_dsn",
                      default="127.0.0.1:3306:db:table",
                      metavar="host:port:db:table"
                      )
    parser.add_option("-L", "--t_dsn",dest="t_dsn",
                      default="127.0.0.1:3306:db:table",
                      metavar="host:port:db:table"
                     )
    parser.add_option("-S", "--f_sid",dest="f_sid",
                      default="orcl",
                      metavar="orcl"
                     )
    parser.add_option("-d", "--t_sid",dest="t_sid",
                      default="orcl",
                      metavar="orcl"
                     )
    parser.add_option("-w", "--col",dest="col",
                      default="where col",
                      metavar="where col"
                     )
    parser.add_option("-v", "--d",dest="days",
                      default="31",
                      metavar="31"
                     )
    (options, args) = parser.parse_args()
    return options

class pub_parameter(object):
      def __init__(self):
        pass 
      @staticmethod
      def parse_date(value):
        curtdate=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        pastdate=(datetime.now()-timedelta(days=value)).strftime('%Y%m%d%H%M%S')
        return pastdate
      @classmethod
      def parse_options(cls):
        options=get_cli_options()
        source=options.f_dsn
        destin=options.t_dsn
        f_sid =options.f_sid
        t_sid =options.t_sid
        f_c   =options.col
        f_d   =pub_parameter.parse_date(int(options.days))          
        return source,f_sid,f_d,f_c,destin,t_sid
      
class sour_inst(object):
      def __init__ (self,host,port,dbname,tablename,sid,colname,dt):
         self.host=host
         self.port=port
         self.db  =dbname
         self.tb  =tablename
         self.col =colname
         self.sid =sid
         self.dt  =dt
      def dict_sql(self):
          sqls="""SELECT cols.column_name,cols.position 
                FROM all_constraints cons, all_cons_columns cols
                WHERE cols.table_name = upper('%s')
                AND cons.constraint_type = 'P'
                AND cons.constraint_name = cols.constraint_name
                AND cons.owner =upper('%s')
                ORDER BY cols.table_name, cols.position""" %(self.tb,self.db)
          return sqls
      def Get_con  (self):
          try:
             dsn_tns =cx_Oracle.makedsn(self.host,self.port,self.sid)
             conn = cx_Oracle.connect('username','passwd',dsn_tns)
          except cx_Oracle.DatabaseError as e:
              print "init connect source database .... ",e
          return conn
      def Is_pri   (self):
          conn=self.Get_con()
          cur=conn.cursor()
          sql=self.dict_sql()
          try:
            cur.execute(sql)
            cur.fetchall()
          except cx_Oracle.DatabaseError as e:
            print "execute sql get primary key is error ",e
          #print cur.rowcount 
          if cur.rowcount==0:
             print "no primary key "
             exit()
          elif cur.rowcount==1:
              conn.close()
              print "primary check pass!!!"
          else:
              print  "not support primary key"
      def out_data (self):
          sqlcmd="select * from %s.%s  where %s<'%s'" %(self.db,self.tb,self.col,self.dt)
          conn=self.Get_con()
          cur=conn.cursor()
          try:
            pass
            cur.execute(sqlcmd)
          except cx_Oracle.DatabaseError as e:
            print "fetch table rows ",e
          rows=cur.fetchmany(500)
          while rows:
                yield rows
                rows=cur.fetchmany(500)
          conn.close()
      @classmethod
      def del_pri  (self,host,port,dbname,tablename,sid,rowin):
          sqlcmd="""SELECT cols.column_name,cols.position 
                FROM all_constraints cons, all_cons_columns cols
                WHERE cols.table_name = upper('%s')
                AND cons.constraint_type = 'P'
                AND cons.constraint_name = cols.constraint_name
                AND cons.owner =upper('%s')
                ORDER BY cols.table_name, cols.position""" %(tablename,dbname)
          try:
             dsn_tns =cx_Oracle.makedsn(host,port,sid)
             conn = cx_Oracle.connect('username','passwd',dsn_tns)
          except cx_Oracle.DatabaseError as e:
              print "init connect source database .... ",e 
          cursor=conn.cursor()
          duparry=[]
          dict={}
          try:
             cursor.execute(sqlcmd)
          except cx_Oracle.DatabaseError as e:
             print "gen delete sql is error ",e
          rows=cursor.fetchall()
          del_sql="""delete from %s.%s where 1=1 """ %(dbname,tablename)
          for r in rows:
              for (name, value) in zip([r[0]],[r[1]]):
                  dict[name] = value 
          for r1 in rowin:
              for col,val in  dict.items():
                  i=+1
                  del_sql+=" and %s=:1" %(col)
                  duparry.append(r1[val-1])
                  exe_sql= del_sql
                  del_sql="""delete from %s.%s where 1=1 """ %(dbname,tablename)
          cursor.prepare(exe_sql)
          cursor.executemany(None, [(v,) for v in duparry])
          cursor.execute('commit')
          duparry=[]
          conn.close()

class dest_inst(object):
      def __init__ (self,host,port,dbname,tablename,sid,s_host,s_port,s_db,s_tb,s_sid):
         self.host      =host
         self.port      =port
         self.dbname    =dbname
         self.tablename =tablename
         self.sid       =sid
         self.s_host    =s_host
         self.s_port    =s_port
         self.s_db      =s_db
         self.s_tb      =s_tb
         self.s_sid     =s_sid
      def Get_con  (self):
         try:
             dsn_tns =cx_Oracle.makedsn(self.host,self.port,self.sid)
             conn = cx_Oracle.connect('username','passwd',dsn_tns)
         except cx_Oracle.DatabaseError as e:
             print "init connect source database .... ",e
         return conn
      
      def Gen_batch  (self):
           conn=self.Get_con()
           cursor=conn.cursor()
           row='('
           ncol='(:1'
           sql_col="select * from %s.%s WHERE 1=2" %(self.dbname,self.tablename)
           insert="insert into %s.%s" %(self.dbname,self.tablename)
           cursor.execute(sql_col)
           data = cursor.fetchall()
           for i in range(0, len(cursor.description)):
              if i==0:
                 row =row+cursor.description[i][0]
                 ncol=ncol
              else:
                 row +=','+cursor.description[i][0]
                 i=i+1
                 ncol +=',:'+str(i)
          
           insert_table=insert+row+' '+')'+' '+'values'+' '+ncol+')'
           conn.close()
           return insert_table
      def insert_row(self,row):
           rowlist=[]
           dellist=[]
           sqlcmd=self.Gen_batch()
           conn=self.Get_con()
           cursor=conn.cursor()
           for r1 in row:
               for rows in r1:
                   rowlist.append(rows)
               try:
                 cursor.prepare(sqlcmd)
                 cursor.executemany(None,rowlist)
               except cx_Oracle.DatabaseError as e:
                 print "insert data error" ,e
                 cursor.execute('rollback')
               dellist=rowlist
               rowlist=[]
               cursor.execute('commit')
               sour_inst.del_pri(self.s_host,self.port,self.s_db,self.s_tb,self.s_sid,dellist)
               dellist=[]
           conn.close()

class  action(object):
     def __init__(self,type):
         self.type=type
     def pares_para(self):
         source_connectstring,source_sid,source_day,source_colname,\
         destination_connectstring,destination_sid=pub_parameter.parse_options()
         source_IP,source_port,source_dbname,source_table=source_connectstring.split(":")
         destination_host,destination_port,destination_dbname,\
         destination_table=destination_connectstring.split(":")
         if self.type.lower()=='source':
            return source_IP,source_port,source_dbname,\
                   source_table,source_sid,source_colname,source_day
         elif self.type.lower()=='destination':
            return destination_host,destination_port,\
                   destination_dbname,destination_table,destination_sid
def main():
     get_cli_options()
     source=action('source')
     destina=action('destination')
     sp1,sp2,sp3,sp4,sp5,sp6,sp7=source.pares_para()
     dp1,dp2,dp3,dp4,dp5=destina.pares_para()
     ss=sour_inst(sp1,sp2,sp3,sp4,sp5,sp6,sp7) 
     dd=dest_inst(dp1,dp2,dp3,dp4,dp5,sp1,sp2,sp3,sp4,sp5)
     ss.Is_pri()
     dd.insert_row(ss.out_data())
if __name__ == '__main__':
     main()
	 
	 使用方法：
	 python DataHist_Arch.py --f_dsn="IP:port:dbname:tablename" --t_dsn="IP:port:dbname:tablename" --f_sid="Oracle_SID" --t_sid="Oracle_SID" --col="colname" --d="天数"
	 迁移删除规则:
	 where colname<=colname-天数