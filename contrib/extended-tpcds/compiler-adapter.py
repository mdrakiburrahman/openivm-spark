#!/usr/bin/env python3
import os,sys,subprocess,uuid,re,json,shutil
from pathlib import Path
sql=sys.stdin.read();view=re.search(r'CREATE OR REPLACE MATERIALIZED VIEW (\w+) AS',sql).group(1)
key='__openivm_mv_'+bytes('memory','utf8').hex()+'_'+bytes('main','utf8').hex()+'_'+view.encode().hex()+'_'
sql=re.sub(r"LOAD '[^']*openivm.duckdb_extension';",'',sql)
sql=sql.replace('SELECT * FROM openivm_compile_with_facts',"UPDATE openivm_views SET published_query=NULL;\nSELECT * FROM openivm_compile_with_facts")
r=subprocess.run([os.environ['EXTENDED_TPCDS_NATIVE_CLI'],'-unsigned','-init',os.environ['EXTENDED_TPCDS_COMPILER_INIT'],*sys.argv[1:]],input=sql,text=True,capture_output=True,timeout=270)
def normalize(s):
 s=s.replace(key,view)
 # Native 91 qualifies its private MV state; the Spark rewriter owns the actual Delta locations.
 s=re.sub(r'(?i)(?:`memory`\.`main`\.|memory\.main\.)(?:`(openivm_[A-Za-z0-9_]+)`|(openivm_[A-Za-z0-9_]+))',lambda m:m[1]or m[2],s)
 s=re.sub(r'`(openivm_[A-Za-z0-9_]+)`',r'\1',s)
 return s
m=re.search(r"SET openivm_files_path='([^']+)'",sql)
if m:
 for f in list(Path(m[1]).glob('*.sql')):
  f.with_name(f.name.replace(key,view)).write_text(normalize(f.read_text()))
trace=os.environ.get('EXTENDED_TPCDS_COMPILE_TRACE')
if trace:
 d=Path(trace);d.mkdir(exist_ok=True);name=uuid.uuid4().hex;(d/(name+'.input.sql')).write_text(sql);(d/(name+'.stderr')).write_text(r.stderr)
out=[]
for line in r.stdout.splitlines():
 try:
  v=json.loads(line)
  if 'sql'in v:v['sql']=normalize(v['sql'])
  out.append(json.dumps(v))
 except:out.append(line)
print('\n'.join(out));sys.stderr.write(r.stderr)
if trace:(d/(name+'.output.json')).write_text('\n'.join(out))
sys.exit(r.returncode)
