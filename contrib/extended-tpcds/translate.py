from pathlib import Path
import json,sys,sqlglot
from sqlglot import exp
root=Path(sys.argv[1]);sys.path.insert(0,str(root/'src/workloads/extended-tpcds'))
import pipeline
runner=Path(sys.argv[3])
plan=Path(sys.argv[2])
configuration=json.loads((plan/'manifest.json').read_text())['source_configuration']
def constant(node):
 if isinstance(node,exp.Paren):return constant(node.this)
 if isinstance(node,exp.Neg):return -constant(node.this)
 if isinstance(node,exp.Literal):return int(node.this)
 raise ValueError('unexpected interval '+repr(node))
def translate(node):
 if isinstance(node,exp.Interval):
  units={'YEAR':0,'MONTH':1,'WEEK':2,'DAY':3,'HOUR':4,'MINUTE':5,'SECOND':6}
  try:value=exp.Literal.number(constant(node.this))
  except ValueError:value=node.this.copy()
  args=[exp.Literal.number(0) for _ in range(7)];args[units[node.args['unit'].name.upper()]]=value
  return exp.Anonymous(this='make_interval',expressions=args)
 if isinstance(node,exp.IgnoreNulls) and isinstance(node.this,exp.FirstValue):
  return exp.Anonymous(this='first_value',expressions=[node.this.this.copy(),exp.Boolean(this=True)])
 if isinstance(node,exp.StrPosition):
  assert node.args.get('position') is None,node
  return exp.Anonymous(this='instr',expressions=[node.this.copy(),node.args['substr'].copy()])
 if isinstance(node,exp.DataType) and node.this==exp.DataType.Type.TIMESTAMPNTZ:return exp.DataType.build('TIMESTAMP')
 if isinstance(node,exp.Anonymous) and node.name.upper()=='TRY_STRPTIME':
  value,format=node.expressions
  if not isinstance(format,exp.Literal) or format.this!='%Y%m%d':raise ValueError('unsupported date format')
  # Generator date keys are fixed-width YYYYMMDD (and month keys YYYYMM).
  # Preserve NULL-on-invalid parsing without an opaque UDF or unsupported
  # compiler-side Spark function. UTC is fixed in the Spark session.
  text=value.sql(dialect='spark')
  return sqlglot.parse_one(f"CASE WHEN LENGTH({text})=8 THEN TRY_CAST(CONCAT(SUBSTRING({text},1,4),'-',SUBSTRING({text},5,2),'-',SUBSTRING({text},7,2)) AS TIMESTAMP) ELSE NULL END",read='spark')
 return node
models=[]
for name in pipeline.selected_models():
 s=pipeline.render_model(name,configuration)
 expression=sqlglot.parse_one(s,read='duckdb').transform(translate)
 t=expression.sql(dialect='spark',unsupported_level=sqlglot.ErrorLevel.RAISE)
 assert 'TRY_STRPTIME(' not in t and 'TIMESTAMP_NTZ' not in t,(name,t)
 models.append({'name':name,'duckdb_sql':s,'sql':t})
(runner/'models.json').write_text(json.dumps(models,indent=2)+'\n');print('Translated',len(models),'models')
