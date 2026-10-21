import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.openivm.spark.common.*;

public class ExtendedTpcdsSpark {
  static SparkSession spark;
  static ObjectMapper mapper=new ObjectMapper();
  static ObjectNode result=mapper.createObjectNode();
  static ArrayNode statements=result.putArray("statements"),checks=result.putArray("checks");
  static Path output;
  static Map<String,String> queries=new HashMap<>();
  static final String monthly="fact_engagement_status_monthly_snapshot";
  static final boolean monthlyProbe=Boolean.getBoolean("extendedtpcds.monthlyProbe");
  static String phase="startup";
  static ScheduledExecutorService timers=Executors.newSingleThreadScheduledExecutor(r->{Thread t=new Thread(r);t.setDaemon(true);return t;});
  static void save() throws Exception { mapper.writerWithDefaultPrettyPrinter().writeValue(output.toFile(),result); }
  static List<Row> execute(String label,String sql) throws Exception {
    System.out.println("BEGIN "+label);System.out.flush();
    long start=System.nanoTime();
    ScheduledFuture<?> deadline=timers.schedule(()->{System.err.println("STATEMENT TIMEOUT "+label);Runtime.getRuntime().halt(124);},1800,TimeUnit.SECONDS);
    try {Dataset<Row> df=spark.sql(sql);List<Row> rows=df.collectAsList();
      ObjectNode item=statements.addObject();item.put("label",label);item.put("duration_s",(System.nanoTime()-start)/1e9);save();
      System.out.println("END "+label);System.out.flush();return rows;
    } finally { deadline.cancel(false); }
  }
  static String literal(Path path){return "'"+path.toString().replace("'","''")+"'";}
  static String normalized(StructType schema,String relation) {
    ArrayList<String> columns=new ArrayList<>();
    for(StructField f:schema.fields()) {String c="`"+f.name().replace("`","``")+"`";
      if(f.dataType().equals(DataTypes.DoubleType)||f.dataType().equals(DataTypes.FloatType)) c="round(cast("+c+" as double),10) AS "+c;
      columns.add(c);
    }
    return "SELECT "+String.join(",",columns)+" FROM "+relation;
  }
  static void validate(String name,int batch,Path references) throws Exception {
    String path=references.resolve("batch"+batch).resolve(name+".parquet").toString();
    String actual=normalized(spark.table(name).schema(),name);
    String expected=normalized(spark.read().parquet(path).schema(),"parquet.`"+path+"`");
    String query="SELECT count(*) AS differences FROM (("+actual+" EXCEPT ALL "+expected+") UNION ALL ("+expected+" EXCEPT ALL "+actual+"))";
    long differences=execute("validate:"+batch+":"+name,query).get(0).getLong(0);
    ObjectNode check=checks.addObject();check.put("batch",batch);check.put("model",name);check.put("differences",differences);save();
    if(differences!=0 || (monthlyProbe && name.equals(monthly))) {
      diagnose(name,batch,references,actual,expected);
      if(monthlyProbe && name.equals(monthly)) return;
      throw new IllegalStateException(name+" has "+differences+" bag differences against DuckDB reference");
    }
  }
  static List<JsonNode> capture(String label,String query) throws Exception {
    List<JsonNode> rows=new ArrayList<>();
    for(Row row:execute(label,"SELECT to_json(struct(t.*)) FROM ("+query+") t LIMIT 10000"))
      rows.add(mapper.readTree(row.getString(0)));
    mapper.writerWithDefaultPrettyPrinter().writeValue(output.resolveSibling(label+".json").toFile(),rows);
    return rows;
  }
  static void diagnose(String name,int batch,Path references,String actual,String expected) throws Exception {
    List<JsonNode> bad=new ArrayList<>();
    bad.addAll(capture("actual-only",actual+" EXCEPT ALL "+expected));
    bad.addAll(capture("expected-only",expected+" EXCEPT ALL "+actual));
    String fresh=normalized(spark.sql(queries.get(name)).schema(),"("+queries.get(name)+") fresh");
    capture("fresh-only",fresh+" EXCEPT ALL "+expected);
    capture("expected-minus-fresh",expected+" EXCEPT ALL "+fresh);
    if(!name.equals(monthly)) return;
    int finalSelect=queries.get(name).lastIndexOf("SELECT TRY_CAST(ROW_NUMBER()");
    if(finalSelect<0)throw new IllegalStateException("Unexpected monthly query shape");
    String ctes=queries.get(name).substring(0,finalSelect);
    List<JsonNode> near=capture("near-half-aggregates",ctes+
        "SELECT * FROM agg WHERE ABS(effective_units_count-FLOOR(effective_units_count)-0.5)<1e-8");
    if(bad.isEmpty())bad.addAll(near.subList(0,Math.min(near.size(),8)));
    String parent="int_engagement_status_transaction";
    String duckParent="parquet.`"+references.resolve("batch"+batch).resolve(parent+".parquet")+"`";
    String duckSql=queries.get(name).replace(parent,duckParent);
    String duckFresh=normalized(spark.sql(duckSql).schema(),"("+duckSql+") fresh");
    capture("duck-parent-fresh-only",duckFresh+" EXCEPT ALL "+expected);
    capture("expected-minus-duck-parent-fresh",expected+" EXCEPT ALL "+duckFresh);
    String[] keys={"household_key","account_offer_key","account_usage_key","platform_key","preference_key",
                   "product_tier_key","product_version_key","engagement_type_key","mode_key"};
    Set<String> groups=new LinkedHashSet<>();
    for(JsonNode row:bad) {
      List<String> terms=new ArrayList<>();
      terms.add("year(event_time)*10000+month(event_time)*100+1="+row.get("dim_date_key").asInt());
      for(String key:keys)terms.add(key+" <=> "+(row.get(key).isNull()?"NULL":row.get(key).asText()));
      groups.add("("+String.join(" AND ",terms)+")");
    }
    if(groups.isEmpty())return;
    String predicate=String.join(" OR ",groups);
    capture("spark-parent-rows","SELECT * FROM "+parent+" WHERE "+predicate);
    capture("duck-parent-rows","SELECT * FROM "+duckParent+" WHERE "+predicate);
    // The canonical query has a final SELECT after the agg CTE. Keep the
    // unrounded aggregate so a half-unit boundary can be examined directly.
    if(finalSelect>=0) {
      capture("spark-unrounded",ctes+"SELECT * FROM agg WHERE dim_date_key IN ("+
          String.join(",",bad.stream().map(row->row.get("dim_date_key").asText()).distinct().toList())+") AND household_key IN ("+
          String.join(",",bad.stream().map(row->row.get("household_key").asText()).distinct().toList())+")");
    }
  }
  static void runMonthlyProbe(Path references,Path candidateReferences) throws Exception {
    String original=queries.get(monthly);
    String candidate=original.replace("SUM(effective_units)","SUM(CAST(effective_units AS DECIMAL(28,10)))");
    if(original.equals(candidate))throw new IllegalStateException("Missing monthly double aggregate");
    String fixedName=monthly+"_fixed";
    queries.put(fixedName,candidate);
    execute("create:decimal-candidate","CREATE MATERIALIZED VIEW "+fixedName+" AS "+candidate);
    requireIncremental(fixedName,MvCatalog.lookup(spark,new TableIdentifier(fixedName,scala.Option.empty())).get().refreshTypeName());
    validate(fixedName,1,candidateReferences);
    spark.conf().set("spark.sql.adaptive.enabled","false");
    ArrayNode probes=result.putArray("precision_probes");
    boolean captured=false;
    for(int partitions:new int[]{1,2,3,4,8}) {
      spark.conf().set("spark.sql.shuffle.partitions",Integer.toString(partitions));
      for(boolean decimal:new boolean[]{false,true}) {
        String label=(decimal?"decimal":"double")+"-partitions-"+partitions;
        String sql=decimal?candidate:original;
        String reference=(decimal?candidateReferences:references).resolve("batch1").resolve((decimal?fixedName:monthly)+".parquet").toString();
        String actual=normalized(spark.sql(sql).schema(),"("+sql+") fresh");
        String expected=normalized(spark.read().parquet(reference).schema(),"parquet.`"+reference+"`");
        String diff="("+actual+" EXCEPT ALL "+expected+") UNION ALL ("+expected+" EXCEPT ALL "+actual+")";
        long count=execute(label,"SELECT count(*) FROM ("+diff+") d").get(0).getLong(0);
        probes.addObject().put("kind",decimal?"decimal":"double").put("partitions",partitions).put("differences",count);save();
        if(count!=0) {
          capture(label+"-differences",diff);
          if(!decimal && !captured) {
            result.put("witness_partitions",partitions);save();
            diagnose(monthly,1,references,actual,expected);
            captured=true;
          }
        }
        if(decimal && count!=0)throw new IllegalStateException("Fixed precision differs for "+partitions+" partitions");
      }
    }
    result.put("status","diagnostic_complete");
    result.put("scope","Initial monthly query and its ancestors only; not a full SF1 benchmark pass");
  }
  static void requireIncremental(String name,String refreshType) {
    if(refreshType.equals("FULL_REFRESH") && !name.equals("meta_ingestion"))
      throw new IllegalStateException("OpenIVM benchmark refused FULL_REFRESH fallback for "+name);
  }
  public static void main(String[] args) throws Exception {
    Path out=Path.of(args[0]),plan=Path.of(args[1]),modelsPath=Path.of(args[2]),references=Path.of(args[3]);
    output=out.resolve("spark-result.json");
    JsonNode manifest=mapper.readTree(plan.resolve("manifest.json").toFile()),models=mapper.readTree(modelsPath.toFile());
    for(JsonNode model:models)queries.put(model.get("name").asText(),model.get("sql").asText());
    result.put("status","running");result.put("plan_id",manifest.get("plan_id").asText());result.put("scale_factor",manifest.at("/source_configuration/base_scale_factor").asDouble());result.put("spark_commit",System.getProperty("extendedtpcds.sparkCommit","54859a476528927f84d777a1f7099626ba644b56"));
    result.put("validation","DuckDB full DAG, bidirectional EXCEPT ALL, floating columns rounded to 10 decimal places");save();
    try {
      spark=SparkSession.builder().master("local["+Integer.getInteger("extendedtpcds.workers",8)+"]").appName("extended-tpcds-openivm-spark")
        .config("spark.ui.enabled","false").config("spark.driver.host","127.0.0.1").config("spark.driver.bindAddress","127.0.0.1")
        .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions")
        .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.openivm.enabled","true").config("spark.openivm.changeFeed.mode","cdf")
        .config("spark.openivm.profile.refresh","true").config("spark.openivm.rocksdb.multiProcess","false")
        .config("spark.sql.legacy.interval.enabled","true").config("spark.sql.parquet.inferTimestampNTZ.enabled","false").config("spark.sql.session.timeZone","UTC").config("spark.sql.shuffle.partitions","8")
        .config("spark.sql.ui.retainedExecutions","5").config("spark.ui.retainedJobs","10").config("spark.ui.retainedStages","20").config("spark.default.parallelism","8").config("spark.databricks.delta.snapshotPartitions","4")
        .config("spark.sql.warehouse.dir",out.resolve("warehouse").toString()).config("spark.local.dir",out.resolve("spill").toString())
        .config("spark.sql.autoBroadcastJoinThreshold","10485760").config("spark.driver.maxResultSize","256m")
        .config("spark.databricks.delta.merge.materializeSource",System.getProperty("extendedtpcds.materializeSource","auto")).getOrCreate();
      spark.sparkContext().setLogLevel("ERROR");
      phase="source_loading";
      var initial=manifest.get("initial").fields();
      while(initial.hasNext()) {var entry=initial.next();String name=entry.getKey();String p=plan.resolve(entry.getValue().get("path").asText()).toString();
        execute("load:"+name,"CREATE TABLE "+name+" USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed'='true') AS SELECT * FROM parquet.`"+p+"`");}
      phase="initial_materialization";
      for(JsonNode model:models) {String name=model.get("name").asText();
        execute("create:"+name,"CREATE MATERIALIZED VIEW "+name+" AS "+model.get("sql").asText());
        MvMetadata meta=MvCatalog.lookup(spark,new TableIdentifier(name,scala.Option.empty())).get();
        result.withObject("/refresh_types").put(name,meta.refreshTypeName());save();
        requireIncremental(name, meta.refreshTypeName());
        validate(name,1,references);
      }
      if(monthlyProbe) {runMonthlyProbe(references,Path.of(args[4]));return;}
      int batch=1;
      for(JsonNode step:manifest.get("batches")) {batch++;phase="mutations:"+batch;
        for(JsonNode op:step.get("operations")) {String name=op.get("table").asText(),kind=op.get("kind").asText();
          JsonNode image=kind.equals("delete")?op.get("before"):op.get("after");String p=plan.resolve(image.get("path").asText()).toString();
          if(kind.equals("insert")) execute("insert:"+batch+":"+name,"INSERT INTO "+name+" SELECT * FROM parquet.`"+p+"`");
          else if(kind.equals("delete")) execute("delete:"+batch+":"+name,"MERGE INTO "+name+" t USING parquet.`"+p+"` s ON t._source_row_id=s._source_row_id WHEN MATCHED THEN DELETE");
          else execute("update:"+batch+":"+name,"MERGE INTO "+name+" t USING parquet.`"+p+"` s ON t._source_row_id=s._source_row_id WHEN MATCHED THEN UPDATE SET *");
        }
        phase="refresh:"+batch;
        for(JsonNode model:models) {String name=model.get("name").asText();execute("refresh:"+batch+":"+name,"REFRESH MATERIALIZED VIEW "+name);
          String refreshType=MvCatalog.lookup(spark,new TableIdentifier(name,scala.Option.empty())).get().refreshTypeName();
          result.withObject("/post_refresh_types/"+batch).put(name,refreshType);save();requireIncremental(name,refreshType);
          validate(name,batch,references);}
      }
      result.put("status","passed");result.put("snapshots",batch);
    } catch(Throwable error) {result.put("status","failed");result.put("phase",phase);result.put("error",error.toString());error.printStackTrace();throw error;}
    finally {save();timers.shutdownNow();if(spark!=null)spark.stop();}
  }
}
