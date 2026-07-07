# 12. TPC-DI live-state inspection: DuckDB side vs Spark side

This chapter inspects `.temp/ivm-bench/mount/results/3/spark-openivm/`. The result is a sharp contrast: Spark-side state is rich; DuckDB-side persistent state is empty.

## What the `*.db` paths are

`bronze.db/`, `silver.db/`, `gold.db/`, and `tpcdi.db/` are Spark warehouse database directories, not DuckDB database files. They are associated with Spark `CREATE DATABASE` / catalog namespaces; the benchmark metastore was external, and table data lives at explicit Delta locations such as `sources/` and `_ivm/views/`.

| path        |                      kind | visible entries |
| ----------- | ------------------------: | --------------: |
| `bronze.db` | directory=True file=False |               0 |
| `silver.db` | directory=True file=False |               0 |
| `gold.db`   | directory=True file=False |               0 |
| `tpcdi.db`  | directory=True file=False |               0 |

Prompt probe against `information_schema.tables`:

```text
bronze : ERROR: IOException IO Error: Could not read from file "/home/mdrrahman/openivm-spark/.temp/ivm-bench/mount/results/3/spark-openivm/bronze.db": Is a directory
silver : ERROR: IOException IO Error: Could not read from file "/home/mdrrahman/openivm-spark/.temp/ivm-bench/mount/results/3/spark-openivm/silver.db": Is a directory
gold : ERROR: IOException IO Error: Could not read from file "/home/mdrrahman/openivm-spark/.temp/ivm-bench/mount/results/3/spark-openivm/gold.db": Is a directory
tpcdi : ERROR: IOException IO Error: Could not read from file "/home/mdrrahman/openivm-spark/.temp/ivm-bench/mount/results/3/spark-openivm/tpcdi.db": Is a directory
```

Prompt probe for `sqlite_master WHERE name LIKE 'openivm_%'`:

```text
bronze : ERROR: IOException IO Error: Could not read from file "/home/mdrrahman/openivm-spark/.temp/ivm-bench/mount/results/3/spark-openivm/bronze.db": Is a directory
silver : ERROR: IOException IO Error: Could not read from file "/home/mdrrahman/openivm-spark/.temp/ivm-bench/mount/results/3/spark-openivm/silver.db": Is a directory
gold : ERROR: IOException IO Error: Could not read from file "/home/mdrrahman/openivm-spark/.temp/ivm-bench/mount/results/3/spark-openivm/gold.db": Is a directory
tpcdi : ERROR: IOException IO Error: Could not read from file "/home/mdrrahman/openivm-spark/.temp/ivm-bench/mount/results/3/spark-openivm/tpcdi.db": Is a directory
```

No `openivm_%` DuckDB tables exist here. The probes fail before listing tables because each path is a directory.

## Why DuckDB-side state is empty

openivm-spark uses DuckDB only as a compiler bridge. `OpenIvmCompiler` spawns `duckdb :memory: -jsonlines`, sets `openivm_compile_only=true`, runs `PRAGMA compile_refresh`, reads the emitted SQL, and exits. Nothing from that transient process is persisted into the four `*.db` paths.

Persistent state is instead Spark-managed: MV row data is Delta under `_ivm/views`; MV metadata, fingerprints, compiled SQL, and staging indexes are RocksDB under `_openivm`; source and staging row data are Delta under `sources/`, `_ivm/staging`, and `_ivm/view_deltas`.

## What is in the captured run

The `*.db` directories themselves are empty, but the logical schemas have Delta state nearby. Each table below shows its full Delta schema and the first five rows (CSV).

## Bronze tier: raw ingested data

### `bronze.brokerage_cash_transaction` — loc=`_ivm/views/bronze/brokerage_cash_transaction`; version=2; rows=3621; schema=`ct_ca_id:long, ct_dts:timestamp, ct_amt:double, ct_name:string`

ct_ca_id,ct_dts,ct_amt,ct_name
2491,2017-07-09 10:02:43+00:00,-9204.27,UhmPrvMHxBAaAqugXnssPCIKEuJeROkJB...
1324,2017-07-08 11:34:20+00:00,8913.39,omBhNguUeU VVQTMYPc GJmIDnMXJBAgN...
0,2012-07-11 08:10:42+00:00,-37215.14,TGDRsaHPherhApDuHfXUPdexIUoEzKdgR...
11,2012-07-07 17:15:11+00:00,-3178.67,PGwhaPC igAVOmHLJppGbXaDEgHSurSvr...
12,2012-07-12 17:41:41+00:00,-3172.19,uQOUlrpDGHQpeeBGxaLTrxUwMwNMFowWA...

### `bronze.brokerage_daily_market` — loc=`_ivm/views/bronze/brokerage_daily_market`; version=2; rows=12830; schema=`dm_date:date, dm_s_symb:string, dm_close:double, dm_high:double, dm_low:double, dm_vol:integer`

dm_date,dm_s_symb,dm_close,dm_high,dm_low,dm_vol
2017-07-08,AAAAAAAAAAAACCH,136.25,174.89,131.56,954467998
2017-07-07,AAAAAAAAAAAACCH,937.2,1317.08,891.53,773766563
2015-07-06,AAAAAAAAAAAABOY,242.93,284.42,185.08,111904727
2015-07-06,AAAAAAAAAAAAARP,445.46,522.3,386.48,78849320
2015-07-06,AAAAAAAAAAAACVI,910.59,1148.89,723.37,807515829

### `bronze.brokerage_holding_history` — loc=`_ivm/views/bronze/brokerage_holding_history`; version=2; rows=3626; schema=`hh_h_t_id:integer, hh_t_id:integer, hh_before_qty:integer, hh_after_qty:integer`

hh_h_t_id,hh_t_id,hh_before_qty,hh_after_qty
371146,372221,6454,0
353232,372101,5225,0
0,0,0,2939
1,1,0,9919
2,2,0,5525

### `bronze.brokerage_trade` — loc=`_ivm/views/bronze/brokerage_trade`; version=2; rows=3912; schema=`t_id:long, t_dts:timestamp, t_st_id:string, t_tt_id:string, t_is_cash:byte, t_s_symb:string, t_qty:integer, t_bid_price:double, t_ca_id:long, t_exec_name:string, t_trade_price:double, t_chrg:double, t_comm:double, t_tax:double`

t_id,t_dts,t_st_id,t_tt_id,t_is_cash,t_s_symb,t_qty,t_bid_price,t_ca_id,t_exec_name,t_trade_price,t_chrg,t_comm,t_tax
372221,2017-07-09 21:56:25+00:00,SBMT,TLS,0,AAAAAAAAAAAACPT,6454,2.97,4355,2140,NULL,NULL,NULL,NULL
372101,2017-07-08 00:57:49+00:00,SBMT,TLS,0,AAAAAAAAAAAABFV,5225,4.06,428,990,NULL,NULL,NULL,NULL
0,2012-07-07 00:02:34+00:00,CMPT,TMB,0,AAAAAAAAAAAACQP,2939,9.57,0,3160,10.02,58.95,27.31,1611.19
1,2012-07-07 00:14:35+00:00,CMPT,TMB,0,AAAAAAAAAAAABHJ,9919,1.54,11,1058,1.52,19.21,30.74,3504.75
2,2012-07-08 12:15:20+00:00,CMPT,TLB,0,AAAAAAAAAAAABDK,5525,9.12,12,3477,9.35,143.51,37.2,20526.18

### `bronze.brokerage_trade_history` — loc=`_ivm/views/bronze/brokerage_trade_history`; version=0; rows=9822; schema=`th_t_id:long, th_dts:timestamp, th_st_id:string`

th_t_id,th_dts,th_st_id
0,2012-07-07 00:01:13+00:00,SBMT
0,2012-07-07 00:02:34+00:00,CMPT
1,2012-07-07 00:09:48+00:00,SBMT
1,2012-07-07 00:14:35+00:00,CMPT
2,2012-07-07 00:10:55+00:00,PNDG

### `bronze.brokerage_watch_history` — loc=`_ivm/views/bronze/brokerage_watch_history`; version=2; rows=9003; schema=`w_c_id:long, w_s_symb:string, w_dts:timestamp, w_action:string`

w_c_id,w_s_symb,w_dts,w_action
961,AAAAAAAAAAAAALW,2017-07-09 00:00:29+00:00,ACTV
1915,AAAAAAAAAAAABTZ,2017-07-08 00:00:19+00:00,ACTV
17,AAAAAAAAAAAAAJR,2012-07-07 00:03:44+00:00,ACTV
18,AAAAAAAAAAAABSM,2012-07-07 00:04:23+00:00,ACTV
0,AAAAAAAAAAAAAQR,2012-07-07 00:09:08+00:00,ACTV

### `bronze.crm_customer_mgmt` — loc=`_ivm/views/bronze/crm_customer_mgmt`; version=2; rows=150; schema=`action_ts:timestamp, action_type:string, c_id:long, c_tax_id:string, c_gndr:string, c_tier:integer, c_dob:date, c_l_name:string, c_f_name:string, c_m_name:string, c_adline1:string, c_adline2:string, c_zipcode:string, c_city:string, c_state_prov:string, c_ctry:string, c_prim_email:string, c_alt_email:string, c_phone_1:string, c_phone_2:string, c_phone_3:string, c_lcl_tx_id:string, c_nat_tx_id:string, ca_id:long, ca_tax_st:integer, ca_b_id:long, ca_name:string`

action_ts,action_type,c_id,c_tax_id,c_gndr,c_tier,c_dob,c_l_name,c_f_name,c_m_name,c_adline1,c_adline2,c_zipcode,c_city,c_state_prov,c_ctry,c_prim_email,c_alt_email,c_phone_1,c_phone_2,c_phone_3,c_lcl_tx_id,c_nat_tx_id,ca_id,ca_tax_st,ca_b_id,ca_name
1970-01-01 01:47:35+00:00,NEW,4739,016-32-5107,M,3.0,1983-06-21,Moncur,Vittorio,NULL,19452 Bryant Irvin West,NULL,H2E 1V8,Paterson,TX,United States of America,Vittorio.Moncur@farce.de,NULL,821-2946,205-8612-06614,1-968-027-5679,MD4,MT5,NULL,NULL,NULL,NULL
1970-01-01 03:34:50+00:00,ADDACCT,4739,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,9029.0,1.0,12285.0,MEGTUPTLNkcqHeuefXAmPBOBlpRqoqq
1970-01-01 01:47:20+00:00,NEW,4728,031-80-9744,NULL,3.0,1933-04-19,Kempffer,Tandie,NULL,16132 Corsicana Road,NULL,85775,Overland Park,DC,United States of America,Tandie.Kempffer@gawab.com,Tandie.Kempffer@mail2world.com,880-2336,266-978-6267,,DE2,MN1,NULL,NULL,NULL,NULL
1970-01-01 03:34:20+00:00,ADDACCT,4728,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,9008.0,1.0,11473.0,NULL
2007-07-07 16:07:49+00:00,NEW,0,923-54-6498,F,3.0,1940-12-02,Joannis,Adara,,4779 Weller Way,,92624,Columbus,Ontario,Canada,Adara.Joannis@moose-mail.com,Adara.Joannis@gmx.com,1-872-523-8928,492-3961,,CA3,YT3,0.0,1.0,10180.0,CJlmMuFyibKOmKLHIaTeWugvCgZdmcfpDsYb

### `bronze.finwire_company` — loc=`_ivm/views/bronze/finwire_company`; version=0; rows=7; schema=`pts:timestamp, company_name:string, cik:string, status:string, industry_id:string, sp_rating:string, founding_date:date, address_line1:string, address_line2:string, postal_code:string, city:string, state_province:string, country:string, ceo_name:string, description:string`

pts,company_name,cik,status,industry_id,sp_rating,founding_date,address_line1,address_line2,postal_code,city,state_province,country,ceo_name,description
2017-07-07 21:49:41+00:00,ZHndjonIXBGIHnRhtZjNfQOillMVsTeZo...,0000001092,ACTV,TO,BB+,1949-08-02,3327 Buckline Court,,99744,Reno,AZ,United States of America,Rasher,PIdLIqzwWDIClBMvVbGtmrICSLYtUlQRW...
2017-07-07 22:12:56+00:00,hThLsTagPSDGXhDhgztjVCNPdPUEsUSJetF,0000001095,ACTV,DD,CCC,1963-03-27,9704 Neilson Crescent,,T6A 1D7,Norfolk,Saskatchewan,United States of America,Rohland,XFDGOBbosLYLIaXKUl UYjremwArRODJI...
2017-07-07 22:45:04+00:00,DWpuJIQTFSLnWvbWMtyZFGUjcikHsMSiL...,0000001091,ACTV,FL,C,2001-11-26,24246 Carlisle Drive,,M5A 1U7,Milwaukee,PA,United States of America,Teem,xBbkFKOQmEShhULuzh
2017-07-07 23:06:24+00:00,SdQWiJjCJhMJjRqJSGPTSSiDGrZ,0000001094,ACTV,AD,BBB,1919-02-26,5902 Lincoln North,,92401,Austin,GA,United States of America,Rabinowitz,seiPPSJRmSSphKsJSEEwhTmOBNodsOIdu...
2017-07-07 23:19:26+00:00,GOXTrVlFqgNCNVbOMxFhOQBDjM,0000000165,INAC,IM,BB-,1929-05-18,292 Sakowitz Road,,80152,Evansville,NE,,Toten,xpTDeoACifJYmH KCPdwdFMgrIZHxKXhD...

### `bronze.finwire_financial` — loc=`_ivm/views/bronze/finwire_financial`; version=0; rows=1007; schema=`pts:timestamp, year:integer, quarter:integer, quarter_start_date:date, posting_date:date, revenue:double, earnings:double, eps:double, diluted_eps:double, margin:double, inventory:double, assets:double, liabilities:double, sh_out:long, diluted_sh_out:long, cik:long, company_name:string`

pts,year,quarter,quarter_start_date,posting_date,revenue,earnings,eps,diluted_eps,margin,inventory,assets,liabilities,sh_out,diluted_sh_out,cik,company_name
2017-07-01 00:14:02+00:00,2017,3,2017-07-01,2017-07-01,729088460.71,84501307.05,0.14,0.12,0.12,676238580.63,557966582333.07,5964482937.4,586268209,690974896,NULL,XAeGuMGOgWVkUKrISBvTVGAAPdBbZQACF...
2017-07-01 00:32:21+00:00,2017,3,2017-07-01,2017-07-01,3019480921.67,1588714731.38,3.41,3.13,0.53,722809627.07,525326953132.02,2270130698.38,465412800,507297333,33.0,NULL
2017-07-01 00:39:25+00:00,2017,3,2017-07-01,2017-07-01,3863532415.94,766499258.96,1.57,1.43,0.2,286686266.66,149569189067.51,7394709194.08,488708140,534532165,988.0,NULL
2017-07-01 00:53:13+00:00,2017,3,2017-07-01,2017-07-01,2436592520.49,915126267.8,1.22,1.2,0.38,887882038.64,819210780772.95,4415332772.35,750178471,759871187,771.0,NULL
2017-07-01 01:08:28+00:00,2017,3,2017-07-01,2017-07-01,1681261385.89,70532059.51,0.09,0.08,0.04,267513232.23,933411878632.38,1904938929.94,806973168,897165266,714.0,NULL

### `bronze.finwire_security` — loc=`_ivm/views/bronze/finwire_security`; version=0; rows=11; schema=`pts:timestamp, symbol:string, issue_type:string, status:string, name:string, ex_id:string, sh_out:long, first_trade_date:date, first_exchange_date:date, dividend:double, cik:long, company_name:string`

pts,symbol,issue_type,status,name,ex_id,sh_out,first_trade_date,first_exchange_date,dividend,cik,company_name
2017-07-07 21:07:15+00:00,AAAAAAAAAAAACYU,PREF_A,ACTV,QFshjBTElyxHuSzOXpZeOnTWV,NASDAQ,960709737,1893-06-18,1916-11-02,0.28,729.0,NULL
2017-07-07 21:25:28+00:00,AAAAAAAAAAAACYS,PREF_B,ACTV,rhKGCQpsOOCCrdhaIjhSSPTjzLJzHUMGT,AMEX,99152906,1952-03-04,1982-04-23,1.93,NULL,XlMstoSukZznSjBWsWA
2017-07-07 21:27:39+00:00,AAAAAAAAAAAACYQ,PREF_B,ACTV,NWQzcUqEpVXbBQOEwGElr,PCX,627181514,1956-05-17,1955-11-22,1.43,998.0,NULL
2017-07-07 21:50:48+00:00,AAAAAAAAAAAACYO,PREF_B,ACTV,TouNDINYjivhTHRBwbLdib,NYSE,708636748,1940-04-07,1972-07-28,1.22,949.0,NULL
2017-07-07 22:18:43+00:00,AAAAAAAAAAAACNU,COMMON,INAC,tTRwEBEVCcFiJndRppARJRK,AMEX,835781161,1867-03-17,1991-10-24,1.66,725.0,NULL

### `bronze.hr_employee` — loc=`_ivm/views/bronze/hr_employee`; version=0; rows=150; schema=`employeeid:string, managerid:string, employeefirstname:string, employeelastname:string, employeemi:string, employeejobcode:string, employeebranch:string, employeeoffice:string, employeephone:string`

employeeid,managerid,employeefirstname,employeelastname,employeemi,employeejobcode,employeebranch,employeeoffice,employeephone
0,211,Ozkan,Douglas,NULL,647,EGZKSobTeknHCbLuHczvWmhTmCSGXD,OFFICE7152,(726) 088-3331
1,413,Suer,Candice,NULL,314,OfOBVvpzNvHCebxyuxXFwsMju  JRU,OFFICE8586,(344) 999-2652
2,246,Somisetty,Jami,P,534,rAHWYkktOXAyPAYHlncZPG,NULL,(984) 538-5366
3,248,Mazurek,Rosalinda,J,364,TJQqsUQQGqWG QleLheUoYlgRNVT,OFFICE8487,(860) 037-6897
4,1304,Aronovich,Delphine,M,314,IEMJHuQgCPDHCwwJkgQQeaqGvzMcVD,OFFICE9420,(604) 387-9350

### `bronze.reference_date` — loc=`_ivm/views/bronze/reference_date`; version=0; rows=260; schema=`sk_date_id:long, date_value:date, date_desc:string, calendar_year_id:integer, calendar_year_desc:string, calendar_qtr_id:integer, calendar_qtr_desc:string, calendar_month_id:integer, calendar_month_desc:string, calendar_week_id:integer, calendar_week_desc:string, day_of_week_num:integer, day_of_week_desc:string, fiscal_year_id:integer, fiscal_year_desc:string, fiscal_qtr_id:integer, fiscal_qtr_desc:string, holiday_flag:boolean`

sk_date_id,date_value,date_desc,calendar_year_id,calendar_year_desc,calendar_qtr_id,calendar_qtr_desc,calendar_month_id,calendar_month_desc,calendar_week_id,calendar_week_desc,day_of_week_num,day_of_week_desc,fiscal_year_id,fiscal_year_desc,fiscal_qtr_id,fiscal_qtr_desc,holiday_flag
19500101,1950-01-01,"January 1, 1950",1950,1950,19501,1950 Q1,19501,1950 January,19501,1950-W1,7,Sunday,1950,1950,19503,1950 Q3,True
19500102,1950-01-02,"January 2, 1950",1950,1950,19501,1950 Q1,19501,1950 January,19501,1950-W1,1,Monday,1950,1950,19503,1950 Q3,False
19500103,1950-01-03,"January 3, 1950",1950,1950,19501,1950 Q1,19501,1950 January,19501,1950-W1,2,Tuesday,1950,1950,19503,1950 Q3,False
19500104,1950-01-04,"January 4, 1950",1950,1950,19501,1950 Q1,19501,1950 January,19501,1950-W1,3,Wednesday,1950,1950,19503,1950 Q3,False
19500105,1950-01-05,"January 5, 1950",1950,1950,19501,1950 Q1,19501,1950 January,19501,1950-W1,4,Thursday,1950,1950,19503,1950 Q3,False

### `bronze.reference_industry` — loc=`_ivm/views/bronze/reference_industry`; version=0; rows=2; schema=`in_id:string, in_name:string, in_sc_id:string`

in_id,in_name,in_sc_id
AA,Misc. Capital Goods,FN
AC,Retail (Drugs),TC

### `bronze.reference_status_type` — loc=`_ivm/views/bronze/reference_status_type`; version=0; rows=1; schema=`st_id:string, st_name:string`

st_id,st_name
ACTV,Active

### `bronze.reference_tax_rate` — loc=`_ivm/views/bronze/reference_tax_rate`; version=0; rows=4; schema=`tx_id:string, tx_name:string, tx_rate:float`

tx_id,tx_name,tx_rate
US1,U.S. Income Tax Bracket for the poor,0.15000000596046448
US2,U.S. Income Tax Bracket for the h...,0.2750000059604645
US3,U.S. Income Tax Bracket for the m...,0.3050000071525574
US4,U.S. Income Tax Bracket for the w...,0.35499998927116394

### `bronze.reference_trade_type` — loc=`_ivm/views/bronze/reference_trade_type`; version=0; rows=1; schema=`tt_id:string, tt_name:string, tt_is_sell:integer, tt_is_mrkt:integer`

tt_id,tt_name,tt_is_sell,tt_is_mrkt
TMB,Market Buy,0,1

### `bronze.syndicated_prospect` — loc=`_ivm/views/bronze/syndicated_prospect`; version=2; rows=152; schema=`agency_id:string, last_name:string, first_name:string, middle_initial:string, gender:string, address_line1:string, address_line2:string, postal_code:string, city:string, state:string, country:string, phone:string, income:string, number_cars:integer, number_children:integer, marital_status:string, age:integer, credit_rating:integer, own_or_rent_flag:string, employer:string, number_credit_cards:integer, net_worth:integer`

agency_id,last_name,first_name,middle_initial,gender,address_line1,address_line2,postal_code,city,state,country,phone,income,number_cars,number_children,marital_status,age,credit_rating,own_or_rent_flag,employer,number_credit_cards,net_worth
PEL0,PELLAND,Netti,NULL,f,21847 olympia street,NULL,T6b 1i1,Fairbanks,MA,United States of America,1-712-522-6088,368776,NULL,3,W,20.0,760,O,Brink's,NULL,1058868
PEL0,PELLAND,Netti,NULL,f,21847 olympia street,NULL,T6b 1i1,Fairbanks,MA,United States of America,1-712-522-6088,368776,NULL,3,W,20.0,760,O,Brink's,NULL,1058868
PEL0,PELLAND,Netti,NULL,f,21847 olympia street,NULL,T6b 1i1,Fairbanks,MA,United States of America,1-712-522-6088,368776,NULL,3,W,20.0,760,O,Brink's,NULL,1058868
LED1,LEDUC,TRUDY,NULL,F,245 congress court,APT. 624,77281,Quebec,PA,United States of America,1-626-426-4298,177967,5.0,1,U,3.0,555,U,NULL,6.0,1988185
lat2,latif,ireland,NULL,NULL,6517 frailing west,NULL,91355,Henderson,Alberta,United States of America,1-524-787-8784,321772,2.0,1,S,NULL,566,O,NULL,6.0,3673128

## Silver tier: cleaned data

### `silver.accounts` — loc=`_ivm/views/silver/accounts`; version=2; rows=133; schema=`action_type:string, status:string, account_id:long, account_desc:string, customer_id:long, tax_id:string, gender:string, tier:integer, dob:date, last_name:string, first_name:string, middle_name:string, address_line1:string, address_line2:string, postal_code:string, city:string, state_province:string, country:string, primary_email:string, alternate_email:string, phone1:string, phone2:string, phone3:string, local_tax_rate_name:string, local_tax_rate:float, national_tax_rate_name:string, national_tax_rate:float, tax_status:integer, broker_id:long, effective_timestamp:timestamp, end_timestamp:timestamp, is_current:boolean, openivm_left_key:string`

action_type,status,account_id,account_desc,customer_id,tax_id,gender,tier,dob,last_name,first_name,middle_name,address_line1,address_line2,postal_code,city,state_province,country,primary_email,alternate_email,phone1,phone2,phone3,local_tax_rate_name,local_tax_rate,national_tax_rate_name,national_tax_rate,tax_status,broker_id,effective_timestamp,end_timestamp,is_current,openivm_left_key
ADDACCT,Active,9029,MEGTUPTLNkcqHeuefXAmPBOBlpRqoqq,4739,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,1.0,12285,1970-01-01 03:34:50,9999-12-31 23:59:59.999000,True,NULL
ADDACCT,Active,9008,NULL,4728,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,1.0,11473,1970-01-01 03:34:20,9999-12-31 23:59:59.999000,True,NULL
UPDACCT,Active,0,goVPhIAoxWXedLPYAOKFXzREOFiLSmNpY...,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,,,,NULL,NULL,NULL,NULL,NULL,6701,2007-07-19 10:34:37,9999-12-31 23:59:59.999000,True,NULL
NEW,Active,0,CJlmMuFyibKOmKLHIaTeWugvCgZdmcfpDsYb,0,923-54-6498,F,3.0,1940-12-02 00:00:00,Joannis,Adara,,4779 Weller Way,,92624,Columbus,Ontario,Canada,Adara.Joannis@moose-mail.com,Adara.Joannis@gmx.com,1-872-523-8928,492-3961,,CA3,NULL,YT3,NULL,1.0,10180,2007-07-07 16:07:49,2007-07-19 10:34:36.999000,False,CA3
NEW,Active,1,BbxTgVGOlgyrYtVRjsXDJKmKDUp s ApI...,1,645-68-9627,F,3.0,1982-12-17 00:00:00,Paperno,Jirina,P,7216 Gates Avenue,,H5K 1Q9,Inglewood,WI,United States of America,Jirina.P.Paperno@ip6.li,Jirina.P.Paperno@devils.com,767-4707,,,BC6,NULL,NU7,NULL,2.0,12538,2007-07-07 17:12:59,9999-12-31 23:59:59.999000,True,BC6

### `silver.cash_transactions` — loc=`_ivm/views/silver/cash_transactions`; version=2; rows=3619; schema=`customer_id:long, account_id:long, transaction_timestamp:timestamp, amount:double, description:string`

customer_id,account_id,transaction_timestamp,amount,description
0,0,2012-07-11 08:10:42+00:00,-37215.14,TGDRsaHPherhApDuHfXUPdexIUoEzKdgR...
11,11,2012-07-07 17:15:11+00:00,-3178.67,PGwhaPC igAVOmHLJppGbXaDEgHSurSvr...
12,12,2012-07-12 17:41:41+00:00,-3172.19,uQOUlrpDGHQpeeBGxaLTrxUwMwNMFowWA...
13,13,2012-09-20 03:17:55+00:00,-16621.0,VRIGhrJYHmbmNyXtIutswBfyfSLRjEJdG...
6,6,2012-07-09 07:40:09+00:00,-1315.7,gySbOpZLevgVdfrrwPiqBrFJFQGWehUzT...

### `silver.companies` — loc=`_ivm/views/silver/companies`; version=0; rows=0; schema=`company_id:string, status:string, name:string, industry:string, ceo:string, address_line1:string, address_line2:string, postal_code:string, city:string, state_province:string, country:string, description:string, founding_date:date, sp_rating:string, effective_timestamp:timestamp, end_timestamp:timestamp, is_current:boolean`

company_id,status,name,industry,ceo,address_line1,address_line2,postal_code,city,state_province,country,description,founding_date,sp_rating,effective_timestamp,end_timestamp,is_current

### `silver.customers` — loc=`_ivm/views/silver/customers`; version=2; rows=81; schema=`action_type:string, status:string, customer_id:long, account_id:long, tax_id:string, gender:string, tier:integer, dob:date, last_name:string, first_name:string, middle_name:string, address_line1:string, address_line2:string, postal_code:string, city:string, state_province:string, country:string, primary_email:string, alternate_email:string, phone1:string, phone2:string, phone3:string, local_tax_rate_name:string, local_tax_rate:float, national_tax_rate_name:string, national_tax_rate:float, account_tax_status:integer, broker_id:long, effective_timestamp:timestamp, end_timestamp:timestamp, is_current:boolean, openivm_left_key:string`

action_type,status,customer_id,account_id,tax_id,gender,tier,dob,last_name,first_name,middle_name,address_line1,address_line2,postal_code,city,state_province,country,primary_email,alternate_email,phone1,phone2,phone3,local_tax_rate_name,local_tax_rate,national_tax_rate_name,national_tax_rate,account_tax_status,broker_id,effective_timestamp,end_timestamp,is_current,openivm_left_key
NEW,Active,4739.0,NULL,016-32-5107,M,3.0,1983-06-21 00:00:00,Moncur,Vittorio,NULL,19452 Bryant Irvin West,NULL,H2E 1V8,Paterson,TX,United States of America,Vittorio.Moncur@farce.de,NULL,821-2946,205-8612-06614,1-968-027-5679,MD4,NULL,MT5,NULL,NULL,NULL,1970-01-01 01:47:35,9999-12-31 23:59:59.999000,True,MD4
NEW,Active,4728.0,NULL,031-80-9744,NULL,3.0,1933-04-19 00:00:00,Kempffer,Tandie,NULL,16132 Corsicana Road,NULL,85775,Overland Park,DC,United States of America,Tandie.Kempffer@gawab.com,Tandie.Kempffer@mail2world.com,880-2336,266-978-6267,,DE2,NULL,MN1,NULL,NULL,NULL,1970-01-01 01:47:20,9999-12-31 23:59:59.999000,True,DE2
UPDCUST,Active,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,,,,NULL,NULL,NULL,NULL,NULL,NULL,2007-07-25 21:33:21,9999-12-31 23:59:59.999000,True,NULL
UPDCUST,Active,0.0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,13533 Olmos Lane,Apt. 950,V6J 1U3,San Buenaventura,VT,NULL,Adara.Joannis@gmx.it,Adara.Joannis@devils.com,298-826-5884,282-768-2031,,NULL,NULL,NULL,NULL,NULL,NULL,2007-08-05 17:07:23,9999-12-31 23:59:59.999000,True,NULL
UPDCUST,Active,0.0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,,NULL,Augusta,British Columbia,United States of America,Adara.Joannis@gmail.com,,357-423-9191,270-141-3475,421-277-2023-59348,NULL,NULL,NULL,NULL,NULL,NULL,2007-07-17 03:58:34,2007-08-05 17:07:22.999000,False,NULL

### `silver.daily_market` — loc=`_ivm/views/silver/daily_market`; version=4; rows=12830; schema=`dm_date:date, dm_s_symb:string, dm_close:double, dm_high:double, dm_low:double, dm_vol:integer, fifty_two_week_low:double, fifty_two_week_high:double, fifty_two_week_low_date:date, fifty_two_week_high_date:date`

dm_date,dm_s_symb,dm_close,dm_high,dm_low,dm_vol,fifty_two_week_low,fifty_two_week_high,fifty_two_week_low_date,fifty_two_week_high_date
2015-07-06,AAAAAAAAAAAACCH,468.58,526.37,364.74,257116818,364.74,526.37,2015-07-06,2015-07-06
2015-07-07,AAAAAAAAAAAACCH,520.01,613.52,328.99,392596471,328.99,613.52,2015-07-07,2015-07-07
2015-07-08,AAAAAAAAAAAACCH,694.49,886.11,645.69,663444025,328.99,886.11,2015-07-07,2015-07-08
2015-07-09,AAAAAAAAAAAACCH,498.81,573.99,402.67,312074929,328.99,886.11,2015-07-07,2015-07-08
2015-07-10,AAAAAAAAAAAACCH,324.81,332.48,242.52,83903109,242.52,886.11,2015-07-10,2015-07-08

### `silver.date` — loc=`_ivm/views/silver/date`; version=0; rows=260; schema=`sk_date_id:long, date_value:date, date_desc:string, calendar_year_id:integer, calendar_year_desc:string, calendar_qtr_id:integer, calendar_qtr_desc:string, calendar_month_id:integer, calendar_month_desc:string, calendar_week_id:integer, calendar_week_desc:string, day_of_week_num:integer, day_of_week_desc:string, fiscal_year_id:integer, fiscal_year_desc:string, fiscal_qtr_id:integer, fiscal_qtr_desc:string, holiday_flag:boolean`

sk_date_id,date_value,date_desc,calendar_year_id,calendar_year_desc,calendar_qtr_id,calendar_qtr_desc,calendar_month_id,calendar_month_desc,calendar_week_id,calendar_week_desc,day_of_week_num,day_of_week_desc,fiscal_year_id,fiscal_year_desc,fiscal_qtr_id,fiscal_qtr_desc,holiday_flag
19500101,1950-01-01,"January 1, 1950",1950,1950,19501,1950 Q1,19501,1950 January,19501,1950-W1,7,Sunday,1950,1950,19503,1950 Q3,True
19500102,1950-01-02,"January 2, 1950",1950,1950,19501,1950 Q1,19501,1950 January,19501,1950-W1,1,Monday,1950,1950,19503,1950 Q3,False
19500103,1950-01-03,"January 3, 1950",1950,1950,19501,1950 Q1,19501,1950 January,19501,1950-W1,2,Tuesday,1950,1950,19503,1950 Q3,False
19500104,1950-01-04,"January 4, 1950",1950,1950,19501,1950 Q1,19501,1950 January,19501,1950-W1,3,Wednesday,1950,1950,19503,1950 Q3,False
19500105,1950-01-05,"January 5, 1950",1950,1950,19501,1950 Q1,19501,1950 January,19501,1950-W1,4,Thursday,1950,1950,19503,1950 Q3,False

### `silver.employees` — loc=`_ivm/views/silver/employees`; version=0; rows=150; schema=`employee_id:string, manager_id:string, first_name:string, last_name:string, middle_initial:string, job_code:string, branch:string, office:string, phone:string`

employee_id,manager_id,first_name,last_name,middle_initial,job_code,branch,office,phone
0,211,Ozkan,Douglas,NULL,647,EGZKSobTeknHCbLuHczvWmhTmCSGXD,OFFICE7152,(726) 088-3331
1,413,Suer,Candice,NULL,314,OfOBVvpzNvHCebxyuxXFwsMju  JRU,OFFICE8586,(344) 999-2652
2,246,Somisetty,Jami,P,534,rAHWYkktOXAyPAYHlncZPG,NULL,(984) 538-5366
3,248,Mazurek,Rosalinda,J,364,TJQqsUQQGqWG QleLheUoYlgRNVT,OFFICE8487,(860) 037-6897
4,1304,Aronovich,Delphine,M,314,IEMJHuQgCPDHCwwJkgQQeaqGvzMcVD,OFFICE9420,(604) 387-9350

### `silver.financials` — loc=`_ivm/views/silver/financials`; version=0; rows=1007; schema=`year:integer, quarter:integer, quarter_start_date:date, posting_date:date, revenue:double, earnings:double, eps:double, diluted_eps:double, margin:double, inventory:double, assets:double, liabilities:double, sh_out:long, diluted_sh_out:long, company_name:string, company_id:string, effective_timestamp:timestamp, end_timestamp:timestamp, is_current:boolean, openivm_col_19:string, openivm_left_key:string`

year,quarter,quarter_start_date,posting_date,revenue,earnings,eps,diluted_eps,margin,inventory,assets,liabilities,sh_out,diluted_sh_out,company_name,company_id,effective_timestamp,end_timestamp,is_current,openivm_col_19,openivm_left_key
2017,3,2017-07-01 00:00:00,2017-07-07 00:00:00,684050224.6,110070604.72,0.64,0.58,0.16,881429271.71,594370852138.2,3096920051.51,171732581,191197421,NULL,NULL,2017-07-07 20:55:00,9999-12-31 23:59:59.999000,True,NULL,NULL
2017,3,2017-07-01 00:00:00,2017-07-07 00:00:00,9539054614.46,3782122047.29,22.51,19.58,0.4,927482864.44,224623920468.55,3799299664.34,167990618,193172993,NULL,NULL,2017-07-07 20:45:10,2017-07-07 20:54:59.999000,False,tyCuwRCMECWVtZFMjhBDoJVBSYKTCXWsQ...,tyCuwRCMECWVtZFMjhBDoJVBSYKTCXWsQ...
2017,3,2017-07-01 00:00:00,2017-07-07 00:00:00,7718361791.04,5208844230.13,6.44,6.06,0.67,964044704.48,93879119445.37,8877150081.46,808881254,859622175,NULL,NULL,2017-07-07 20:37:25,2017-07-07 20:45:09.999000,False,KOoRBLuVJAmbedckFhAQyRqTdgMMbJwtQ...,KOoRBLuVJAmbedckFhAQyRqTdgMMbJwtQ...
2017,3,2017-07-01 00:00:00,2017-07-07 00:00:00,9227852363.45,5818401335.4,12.54,10.75,0.63,29609579.97,183512542328.27,1358195093.5,464034671,541321395,NULL,NULL,2017-07-07 20:28:15,2017-07-07 20:37:24.999000,False,NULL,NULL
2017,3,2017-07-01 00:00:00,2017-07-07 00:00:00,8488172128.81,2213212392.31,4.71,4.39,0.26,679351331.94,29939570480.86,6289687648.24,469645622,503903129,NULL,NULL,2017-07-07 20:27:58,2017-07-07 20:28:14.999000,False,NULL,NULL

### `silver.holdings_history` — loc=`_ivm/views/silver/holdings_history`; version=0; rows=0; schema=`trade_id:integer, previous_trade_id:integer, previous_quantity:integer, quantity:integer, account_id:long, symbol:string, create_timestamp:timestamp, close_timestamp:timestamp, trade_price:double, bid_price:double, fee:double, commission:double`

trade_id,previous_trade_id,previous_quantity,quantity,account_id,symbol,create_timestamp,close_timestamp,trade_price,bid_price,fee,commission

### `silver.securities` — loc=`_ivm/views/silver/securities`; version=0; rows=11; schema=`symbol:string, issue_type:string, status:string, name:string, exchange_id:string, shares_outstanding:long, first_trade_date:date, first_exchange_date:date, dividend:double, company_name:string, company_id:string, effective_timestamp:timestamp, end_timestamp:timestamp, is_current:boolean, openivm_col_14:string, openivm_left_key:string`

symbol,issue_type,status,name,exchange_id,shares_outstanding,first_trade_date,first_exchange_date,dividend,company_name,company_id,effective_timestamp,end_timestamp,is_current,openivm_col_14,openivm_left_key
AAAAAAAAAAAAAFX,PREF_A,Active,MRkQFITQREXV MPRJdLIaYNCCN,NASDAQ,643378000,1939-04-30 00:00:00,1906-05-14 00:00:00,2.94,NULL,NULL,2017-07-07 23:59:59,9999-12-31 23:59:59.999000,True,NULL,NULL
AAAAAAAAAAAACNU,COMMON,Inactive,tTRwEBEVCcFiJndRppARJRK,AMEX,835781161,1867-03-17 00:00:00,1991-10-24 00:00:00,1.66,NULL,NULL,2017-07-07 22:18:43,9999-12-31 23:59:59.999000,True,NULL,NULL
AAAAAAAAAAAACYN,COMMON,Active,cFRtVQgjbvfCkyApCS KnS,PCX,371656067,1920-07-11 00:00:00,1921-03-28 00:00:00,1.91,NULL,NULL,2017-07-07 23:58:18,9999-12-31 23:59:59.999000,True,lbDjHZNgQbBNbMfqAToWMEQSctFarRUaH...,lbDjHZNgQbBNbMfqAToWMEQSctFarRUaH...
AAAAAAAAAAAACYO,PREF_B,Active,TouNDINYjivhTHRBwbLdib,NYSE,708636748,1940-04-07 00:00:00,1972-07-28 00:00:00,1.22,NULL,NULL,2017-07-07 21:50:48,9999-12-31 23:59:59.999000,True,NULL,NULL
AAAAAAAAAAAACYP,PREF_B,Active,LtywXVIQDnnRkKE,AMEX,865013067,1974-09-12 00:00:00,1872-12-02 00:00:00,1.64,NULL,NULL,2017-07-07 23:28:25,9999-12-31 23:59:59.999000,True,IaIJkJbThYSCqPmZqGFESRtOvhVxcLlZs...,IaIJkJbThYSCqPmZqGFESRtOvhVxcLlZs...

### `silver.trades` — loc=`_ivm/views/silver/trades`; version=0; rows=0; schema=`trade_id:long, account_id:long, trade_status:string, trade_type:string, transaction_type:string, symbol:string, executor_name:string, quantity:integer, bid_price:double, trade_price:double, fee:double, commission:double, tax:double, create_timestamp:timestamp, close_timestamp:timestamp`

trade_id,account_id,trade_status,trade_type,transaction_type,symbol,executor_name,quantity,bid_price,trade_price,fee,commission,tax,create_timestamp,close_timestamp

### `silver.trades_history` — loc=`_ivm/views/silver/trades_history`; version=0; rows=0; schema=`trade_id:long, trade_timestamp:timestamp, account_id:long, trade_status:string, trade_type:string, transaction_type:string, symbol:string, executor_name:string, quantity:integer, bid_price:double, trade_price:double, fee:double, commission:double, tax:double, update_status:string, effective_timestamp:timestamp, end_timestamp:timestamp, is_current:boolean`

trade_id,trade_timestamp,account_id,trade_status,trade_type,transaction_type,symbol,executor_name,quantity,bid_price,trade_price,fee,commission,tax,update_status,effective_timestamp,end_timestamp,is_current

### `silver.watches` — loc=`_ivm/views/silver/watches`; version=2; rows=5; schema=`customer_id:long, symbol:string, company_id:string, company_name:string, exchange_id:string, security_status:string, placed_timestamp:timestamp, removed_timestamp:timestamp, watch_status:string`

customer_id,symbol,company_id,company_name,exchange_id,security_status,placed_timestamp,removed_timestamp,watch_status
12,AAAAAAAAAAAAAFX,NULL,NULL,NASDAQ,Active,2012-07-17 11:57:52+00:00,NULL,Active
20,AAAAAAAAAAAAAFX,NULL,NULL,NASDAQ,Active,2012-07-23 20:11:18+00:00,NULL,Active
30,AAAAAAAAAAAAAFX,NULL,NULL,NASDAQ,Active,2012-07-23 16:57:38+00:00,NULL,Active
3,AAAAAAAAAAAAAFX,NULL,NULL,NASDAQ,Active,2012-07-13 02:42:05+00:00,NULL,Active
9,AAAAAAAAAAAAAFX,NULL,NULL,NASDAQ,Active,2012-07-09 07:52:22+00:00,NULL,Active

### `silver.watches_history` — loc=`_ivm/views/silver/watches_history`; version=2; rows=5; schema=`symbol:string, customer_id:long, watch_timestamp:timestamp, action_type:string, company_id:string, company_name:string, exchange_id:string, security_status:string`

symbol,customer_id,watch_timestamp,action_type,company_id,company_name,exchange_id,security_status
AAAAAAAAAAAAAFX,9,2012-07-09 07:52:22+00:00,Activate,NULL,NULL,NASDAQ,Active
AAAAAAAAAAAAAFX,3,2012-07-13 02:42:05+00:00,Activate,NULL,NULL,NASDAQ,Active
AAAAAAAAAAAAAFX,12,2012-07-17 11:57:52+00:00,Activate,NULL,NULL,NASDAQ,Active
AAAAAAAAAAAAAFX,30,2012-07-23 16:57:38+00:00,Activate,NULL,NULL,NASDAQ,Active
AAAAAAAAAAAAAFX,20,2012-07-23 20:11:18+00:00,Activate,NULL,NULL,NASDAQ,Active

## Gold tier: business-facing facts, dimensions, aggregates

### `gold.broker_performance` — loc=`_ivm/views/gold/broker_performance`; version=0; rows=0; schema=`sk_broker_id:string, broker_id:string, first_name:string, last_name:string, trade_count:long, unique_customers:long, unique_securities:long, unique_accounts:long, active_days:long, total_volume:long, total_notional:double, avg_trade_price:double, total_commission:double, total_fees:double, avg_commission:double, avg_fee:double, pct_of_trades:decimal(29,4), pct_of_notional:double, notional_z_score:double, customer_z_score:double, rank_by_notional:integer, rank_by_customers:integer, rank_by_commission:integer`

sk_broker_id,broker_id,first_name,last_name,trade_count,unique_customers,unique_securities,unique_accounts,active_days,total_volume,total_notional,avg_trade_price,total_commission,total_fees,avg_commission,avg_fee,pct_of_trades,pct_of_notional,notional_z_score,customer_z_score,rank_by_notional,rank_by_customers,rank_by_commission

### `gold.customer_concentration` — loc=`_ivm/views/gold/customer_concentration`; version=0; rows=0; schema=`sk_customer_id:string, num_accounts:long, num_securities:long, total_trades:long, total_portfolio_value:double, total_costs:double, largest_position:double, concentration_pct:double, portfolio_z_score:double, pct_of_market:double, rank_by_portfolio:integer, rank_by_diversity:integer`

sk_customer_id,num_accounts,num_securities,total_trades,total_portfolio_value,total_costs,largest_position,concentration_pct,portfolio_z_score,pct_of_market,rank_by_portfolio,rank_by_diversity

### `gold.daily_market_pulse` — loc=`_ivm/views/gold/daily_market_pulse`; version=2; rows=10; schema=`dm_date:date, num_records:long, active_symbols:long, total_volume:long, avg_close_price:double, close_dispersion:double, market_low:double, market_high:double, avg_intraday_spread:double, closed_upper_half_count:long, volume_change_pct:decimal(29,4), close_change_pct:double, symbol_count_change:long, volume_z_score:double, pct_of_total_volume:decimal(29,4), rank_by_volume:integer, rank_by_dispersion:integer`

dm_date,num_records,active_symbols,total_volume,avg_close_price,close_dispersion,market_low,market_high,avg_intraday_spread,closed_upper_half_count,volume_change_pct,close_change_pct,symbol_count_change,volume_z_score,pct_of_total_volume,rank_by_volume,rank_by_dispersion
2015-07-12,1724,1724,881459032080,490.5302784222737,295.420618,1.1,1456.37,245.85917633410673,822,0.8986,-3.2463,0.0,0.6553,13.7716,1,1
2015-07-11,1724,1724,873608557952,506.9888167053365,293.688921,0.34,1481.42,252.42765661252943,829,3.6319,2.6928,0.0,0.634,13.6490,2,2
2015-07-06,1724,1724,852098086633,486.55004060324916,290.080455,0.98,1469.79,241.46639791183318,903,NULL,NULL,NULL,0.5756,13.3129,5,3
2015-07-07,1724,1724,860834726073,509.0667807424594,289.550434,0.68,1488.27,254.40723897911835,864,1.0253,4.6278,0.0,0.5993,13.4494,4,4
2015-07-08,1724,1724,835845862822,510.37970997679747,289.477067,0.5,1464.89,251.96955916473357,846,-2.9029,0.2579,0.0,0.5315,13.0590,7,5

### `gold.dim_account` — loc=`_ivm/views/gold/dim_account`; version=2; rows=2; schema=`sk_account_id:string, account_id:long, sk_broker_id:string, sk_customer_id:string, status:string, account_desc:string, tax_status:integer, effective_timestamp:timestamp, end_timestamp:timestamp, is_current:boolean`

sk_account_id,account_id,sk_broker_id,sk_customer_id,status,account_desc,tax_status,effective_timestamp,end_timestamp,is_current
5933a57dfdd7754a9266093ba41b607e,50,1c383cd30b7c298ab50293adfecb7b18,416f06966b58c40f4d5bfe54d88d2da5,Active,CddrEzyNqVTYgyIJxQmJlUjsLVBlcIGaO...,1,2007-07-27 19:19:00,9999-12-31 23:59:59.999000,True
235aa20b5a93173d113273e4b589d696,65,a5771bce93e200c36f7cd9dfd0e5deaa,b095166903a99a11ea6ca4a7972abc3b,Active,GVFbyZzhLdqkEjpJiNwGEeOBnKPqjWJAMYne,1,2007-08-03 05:04:43,9999-12-31 23:59:59.999000,True

### `gold.dim_broker` — loc=`_ivm/views/gold/dim_broker`; version=0; rows=150; schema=`sk_broker_id:string, broker_id:string, manager_id:string, first_name:string, last_name:string, middle_initial:string, job_code:string, branch:string, office:string, phone:string`

sk_broker_id,broker_id,manager_id,first_name,last_name,middle_initial,job_code,branch,office,phone
cfcd208495d565ef66e7dff9f98764da,0,211,Ozkan,Douglas,NULL,647,EGZKSobTeknHCbLuHczvWmhTmCSGXD,OFFICE7152,(726) 088-3331
c4ca4238a0b923820dcc509a6f75849b,1,413,Suer,Candice,NULL,314,OfOBVvpzNvHCebxyuxXFwsMju  JRU,OFFICE8586,(344) 999-2652
c81e728d9d4c2f636f067f89cc14862c,2,246,Somisetty,Jami,P,534,rAHWYkktOXAyPAYHlncZPG,NULL,(984) 538-5366
eccbc87e4b5ce2fe28308fd9f2a7baf3,3,248,Mazurek,Rosalinda,J,364,TJQqsUQQGqWG QleLheUoYlgRNVT,OFFICE8487,(860) 037-6897
a87ff679a2f3e71d9181a67b7542122c,4,1304,Aronovich,Delphine,M,314,IEMJHuQgCPDHCwwJkgQQeaqGvzMcVD,OFFICE9420,(604) 387-9350

### `gold.dim_company` — loc=`_ivm/views/gold/dim_company`; version=0; rows=0; schema=`sk_company_id:string, company_id:string, status:string, name:string, industry:string, ceo:string, address_line1:string, address_line2:string, postal_code:string, city:string, state_province:string, country:string, description:string, founding_date:date, sp_rating:string, is_lowgrade:boolean, effective_timestamp:timestamp, end_timestamp:timestamp, is_current:boolean`

sk_company_id,company_id,status,name,industry,ceo,address_line1,address_line2,postal_code,city,state_province,country,description,founding_date,sp_rating,is_lowgrade,effective_timestamp,end_timestamp,is_current

### `gold.dim_customer` — loc=`_ivm/views/gold/dim_customer`; version=2; rows=81; schema=`sk_customer_id:string, customer_id:long, tax_id:string, status:string, last_name:string, first_name:string, middleinitial:string, gender:string, tier:integer, dob:date, address_line1:string, address_line2:string, postal_code:string, city:string, state_province:string, country:string, phone1:string, phone2:string, phone3:string, primary_email:string, alternate_email:string, local_tax_rate_name:string, local_tax_rate:float, national_tax_rate_name:string, national_tax_rate:float, agency_id:string, credit_rating:integer, net_worth:integer, effective_timestamp:timestamp, end_timestamp:timestamp, is_current:boolean, openivm_col_31:string, openivm_left_key:string`

sk_customer_id,customer_id,tax_id,status,last_name,first_name,middleinitial,gender,tier,dob,address_line1,address_line2,postal_code,city,state_province,country,phone1,phone2,phone3,primary_email,alternate_email,local_tax_rate_name,local_tax_rate,national_tax_rate_name,national_tax_rate,agency_id,credit_rating,net_worth,effective_timestamp,end_timestamp,is_current,openivm_col_31,openivm_left_key
4856b2b718d15c9d4c3ec59da27f0b9f,4739.0,016-32-5107,Active,Moncur,Vittorio,NULL,M,3.0,1983-06-21 00:00:00,19452 Bryant Irvin West,NULL,H2E 1V8,Paterson,TX,United States of America,821-2946,205-8612-06614,1-968-027-5679,Vittorio.Moncur@farce.de,NULL,MD4,NULL,MT5,NULL,NULL,NULL,NULL,1970-01-01 01:47:35,9999-12-31 23:59:59.999000,True,Vittorio,Vittorio
c1608165e965d20a7ddd46dd4650641a,4728.0,031-80-9744,Active,Kempffer,Tandie,NULL,NULL,3.0,1933-04-19 00:00:00,16132 Corsicana Road,NULL,85775,Overland Park,DC,United States of America,880-2336,266-978-6267,,Tandie.Kempffer@gawab.com,Tandie.Kempffer@mail2world.com,DE2,NULL,MN1,NULL,NULL,NULL,NULL,1970-01-01 01:47:20,9999-12-31 23:59:59.999000,True,Tandie,Tandie
6b8fbb617d9b114ac130a28542d12276,NULL,NULL,Active,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,,,,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,2007-07-25 21:33:21,9999-12-31 23:59:59.999000,True,NULL,NULL
034d2a9d1d32aa2cfe0c89b1a4670b2c,0.0,923-54-6498,Active,Joannis,Adara,,F,3.0,1940-12-02 00:00:00,4779 Weller Way,,92624,Columbus,Ontario,Canada,1-872-523-8928,492-3961,,Adara.Joannis@moose-mail.com,Adara.Joannis@gmx.com,CA3,NULL,YT3,NULL,NULL,NULL,NULL,2007-07-07 16:07:49,2007-07-17 03:58:33.999000,False,Adara,Adara
6ddd2386f990cfbf293c1ee5379ce2a4,0.0,923-54-6498,Active,Joannis,Adara,,F,3.0,1940-12-02 00:00:00,4779 Weller Way,,92624,Augusta,British Columbia,United States of America,357-423-9191,270-141-3475,421-277-2023-59348,Adara.Joannis@gmail.com,,CA3,NULL,YT3,NULL,NULL,NULL,NULL,2007-07-17 03:58:34,2007-08-05 17:07:22.999000,False,NULL,NULL

### `gold.dim_date` — loc=`_ivm/views/gold/dim_date`; version=0; rows=260; schema=`sk_date_id:long, date_value:date, date_desc:string, calendar_year_id:integer, calendar_year_desc:string, calendar_qtr_id:integer, calendar_qtr_desc:string, calendar_month_id:integer, calendar_month_desc:string, calendar_week_id:integer, calendar_week_desc:string, day_of_week_num:integer, day_of_week_desc:string, fiscal_year_id:integer, fiscal_year_desc:string, fiscal_qtr_id:integer, fiscal_qtr_desc:string, holiday_flag:boolean`

sk_date_id,date_value,date_desc,calendar_year_id,calendar_year_desc,calendar_qtr_id,calendar_qtr_desc,calendar_month_id,calendar_month_desc,calendar_week_id,calendar_week_desc,day_of_week_num,day_of_week_desc,fiscal_year_id,fiscal_year_desc,fiscal_qtr_id,fiscal_qtr_desc,holiday_flag
19500101,1950-01-01,"January 1, 1950",1950,1950,19501,1950 Q1,19501,1950 January,19501,1950-W1,7,Sunday,1950,1950,19503,1950 Q3,True
19500102,1950-01-02,"January 2, 1950",1950,1950,19501,1950 Q1,19501,1950 January,19501,1950-W1,1,Monday,1950,1950,19503,1950 Q3,False
19500103,1950-01-03,"January 3, 1950",1950,1950,19501,1950 Q1,19501,1950 January,19501,1950-W1,2,Tuesday,1950,1950,19503,1950 Q3,False
19500104,1950-01-04,"January 4, 1950",1950,1950,19501,1950 Q1,19501,1950 January,19501,1950-W1,3,Wednesday,1950,1950,19503,1950 Q3,False
19500105,1950-01-05,"January 5, 1950",1950,1950,19501,1950 Q1,19501,1950 January,19501,1950-W1,4,Thursday,1950,1950,19503,1950 Q3,False

### `gold.dim_security` — loc=`_ivm/views/gold/dim_security`; version=0; rows=0; schema=`sk_security_id:string, symbol:string, issue:string, status:string, name:string, exchange_id:string, sk_company_id:string, shares_outstanding:long, first_trade_date:date, first_exchange_date:date, dividend:double, effective_timestamp:timestamp, end_timestamp:timestamp, is_current:boolean`

sk_security_id,symbol,issue,status,name,exchange_id,sk_company_id,shares_outstanding,first_trade_date,first_exchange_date,dividend,effective_timestamp,end_timestamp,is_current

### `gold.dim_trade` — loc=`_ivm/views/gold/dim_trade`; version=0; rows=0; schema=`sk_trade_id:string, trade_id:long, status:string, transaction_type:string, type:string, executed_by:string, effective_timestamp:timestamp, end_timestamp:timestamp, is_current:boolean`

sk_trade_id,trade_id,status,transaction_type,type,executed_by,effective_timestamp,end_timestamp,is_current

### `gold.fact_cash_balances` — loc=`_ivm/views/gold/fact_cash_balances`; version=2; rows=16; schema=`sk_customer_id:string, sk_account_id:string, sk_transaction_date:date, amount:double, description:string`

sk_customer_id,sk_account_id,sk_transaction_date,amount,description
416f06966b58c40f4d5bfe54d88d2da5,5933a57dfdd7754a9266093ba41b607e,2012-07-21,-8381.73,SbcLRArRxUBCEC NKhFLhdQkP
416f06966b58c40f4d5bfe54d88d2da5,5933a57dfdd7754a9266093ba41b607e,2012-10-02,78099.69,RJSxTDAPMqDcubRNIDZaGpNOMNJNPUNbJ...
416f06966b58c40f4d5bfe54d88d2da5,5933a57dfdd7754a9266093ba41b607e,2012-07-27,25106.43,WLQyjTaONEgflJyGDSx wHRYS rdTwzJY...
416f06966b58c40f4d5bfe54d88d2da5,5933a57dfdd7754a9266093ba41b607e,2012-10-07,6938.08,vXvQCVpB PyqueTvFtheKpZNRzWRJRxPB...
416f06966b58c40f4d5bfe54d88d2da5,5933a57dfdd7754a9266093ba41b607e,2012-09-25,-5457.45,nFREPnJmlwMaPeKd VULNELbOxQiHxFVD...

### `gold.fact_cash_transactions` — loc=`_ivm/views/gold/fact_cash_transactions`; version=2; rows=16; schema=`sk_customer_id:string, sk_account_id:string, sk_transaction_date:date, transaction_timestamp:timestamp, amount:double, description:string`

sk_customer_id,sk_account_id,sk_transaction_date,transaction_timestamp,amount,description
416f06966b58c40f4d5bfe54d88d2da5,5933a57dfdd7754a9266093ba41b607e,2012-07-24,2012-07-24 22:53:50+00:00,-2008.77,YxjjKlOFMxVNKQXZa WMNddDiWbovXuGJ...
416f06966b58c40f4d5bfe54d88d2da5,5933a57dfdd7754a9266093ba41b607e,2012-07-21,2012-07-21 21:54:37+00:00,-8381.73,SbcLRArRxUBCEC NKhFLhdQkP
416f06966b58c40f4d5bfe54d88d2da5,5933a57dfdd7754a9266093ba41b607e,2012-09-02,2012-09-02 18:13:19+00:00,-63892.33,WsGhcuLAmQEXfjm
416f06966b58c40f4d5bfe54d88d2da5,5933a57dfdd7754a9266093ba41b607e,2012-10-05,2012-10-05 09:17:40+00:00,-6947.93,FAgKcMUNkNGDpgUjGIBVVOIFOPpwVUsLq...
416f06966b58c40f4d5bfe54d88d2da5,5933a57dfdd7754a9266093ba41b607e,2012-10-01,2012-10-01 02:25:18+00:00,-81677.7,RgSYNXTUFVfpnSvpeJAMRVBiD RawKYIh...

### `gold.fact_holdings` — loc=`_ivm/views/gold/fact_holdings`; version=0; rows=0; schema=`sk_current_trade_id:string, sk_trade_id:string, sk_customer_id:string, sk_account_id:string, sk_security_id:string, sk_trade_date:date, trade_timestamp:timestamp, current_price:double, current_holding:integer, current_bid_price:double, current_fee:double, current_commission:double`

sk_current_trade_id,sk_trade_id,sk_customer_id,sk_account_id,sk_security_id,sk_trade_date,trade_timestamp,current_price,current_holding,current_bid_price,current_fee,current_commission

### `gold.fact_market_history` — loc=`_ivm/views/gold/fact_market_history`; version=0; rows=0; schema=`sk_security_id:string, sk_company_id:string, sk_date_id:date, yield:double, fifty_two_week_high:double, sk_fifty_two_week_high_date:date, fifty_two_week_low:double, sk_fifty_two_week_low_date:date, closeprice:double, dayhigh:double, daylow:double, volume:integer`

sk_security_id,sk_company_id,sk_date_id,yield,fifty_two_week_high,sk_fifty_two_week_high_date,fifty_two_week_low,sk_fifty_two_week_low_date,closeprice,dayhigh,daylow,volume

### `gold.fact_trade` — loc=`_ivm/views/gold/fact_trade`; version=0; rows=0; schema=`sk_trade_id:string, sk_broker_id:string, sk_customer_id:string, sk_account_id:string, sk_security_id:string, sk_create_date:date, create_timestamp:timestamp, sk_close_date:date, close_timestamp:timestamp, executed_by:string, quantity:integer, bid_price:double, trade_price:double, fee:double, commission:double, tax:double`

sk_trade_id,sk_broker_id,sk_customer_id,sk_account_id,sk_security_id,sk_create_date,create_timestamp,sk_close_date,close_timestamp,executed_by,quantity,bid_price,trade_price,fee,commission,tax

### `gold.fact_watches` — loc=`_ivm/views/gold/fact_watches`; version=0; rows=0; schema=`sk_customer_id:string, sk_security_id:string, sk_date_placed:date, sk_date_removed:date, watch_cnt:integer`

sk_customer_id,sk_security_id,sk_date_placed,sk_date_removed,watch_cnt

### `gold.market_volatility` — loc=`_ivm/views/gold/market_volatility`; version=4; rows=1724; schema=`dm_s_symb:string, trading_days:long, avg_daily_return:double, return_volatility:double, avg_intraday_range:double, max_intraday_range:double, total_volume:long, avg_volume:double, volume_volatility:double, unique_trading_dates:long, volatility_z_score:double, pct_market_volume:double, rank_by_volatility:integer, rank_by_volume:integer`

dm_s_symb,trading_days,avg_daily_return,return_volatility,avg_intraday_range,max_intraday_range,total_volume,avg_volume,volume_volatility,unique_trading_dates,volatility_z_score,pct_market_volume,rank_by_volatility,rank_by_volume
AAAAAAAAAAAACUB,8,1.935,4.2332,269.19,706.1700000000001,6273518909,784189863.625,293601537.2678,8,-0.1133,0.098,451,1
AAAAAAAAAAAACTB,8,0.3824,1.507,232.0663,630.8300000000002,6240996188,780124523.5,122110849.807,8,-0.244,0.0975,1029,2
AAAAAAAAAAAACPD,8,1.3266,3.9462,211.8513,723.1400000000001,6154977924,769372240.5,146308979.6897,8,-0.1271,0.0962,478,3
AAAAAAAAAAAAAQJ,8,0.7931,1.9182,253.9,501.13,6115379580,764422447.5,127570835.6807,8,-0.2243,0.0955,879,4
AAAAAAAAAAAAANF,8,1.3313,3.4486,182.8,511.39000000000004,6033058605,754132325.625,140730162.0912,8,-0.1509,0.0943,545,5

### `gold.trade_volume_stats` — loc=`_ivm/views/gold/trade_volume_stats`; version=0; rows=0; schema=`symbol:string, sk_security_id:string, sk_company_id:string, trade_count:long, unique_accounts:long, unique_brokers:long, active_days:long, total_volume:long, total_notional:double, avg_price:double, price_stddev:double, min_price:double, max_price:double, avg_fee:double, avg_commission:double, total_cost:double, pct_of_trades:decimal(29,4), pct_of_notional:double, volume_z_score:double, notional_z_score:double, rank_by_notional:integer, rank_by_volume:integer, rank_by_diversity:integer`

symbol,sk_security_id,sk_company_id,trade_count,unique_accounts,unique_brokers,active_days,total_volume,total_notional,avg_price,price_stddev,min_price,max_price,avg_fee,avg_commission,total_cost,pct_of_trades,pct_of_notional,volume_z_score,notional_z_score,rank_by_notional,rank_by_volume,rank_by_diversity

## tpcdi tier: staging and driver source tables

### `tpcdi.audit` — loc=`sources/audit`; version=0; rows=1; schema=`dataset:string, batchid:integer, date:date, attribute:string, value:long, dvalue:decimal(15,5)`

dataset,batchid,date,attribute,value,dvalue
Generator,1,NULL,SF,NULL,3000.00000

### `tpcdi.batch1_customer_mgmt` — loc=`sources/batch1_customer_mgmt`; version=0; rows=146; schema=`Customer:struct<Account:struct<CA_B_ID:long,CA_NAME:string,_CA_ID:long,_CA_TAX_ST:long,_VALUE:string>,Address:struct<C_ADLINE1:string,C_ADLINE2:string,C_CITY:string,C_CTRY:string,C_STATE_PROV:string,C_ZIPCODE:string>,ContactInfo:struct<C_ALT_EMAIL:string,C_PHONE_1:struct<C_AREA_CODE:long,C_CTRY_CODE:long,C_EXT:long,C_LOCAL:string>,C_PHONE_2:struct<C_AREA_CODE:long,C_CTRY_CODE:long,C_EXT:long,C_LOCAL:string>,C_PHONE_3:struct<C_AREA_CODE:long,C_CTRY_CODE:long,C_EXT:long,C_LOCAL:string>,C_PRIM_EMAIL:string>,Name:struct<C_F_NAME:string,C_L_NAME:string,C_M_NAME:string>,TaxInfo:struct<C_LCL_TX_ID:string,C_NAT_TX_ID:string>,_C_DOB:date,_C_GNDR:string,_C_ID:long,_C_TAX_ID:string,_C_TIER:long,_VALUE:string>, _ActionTS:timestamp, _ActionType:string`

Customer,\_ActionTS,\_ActionType
"{'Account': {'CA_B_ID': 10180, 'C...",2007-07-07 16:07:49+00:00,NEW
"{'Account': {'CA_B_ID': 12538, 'C...",2007-07-07 17:12:59+00:00,NEW
"{'Account': {'CA_B_ID': 12944, 'C...",2007-07-07 22:38:23+00:00,NEW
"{'Account': {'CA_B_ID': 5063, 'CA...",2007-07-08 04:38:15+00:00,NEW
"{'Account': {'CA_B_ID': 1816, 'CA...",2007-07-08 10:41:47+00:00,NEW

### `tpcdi.batch1_date` — loc=`sources/batch1_date`; version=0; rows=260; schema=`sk_dateid:long, datevalue:date, datedesc:string, calendaryearid:integer, calendaryeardesc:string, calendarqtrid:integer, calendarqtrdesc:string, calendarmonthid:integer, calendarmonthdesc:string, calendarweekid:integer, calendarweekdesc:string, dayofweeknum:integer, dayofweekdesc:string, fiscalyearid:integer, fiscalyeardesc:string, fiscalqtrid:integer, fiscalqtrdesc:string, holidayflag:boolean`

sk_dateid,datevalue,datedesc,calendaryearid,calendaryeardesc,calendarqtrid,calendarqtrdesc,calendarmonthid,calendarmonthdesc,calendarweekid,calendarweekdesc,dayofweeknum,dayofweekdesc,fiscalyearid,fiscalyeardesc,fiscalqtrid,fiscalqtrdesc,holidayflag
19500101,1950-01-01,"January 1, 1950",1950,1950,19501,1950 Q1,19501,1950 January,19501,1950-W1,7,Sunday,1950,1950,19503,1950 Q3,True
19500102,1950-01-02,"January 2, 1950",1950,1950,19501,1950 Q1,19501,1950 January,19501,1950-W1,1,Monday,1950,1950,19503,1950 Q3,False
19500103,1950-01-03,"January 3, 1950",1950,1950,19501,1950 Q1,19501,1950 January,19501,1950-W1,2,Tuesday,1950,1950,19503,1950 Q3,False
19500104,1950-01-04,"January 4, 1950",1950,1950,19501,1950 Q1,19501,1950 January,19501,1950-W1,3,Wednesday,1950,1950,19503,1950 Q3,False
19500105,1950-01-05,"January 5, 1950",1950,1950,19501,1950 Q1,19501,1950 January,19501,1950-W1,4,Thursday,1950,1950,19503,1950 Q3,False

### `tpcdi.batch1_finwire` — loc=`sources/batch1_finwire`; version=0; rows=1025; schema=`line:string, rec_type:string, pts:timestamp`

line,rec_type,pts
20170701-001402FIN201732017070120...,FIN,2017-07-01 00:14:02+00:00
20170701-003221FIN201732017070120...,FIN,2017-07-01 00:32:21+00:00
20170701-003925FIN201732017070120...,FIN,2017-07-01 00:39:25+00:00
20170701-005313FIN201732017070120...,FIN,2017-07-01 00:53:13+00:00
20170701-010828FIN201732017070120...,FIN,2017-07-01 01:08:28+00:00

### `tpcdi.batch1_hr` — loc=`sources/batch1_hr`; version=0; rows=150; schema=`employeeid:string, managerid:string, employeefirstname:string, employeelastname:string, employeemi:string, employeejobcode:string, employeebranch:string, employeeoffice:string, employeephone:string`

employeeid,managerid,employeefirstname,employeelastname,employeemi,employeejobcode,employeebranch,employeeoffice,employeephone
0,211,Ozkan,Douglas,NULL,647,EGZKSobTeknHCbLuHczvWmhTmCSGXD,OFFICE7152,(726) 088-3331
1,413,Suer,Candice,NULL,314,OfOBVvpzNvHCebxyuxXFwsMju  JRU,OFFICE8586,(344) 999-2652
2,246,Somisetty,Jami,P,534,rAHWYkktOXAyPAYHlncZPG,NULL,(984) 538-5366
3,248,Mazurek,Rosalinda,J,364,TJQqsUQQGqWG QleLheUoYlgRNVT,OFFICE8487,(860) 037-6897
4,1304,Aronovich,Delphine,M,314,IEMJHuQgCPDHCwwJkgQQeaqGvzMcVD,OFFICE9420,(604) 387-9350

### `tpcdi.batch1_industry` — loc=`sources/batch1_industry`; version=0; rows=2; schema=`in_id:string, in_name:string, in_sc_id:string`

in_id,in_name,in_sc_id
AA,Misc. Capital Goods,FN
AC,Retail (Drugs),TC

### `tpcdi.batch1_status_type` — loc=`sources/batch1_status_type`; version=0; rows=1; schema=`st_id:string, st_name:string`

st_id,st_name
ACTV,Active

### `tpcdi.batch1_tax_rate` — loc=`sources/batch1_tax_rate`; version=0; rows=4; schema=`tx_id:string, tx_name:string, tx_rate:float`

tx_id,tx_name,tx_rate
US1,U.S. Income Tax Bracket for the poor,0.15000000596046448
US2,U.S. Income Tax Bracket for the h...,0.2750000059604645
US3,U.S. Income Tax Bracket for the m...,0.3050000071525574
US4,U.S. Income Tax Bracket for the w...,0.35499998927116394

### `tpcdi.batch1_trade_history` — loc=`sources/batch1_trade_history`; version=0; rows=9822; schema=`th_t_id:long, th_dts:timestamp, th_st_id:string`

th_t_id,th_dts,th_st_id
0,2012-07-07 00:01:13+00:00,SBMT
0,2012-07-07 00:02:34+00:00,CMPT
1,2012-07-07 00:09:48+00:00,SBMT
1,2012-07-07 00:14:35+00:00,CMPT
2,2012-07-07 00:10:55+00:00,PNDG

### `tpcdi.batch1_trade_type` — loc=`sources/batch1_trade_type`; version=0; rows=1; schema=`tt_id:string, tt_name:string, tt_is_sell:integer, tt_is_mrkt:integer`

tt_id,tt_name,tt_is_sell,tt_is_mrkt
TMB,Market Buy,0,1

### `tpcdi.staging_account` — loc=`sources/staging_account`; version=2; rows=2; schema=`cdc_flag:string, cdc_dsn:long, accountid:long, ca_b_id:long, ca_c_id:long, accountdesc:string, taxstatus:byte, ca_st_id:string`

cdc_flag,cdc_dsn,accountid,ca_b_id,ca_c_id,accountdesc,taxstatus,ca_st_id
I,12890,9029,12285,4739,MEGTUPTLNkcqHeuefXAmPBOBlpRqoqq,1,ACTV
I,12860,9008,11473,4728,NULL,1,ACTV

### `tpcdi.staging_batch_date` — loc=`sources/staging_batch_date`; version=2; rows=2; schema=`batchdate:date`

batchdate
2017-07-09
2017-07-08

### `tpcdi.staging_cash_transaction` — loc=`sources/staging_cash_transaction`; version=2; rows=3621; schema=`cdc_flag:string, cdc_dsn:long, ct_ca_id:long, ct_dts:timestamp, ct_amt:double, ct_name:string`

cdc_flag,cdc_dsn,ct_ca_id,ct_dts,ct_amt,ct_name
I,1487915.0,2491,2017-07-09 10:02:43+00:00,-9204.27,UhmPrvMHxBAaAqugXnssPCIKEuJeROkJB...
I,1485223.0,1324,2017-07-08 11:34:20+00:00,8913.39,omBhNguUeU VVQTMYPc GJmIDnMXJBAgN...
NULL,NULL,0,2012-07-11 08:10:42+00:00,-37215.14,TGDRsaHPherhApDuHfXUPdexIUoEzKdgR...
NULL,NULL,11,2012-07-07 17:15:11+00:00,-3178.67,PGwhaPC igAVOmHLJppGbXaDEgHSurSvr...
NULL,NULL,12,2012-07-12 17:41:41+00:00,-3172.19,uQOUlrpDGHQpeeBGxaLTrxUwMwNMFowWA...

### `tpcdi.staging_customer` — loc=`sources/staging_customer`; version=2; rows=2; schema=`cdc_flag:string, cdc_dsn:long, customerid:long, taxid:string, status:string, lastname:string, firstname:string, middleinitial:string, gender:string, tier:byte, dob:date, addressline1:string, addressline2:string, postalcode:string, city:string, stateprov:string, country:string, c_ctry_1:string, c_area_1:string, c_local_1:string, c_ext_1:string, c_ctry_2:string, c_area_2:string, c_local_2:string, c_ext_2:string, c_ctry_3:string, c_area_3:string, c_local_3:string, c_ext_3:string, email1:string, email2:string, lcl_tx_id:string, nat_tx_id:string`

cdc_flag,cdc_dsn,customerid,taxid,status,lastname,firstname,middleinitial,gender,tier,dob,addressline1,addressline2,postalcode,city,stateprov,country,c_ctry_1,c_area_1,c_local_1,c_ext_1,c_ctry_2,c_area_2,c_local_2,c_ext_2,c_ctry_3,c_area_3,c_local_3,c_ext_3,email1,email2,lcl_tx_id,nat_tx_id
I,6455,4739,016-32-5107,ACTV,Moncur,Vittorio,NULL,M,3,1983-06-21,19452 Bryant Irvin West,NULL,H2E 1V8,Paterson,TX,United States of America,NULL,NULL,821-2946,NULL,NULL,NULL,205-8612,06614,1,968,027-5679,NULL,Vittorio.Moncur@farce.de,NULL,MD4,MT5
I,6440,4728,031-80-9744,ACTV,Kempffer,Tandie,NULL,NULL,3,1933-04-19,16132 Corsicana Road,NULL,85775,Overland Park,DC,United States of America,NULL,NULL,880-2336,NULL,NULL,266,978-6267,NULL,NULL,NULL,NULL,NULL,Tandie.Kempffer@gawab.com,Tandie.Kempffer@mail2world.com,DE2,MN1

### `tpcdi.staging_daily_market` — loc=`sources/staging_daily_market`; version=2; rows=12830; schema=`cdc_flag:string, cdc_dsn:long, dm_date:date, dm_s_symb:string, dm_close:double, dm_high:double, dm_low:double, dm_vol:integer`

cdc_flag,cdc_dsn,dm_date,dm_s_symb,dm_close,dm_high,dm_low,dm_vol
I,1284557.0,2017-07-08,AAAAAAAAAAAACCH,136.25,174.89,131.56,954467998
I,1282769.0,2017-07-07,AAAAAAAAAAAACCH,937.2,1317.08,891.53,773766563
NULL,NULL,2015-07-06,AAAAAAAAAAAABOY,242.93,284.42,185.08,111904727
NULL,NULL,2015-07-06,AAAAAAAAAAAAARP,445.46,522.3,386.48,78849320
NULL,NULL,2015-07-06,AAAAAAAAAAAACVI,910.59,1148.89,723.37,807515829

### `tpcdi.staging_holding_history` — loc=`sources/staging_holding_history`; version=2; rows=3626; schema=`cdc_flag:string, cdc_dsn:long, hh_h_t_id:integer, hh_t_id:integer, hh_before_qty:integer, hh_after_qty:integer`

cdc_flag,cdc_dsn,hh_h_t_id,hh_t_id,hh_before_qty,hh_after_qty
I,1488886.0,371146,372221,6454,0
I,1488406.0,353232,372101,5225,0
NULL,NULL,0,0,0,2939
NULL,NULL,1,1,0,9919
NULL,NULL,2,2,0,5525

### `tpcdi.staging_prospect` — loc=`sources/staging_prospect`; version=2; rows=152; schema=`agencyid:string, lastname:string, firstname:string, middleinitial:string, gender:string, addressline1:string, addressline2:string, postalcode:string, city:string, state:string, country:string, phone:string, income:string, numbercars:integer, numberchildren:integer, maritalstatus:string, age:integer, creditrating:integer, ownorrentflag:string, employer:string, numbercreditcards:integer, networth:integer`

agencyid,lastname,firstname,middleinitial,gender,addressline1,addressline2,postalcode,city,state,country,phone,income,numbercars,numberchildren,maritalstatus,age,creditrating,ownorrentflag,employer,numbercreditcards,networth
PEL0,PELLAND,Netti,NULL,f,21847 olympia street,NULL,T6b 1i1,Fairbanks,MA,United States of America,1-712-522-6088,368776,NULL,3,W,20.0,760,O,Brink's,NULL,1058868
PEL0,PELLAND,Netti,NULL,f,21847 olympia street,NULL,T6b 1i1,Fairbanks,MA,United States of America,1-712-522-6088,368776,NULL,3,W,20.0,760,O,Brink's,NULL,1058868
PEL0,PELLAND,Netti,NULL,f,21847 olympia street,NULL,T6b 1i1,Fairbanks,MA,United States of America,1-712-522-6088,368776,NULL,3,W,20.0,760,O,Brink's,NULL,1058868
LED1,LEDUC,TRUDY,NULL,F,245 congress court,APT. 624,77281,Quebec,PA,United States of America,1-626-426-4298,177967,5.0,1,U,3.0,555,U,NULL,6.0,1988185
lat2,latif,ireland,NULL,NULL,6517 frailing west,NULL,91355,Henderson,Alberta,United States of America,1-524-787-8784,321772,2.0,1,S,NULL,566,O,NULL,6.0,3673128

### `tpcdi.staging_trade` — loc=`sources/staging_trade`; version=2; rows=3912; schema=`cdc_flag:string, cdc_dsn:long, t_id:long, t_dts:timestamp, t_st_id:string, t_tt_id:string, t_is_cash:byte, t_s_symb:string, t_qty:integer, t_bid_price:double, t_ca_id:long, t_exec_name:string, t_trade_price:double, t_chrg:double, t_comm:double, t_tax:double`

cdc_flag,cdc_dsn,t_id,t_dts,t_st_id,t_tt_id,t_is_cash,t_s_symb,t_qty,t_bid_price,t_ca_id,t_exec_name,t_trade_price,t_chrg,t_comm,t_tax
U,1488885.0,372221,2017-07-09 21:56:25+00:00,SBMT,TLS,0,AAAAAAAAAAAACPT,6454,2.97,4355,2140,NULL,NULL,NULL,NULL
U,1488405.0,372101,2017-07-08 00:57:49+00:00,SBMT,TLS,0,AAAAAAAAAAAABFV,5225,4.06,428,990,NULL,NULL,NULL,NULL
NULL,NULL,0,2012-07-07 00:02:34+00:00,CMPT,TMB,0,AAAAAAAAAAAACQP,2939,9.57,0,3160,10.02,58.95,27.31,1611.19
NULL,NULL,1,2012-07-07 00:14:35+00:00,CMPT,TMB,0,AAAAAAAAAAAABHJ,9919,1.54,11,1058,1.52,19.21,30.74,3504.75
NULL,NULL,2,2012-07-08 12:15:20+00:00,CMPT,TLB,0,AAAAAAAAAAAABDK,5525,9.12,12,3477,9.35,143.51,37.2,20526.18

### `tpcdi.staging_watch_history` — loc=`sources/staging_watch_history`; version=2; rows=9003; schema=`cdc_flag:string, cdc_dsn:long, w_c_id:long, w_s_symb:string, w_dts:timestamp, w_action:string`

cdc_flag,cdc_dsn,w_c_id,w_s_symb,w_dts,w_action
I,902140.0,961,AAAAAAAAAAAAALW,2017-07-09 00:00:29+00:00,ACTV
I,900042.0,1915,AAAAAAAAAAAABTZ,2017-07-08 00:00:19+00:00,ACTV
NULL,NULL,17,AAAAAAAAAAAAAJR,2012-07-07 00:03:44+00:00,ACTV
NULL,NULL,18,AAAAAAAAAAAABSM,2012-07-07 00:04:23+00:00,ACTV
NULL,NULL,0,AAAAAAAAAAAAAQR,2012-07-07 00:09:08+00:00,ACTV

## Delta-vs-DuckDB-table relationship

In the live Spark runtime, a logical name such as `bronze.brokerage_trade` resolves through Spark catalog metadata to a Delta path like `_ivm/views/bronze/brokerage_trade/`. `DESCRIBE FORMATTED bronze.brokerage_trade` is the Spark-side confirmation when the same metastore is running. Offline, the durable evidence is the Delta log under `_ivm/views/<db>/<mv>/_delta_log/`.

These `*.db` directories do not know about Delta. They cannot be used as DuckDB catalogs to query MVs. To query rows, use Spark or Python `deltalake` against the Delta location. DuckDB can read selected Parquet files manually, but that bypasses Delta transaction semantics and is not MV-table access.

## Where to look for what

| What I want to inspect       | Where to look                                                            | How                                                                          |
| ---------------------------- | ------------------------------------------------------------------------ | ---------------------------------------------------------------------------- |
| MV row data                  | `_ivm/views/<db>/<mv>/`                                                  | `DeltaTable(...).to_pandas()` or Spark SQL                                   |
| MV catalog metadata          | `_openivm/index/rocksdb` (CF=`mv_index`)                                 | Scala via `MvCatalog.list`                                                   |
| Per-MV watermark/fingerprint | `_openivm/mvs/<safe>/rocksdb` (CF=`meta`/`properties`)                   | Scala via `MvCatalog.lookup`                                                 |
| Compiled SQL cache           | `_openivm/mvs/<safe>/rocksdb` (CF=`properties`, key=`_ivm_compiled_sql`) | Scala via `MvCatalog.lookup`                                                 |
| Base-table tracking          | Spark/Hive metastore plus warehouse dirs like `bronze.db/`               | Live Spark `SHOW TABLES` / `DESCRIBE FORMATTED`; not DuckDB in this artifact |
| Staging entries              | `_openivm/tables/<safe>/rocksdb` (CF=`staging`)                          | Scala via `StagingCatalog`                                                   |
| Staged row data              | `_ivm/staging/<base>/` and `_ivm/view_deltas/<mv>/`                      | Spark or `deltalake`                                                         |
| Raw source data              | `sources/` Delta tables and upstream raw files                           | `deltalake`, Spark, Pandas, or explicit Parquet reads                        |
| DuckDB compile state         | nowhere persistent                                                       | `duckdb :memory:` exits per compile                                          |

## Demo unified probe script

```bash
cat > inspect_tpcdi_state.py <<'PY'
from pathlib import Path
import duckdb
from deltalake import DeltaTable
BASE = Path('/home/mdrrahman/openivm-spark/.temp/ivm-bench/mount/results/3/spark-openivm')
def dpaths(root): return sorted((p.parent for p in root.rglob('_delta_log')), key=str)
def head(dt):
    try:
        return dt.to_pyarrow_dataset().head(5).to_pandas()
    except Exception:
        files = [str(x).replace("'", "''") for x in dt.file_uris()]
        if not files:
            return dt.to_pyarrow_dataset().head(0).to_pandas()
        list_sql = '[' + ','.join("'" + f + "'" for f in files) + ']'
        con = duckdb.connect(':memory:')
        try:
            return con.sql(f'select * from read_parquet({list_sql}) limit 5').df()
        finally:
            con.close()
print('# TPC-DI Spark-OpenIVM state report')
for name in ['bronze','silver','gold','tpcdi']:
    p = BASE / f'{name}.db'; print(f'## {name}.db\n- directory={p.is_dir()} file={p.is_file()}')
    try:
        con = duckdb.connect(str(p), read_only=True)
        print('- tables:', con.sql('select table_schema, table_name from information_schema.tables').fetchall())
        print('- openivm:', con.sql("select name from sqlite_master where name like 'openivm_%'").fetchall())
    except Exception as e:
        print('- duckdb_open_error:', type(e).__name__, str(e).replace('\n',' '))
for root in [BASE/'_ivm/views/bronze', BASE/'_ivm/views/silver', BASE/'_ivm/views/gold', BASE/'sources']:
    for p in dpaths(root):
        dt = DeltaTable(str(p)); df = head(dt)
        print(f'## {p.relative_to(BASE)}\n- version={dt.version()} rows={dt.to_pyarrow_dataset().count_rows()}')
        print(df.astype(str).to_markdown(index=False) if not df.empty else '_No rows._')
PY
python3 inspect_tpcdi_state.py > tpcdi-state-report.md
```

## Sharp contrast

| State item                   | Spark side                                  | Duck side in this run                  |
| ---------------------------- | ------------------------------------------- | -------------------------------------- |
| Logical databases            | Spark catalog namespaces and warehouse dirs | no DuckDB files                        |
| MV rows                      | Delta under `_ivm/views`                    | absent                                 |
| MV metadata and fingerprints | RocksDB under `_openivm`                    | absent                                 |
| Compiled refresh SQL         | cached in RocksDB properties                | transient compile output only          |
| DML staging index            | RocksDB under `_openivm/tables`             | absent                                 |
| Compiler internal state      | not persistent on Spark side                | discarded when `duckdb :memory:` exits |

Use Spark, Delta, and RocksDB readers for live-state forensics. Use DuckDB only for compile-time behavior, or for ad-hoc Parquet inspection when Delta transaction semantics are not required.
