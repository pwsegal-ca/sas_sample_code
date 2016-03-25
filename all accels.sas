/* define connection to TD box */


libname td teradata server=127.0.0.1 user=me password=me  database=namedata;


%let idconn= server=127.0.0.1 user=me password=me mode=teradata;


/* turn on sql tracing*/

Options sastrace=',,,ds' sastraceloc=saslog nostsuffix;

/*ensure all that can be passed through to TD is actually sent into db*/

options dbidirectexec;

/*********************************************
*                                            *
*           Data Quality Accelerator         *
*                                            *
*********************************************/
/* data quality checks on customer info*/
proc sql;
select count(*)
from td.cust_vw;
quit;

proc sql;
select * from td.cust_vw
where cid between 500000 and 500100;
quit;


/****************

generate the gender codes based on the customers name

*****************/

proc sql;
connect to teradata(&idconn);
execute (drop table namedata.cust_gend) by teradata;
execute (
          call sas_sysfnlib.dq_gender(
          'Name',
          'namedata.cust_vw',
          'fullname',
          'cid',
          'namedata.cust_gend',
          'ENUSA')
        ) by teradata;
        
quit;

/************

look at the frequency of the results
we have U for undetermined due to possible non-Anglo names
or gender neutral names like Lindsey

************/
proc freq data=td.cust_gend;
tables gender;
run;

/***********

look at the state
we have mostly 2 char codes 
except for California and New York

***********/
proc freq data=td.cust_vw;
tables state;
run;

/**************

Standardise all state information
down to 2 character code

***************/

proc sql;
connect to teradata(&idconn);
execute (drop table namedata.cust_state_std) by teradata;
execute (
          call sas_sysfnlib.dq_standardize(
          'State/Province (Abbreviation)',
          'namedata.cust_vw',
          'state',
          'cid',
          'namedata.cust_state_std',
          'ENUSA')
        ) by teradata;
        
quit;


proc freq data=td.cust_state_std;
tables standardized;
run;




/* look at base table*/
proc sql;
select * from td.txn_12m
where id <=10;
quit;

proc sql;
select count(*) from td.txn_12m;
quit;


/* implicit proc sql running inside database */

/* extract month from the base data in txn and put into a table*/
proc sql;
insert into td.txn_mnth
select id, month(dt), t1,t2,t3,t4,t5
from td.txn_12m;
quit;

proc sql;
select * from td.txn_mnth
where id <10;
quit;



/* have a look at the data pushes means into DB*/
proc means data=td.txn_mnth;
run;


/*********************************************
*                                            *
*           Code Accelerator                 *
*                                            *
*********************************************/


/* ****************************************
     create ds2 Program to denorm table
      takes base table from 365 000 000
      to 1 000 000. Rolled up to 
      unique id, with sums of T1-T5 across months
********************************************/

proc ds2 ds2accel=yes;

  /* thread program defines the parallel logic to run on each Teradata AMP */
  thread work.p_thread / overwrite=yes;
	vararray double T1_c[12];   /* create arrays to hold the pivoted data*/
	vararray double T2_c[12];
    vararray double T3_c[12];
    vararray double T4_c[12];
    vararray double T5_c[12];
    dcl double T1_ave;
	dcl  integer id;            /* this is the group by variable id */
	
	keep id T1_ave T1_c1-T1_c12 T2_c1-T2_c12 T3_c1-T3_c12 T4_c1-T4_c12 T5_c1-T5_c12 ; 
	retain T1_Ave T1_c1-T1_c12 T2_c1-T2_c12 T3_c1-T3_c12 T4_c1-T4_c12 T5_c1-T5_c12;

	method clear_array();      /* zero out the arrays */
      dcl  float i; 
      do i=1 to 12 ; 
     	T1_c[i] = 0;  
     	T2_c[i] = 0;
        T3_c[i] = 0;
        T4_c[i] = 0;
        T5_c[i] = 0; 
  	  end; 
	end;

	method run(); 
  	  set td.txn_mnth;    /* read data in from txn_mnth table in TD */ 
      by id;              /* the role up level */
  	  if first.id then    /* for each new id clear out the arrays as these are reused */
        clear_array(); 
        T1_c[mnth]=T1+T1_c[mnth];  /* for each month create the totals */
		T2_c[mnth]=T2+T2_c[mnth];
		T3_c[mnth]=T3+T3_c[mnth];
		T4_c[mnth]=T4+T4_c[mnth];
		T5_c[mnth]=T5+T5_c[mnth];
		T1_ave=(T1_c1+T1_c2+T1_c3+T1_c4+T1_c5+T1_c6+T1_c7+T1_c8+T1_c9+T1_c10+T1_c11+T1_c12)/12.00;
      if last.id then              /* then write out the results */ 
        output; 
    end;
  endthread;
  run;

  /* Execute the DS2 we wrote above */
  data td.txn_pvt;         /* results are going into a teradata table*/
    dcl thread p_thread p; /* instance of the thread */
    method run();          /* call the run method we created above */
      set from p;
      output;
    end;
  enddata;

run;
quit;

proc sql ;
select * from td.txn_pvt
where id<=10;
quit;


/*explicit proc sql to build up % change over quarters
could do this in ds2 but want to show both methods 
being used to prep the data */
proc sql;
connect to teradata(&idconn);
execute (
 create table namedata.txn_pc 
        as (
             select id,
			(T1_c3-T1_c1)/T1_c1 as t1_1_3,
			(T1_c6-T1_c3)/T1_c3 as t1_3_6,
			(T1_c9-T1_c6)/T1_c6 as t1_6_9,
			(T1_c12-T1_c9)/T1_c9 as t1_9_12,
			(T2_c3-T2_c1)/T2_c1 as t2_1_3,
			(T2_c6-T2_c3)/T2_c3 as t2_3_6,
			(T2_c9-T2_c6)/T2_c6 as t2_6_9,
			(T2_c12-T2_c9)/T2_c9 as t2_9_12,
			(T3_c3-T3_c1)/T3_c1 as t3_1_3,
			(T3_c6-T3_c3)/T3_c3 as t3_3_6,
			(T3_c9-T3_c6)/T3_c6 as t3_6_9,
			(T3_c12-T3_c9)/T3_c9 as t3_9_12,
			(T4_c3-T4_c1)/T4_c1 as t4_1_3,
			(T4_c6-T4_c3)/T4_c3 as t4_3_6,
			(T4_c9-T4_c6)/T4_c6 as t4_6_9,
			(T4_c12-T4_c9)/T4_c9 as t4_9_12,
			(T5_c3-T5_c1)/T5_c1 as t5_1_3,
			(T5_c6-T5_c3)/T5_c3 as t5_3_6,
			(T5_c9-T5_c6)/T5_c6 as t5_6_9,
			(T5_c12-T5_c9)/T5_c9 as t5_9_12
			from namedata.txn_pvt
			) 
			with data unique primary index(id)) by teradata;

disconnect from teradata;
quit;

proc means data=td.txn_pvt;
run;

/* 
implicit proc sql, looks like it creates a sas view
but since we have dbidirectexec enable it pushes everything to DB
including view creation
*/

proc sql;
create view td.ads_p1
as (select 
cid,
    Title_,
    GivenName,
    MiddleInitial,
    Surname,
    fullname,
    gender,
    StreetAddress,
    City,
    standardized as State,
    ZipCode,
    Country,
    EmailAddress,
    TelephoneNumber,
    Birthday,
    CCType,
    Occupation,
    Company,
    Vehicle,
    m1,
	T1_c1,
    T1_c2,
    T1_c3,
    T1_c4,
    T1_c5,
    T1_c6,
    T1_c7,
    T1_c8,
    T1_c9,
    T1_c10,
    T1_c11,
    T1_c12,
    T2_c1,
    T2_c2,
    T2_c3,
    T2_c4,
    T2_c5,
    T2_c6,
    T2_c7,
    T2_c8,
    T2_c9,
    T2_c10,
    T2_c11,
    T2_c12,
    T3_c1,
    T3_c2,
    T3_c3,
    T3_c4,
    T3_c5,
    T3_c6,
    T3_c7,
    T3_c8,
    T3_c9,
    T3_c10,
    T3_c11,
    T3_c12,
    T4_c1,
    T4_c2,
    T4_c3,
    T4_c4,
    T4_c5,
    T4_c6,
    T4_c7,
    T4_c8,
    T4_c9,
    T4_c10,
    T4_c11,
    T4_c12,
    T5_c1,
    T5_c2,
    T5_c3,
    T5_c4,
    T5_c5,
    T5_c6,
    T5_c7,
    T5_c8,
    T5_c9,
    T5_c10,
    T5_c11,
    T5_c12,
    T1_ave,
	 t1_1_3,
    t1_3_6,
    t1_6_9,
    t1_9_12,
    t2_1_3,
    t2_3_6,
    t2_6_9,
    t2_9_12,
    t3_1_3,
    t3_3_6,
    t3_6_9,
    t3_9_12,
    t4_1_3,
    t4_3_6,
    t4_6_9,
    t4_9_12,
    t5_1_3,
    t5_3_6,
    t5_6_9,
    t5_9_12
from td.txn_pvt as t 
inner join td.cust as c 
      on t.id=c.cid 
inner join td.txn_pc as p 
      on c.cid=p.id
inner join td.cust_gend as cg 
      on c.cid=cg._pk_
inner join td.cust_state_std as css
      on c.cid=css._pk_     
      );
quit;

proc sql ;
select * from td.ads_p1
where cid<10;
quit;



/*define a user format*/

proc format ;
value txn_cat
0-90 = 'cat1'
90-100 = 'cat2'
100-120 = 'cat3'
120-150 = 'cat4'
other='cat5';
run;

/*needed for format publish macro */
%let indconn=server=127.0.0.1 user=me password=me database=namedata;

/* now publish this format to the database */

/*initialise the format publishing system*/
%indtdpf;

/*now send the formats to the DB as VDFs*/
%indtd_publish_formats (fmtcat=work, 
database=namedata, 
fmttable=sas_formats, 
action=replace, 
mode=protected);

/*use this format in a proc freq
notice in the log we see the SQL
the format has been written as SQL in the where clause
as a case statement
This is faster that having to call a VDF.
If the Access engine is unable to rewrite the SQL to
accomodate the format, it will call the sas_put VDF */

proc freq data=td.ads_p1;
format t1_ave txn_cat.;
tables t1_ave * state;
run;

/* run a linear regression using the maxr2 selection method
Notice in log we have SQL used message
Generating the inv(X'X) matrix in db 
*/

/*********************************************
*                                            *
*           Analytics Accelerator            *
*                                            *
*********************************************/



/* this was the model we decided on 
so run it and store the estimated coeff
in work.regtest*/

proc reg data=td.ads_p1 outest=work.regest;
model m1=t1_1_3 t1_3_6 t1_6_9 t1_9_12 t2_1_3 t2_3_6 t2_6_9 t2_9_12 t3_1_3 t3_3_6 t3_6_9 t3_9_12 t4_1_3 t4_3_6 t4_6_9 t4_9_12 t5_1_3 t5_3_6 t5_6_9 t5_9_12 /selection = maxr;
run;

/* score using proc score in database*/

proc score data=td.ads_p1 out=td.ads_p1_linest score=work.regest type=parms;
var t1_1_3 t1_3_6 t1_6_9 t1_9_12 t2_1_3 t2_3_6 t2_6_9 t2_9_12 t3_1_3 t3_3_6 t3_6_9 t3_9_12 t4_1_3 t4_3_6 t4_6_9 t4_9_12 t5_1_3 t5_3_6 t5_6_9 t5_9_12;
run;

proc sql;
select * from td.ads_p1_linest
where cid<10;
quit;


/*********************************************
*                                            *
*           Scoring Accelerator              *
*                                            *
*********************************************/



/* we have EM model to do clustering of data

can look at the EM project if you wish
point browser to durham1.labs.teradata.com:7980/SASEnterpriseMinerJWS/Status
click the launch button
this will download the Java WebStart EM object
once downloaded click on the main.jlp which will start EM
login as sasdemo/sasdemo
run clustering (5 clusters), and exports code
to /home/sasdemo/SGF2015/SA/sasdemo

*/


/* when using EP, indconn MUST have database defined, even if we overwrite it later*/

%let indconn= server=127.0.0.1 user=me password=me database=namedata;

/* publish SA code */
%indtdpm;

%indtd_create_modeltable(
database=namedata,
modeltable=test_models,
action=replace);

%indtd_publish_model (
dir=/home/sasdemo/SGF2015/SA/sasdemo,
modelname=clustering,
modeltable=test_models,
action=replace,
mechanism=EP);

/* show foreign server def*/
proc sql;
connect to teradata (&indconn mode=teradata);
select * from connection to teradata (
show foreign server apex) ;
disconnect from teradata;
quit;

proc sql;
connect to teradata (&indconn mode=teradata);
execute (
replace view namedata.cluster_hy 
as (select * from sase2eh.cl_prep_out_hdp@apex)) by teradata;
disconnect from teradata;
quit;

/* score using SA and Querygrid pulling data 
from Hadoop and storing scores in TD*/

proc sql noerrorstop;
connect to teradata (&indconn mode=teradata);
execute (
call sas_sysfnlib.sas_score_ep 
                       ( 'MODELTABLE=namedata.test_models',
                         'MODELNAME=clustering',
                         'INQUERY=namedata.cluster_hy',
                         'OUTTABLE=namedata.clustered_out',
                         'OUTKEY=s_ky',
                         'OPTIONS=VOLATILE=NO;UNIQUE=YES;DIRECT=YES;'
                        )
          ) by teradata;
          disconnect from teradata;
quit;
      
             
proc sql;
select * from td.clustered_out
where s_ky <=10;
quit;

proc freq data=td.clustered_out;
tables _segment_label_;
run;



