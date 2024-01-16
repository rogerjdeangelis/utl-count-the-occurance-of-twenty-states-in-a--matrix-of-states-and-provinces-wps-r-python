%let pgm=utl-count-the-occurance-of-twenty-states-in-a--matrix-of-states-and-provinces-wps-r-python;

Count the occurance of twenty states in a large matrix of provinces wps r python

   SOLUTIONS

      1 wps sql
      2 wps r sql
      3 wps python sql
      4 wps r no sql

github
http://tinyurl.com/yc4yxxhs
https://github.com/rogerjdeangelis/utl-count-the-occurance-of-twenty-states-in-a--matrix-of-states-and-provinces-wps-r-python

/************************************************************************************************** ************************/
/*                                                 |                                                |                      */
/*          INPUTS                                 |      PROCESS (workd in WPS R & Python)         |    OUTPUT            */
/*                                                 |                                                |                      */
/* SD1.STATES |      SD1.PROVINCES                 | STATES THAT ARE ALSO PROVINCES                 | SD1.INTERSECT        */
/* ========== | =========================          | =============================                  |                      */
/*   STATE    |  A1  A2  A3  A4  A5  A6  A7  A8    |  select                                        |                      */
/*            |                                    |      l.state                                   |  STATE CNT           */
/*    AL      |  EQ  WQ  WU  RN  OR  CY  MR  MR    |     ,count(r.province) as state_count          |                      */
/*    AK      |  UI  XV  AK  EB  EJ  XQ  VW  IM    |  from                                          |   AK   1             */
/*    AZ      |  YO  KL  LJ  RU  YU  NO  RJ  RL    |     sd1.states as l                            |   AR   1             */
/*    AR      |  OU  FP  YM  EV  RT  RO  DY  DW    |     left join                                  |   CO   2 ** see      */
/*    CA      |  WE  LV  TT  XE  NX  LW  JI  ZO    |      (                                         |   DE   1  input      */
/*    CO      |  BI  RD  WK  BK  WV  PS  SX  PI    |        %do_over                                |   FL   1             */
/*    CT      |  IG  DT  DA  GX  FF  XB  MX  SF    |         (_vs,phrase=select ? as province       |   IL   1             */
/*    DE      |  HA  GZ  JJ  GF  IY  XL  EA  ZX    |          from sd1.provinces,between=union all) |   MD   1             */
/*    FL      |  GO  FE  HK  NX  BK  BD  WZ  XW    |      ) as r                                    |                      */
/*                               --                |  on                                            |                      */
/*    GA      |  YE  WG  PL  UD  CO* AG  BW  YO    |     l.state = r.province                       |                      */
/*                               ==                |  group                                         |                      */
/*    HI      |  FK  NV  WW  QR  GG  KV  WX  VJ    |     by l.state                                 |                      */
/*    ID      |  ZW  XW  MQ  GY  XS  ER  SG  QC    |  having                                        |                      */
/*    IL      |  JK  TK  NV  FE  WK  VQ  PW  IP    |     count(r.province) > 0                      |                      */
/*    IN      |  KD  PO  MH  UK  QD  IE  MY  IL    |                                                |                      */
/*    IA      |  LF  SZ  MX  OP  HP  IZ  PL  LM    |  R NO SQL                                      |                      */
/*    KS      |  RE  AR  BV  SU  KR  LD  KG  LY    |  ========                                      |                      */
/*                                           --    |  statefrq<-data.frame(table(unlist(provinces)))|                      */
/*    KY      |  QD  KI  JJ  DE  JN  EA  DL  CO* 2 |   %>% setNames(c("STATE","STATE_COUNT"));      |                      */
/*                                           --    |  want <- left_join(states,statefrq,by="STATE") |                      */
/*    LA      |  FH  LH  FL  PO  EP  MG  MS  HJ    |   %>% filter(STATE_COUNT > 0);                 |                      */
/*    ME      |  WY  TY  JR  CF  MD  IC  XP  YV    |                                                |                      */
/*    MD      |  OB  GW  IK  HP  OY  BE  BI  FX    |                                                |                      */
/*            |                                    |                                                |                      */
/***************************************************************************************************************************/


options validvarname=upcase;
libname sd1 "d:/sd1";
data sd1.states;
 input state $2.;
cards4;
AL
AK
AZ
AR
CA
CO
CT
DE
FL
GA
HI
ID
IL
IN
IA
KS
KY
LA
ME
MD
;;;;
run;quit;

data sd1.provinces;
 informat a1-a8 $2.;
 input a1-a8;
cards4;
EQ WQ WU RN OR CY MR MR
UI XV AK EB EJ XQ VW IM
YO KL LJ RU YU NO RJ RL
OU FP YM EV RT RO DY DW
WE LV TT XE NX LW JI ZO
BI RD WK BK WV PS SX PI
IG DT DA GX FF XB MX SF
HA GZ JJ GF IY XL EA ZX
GO FE HK NX BK BD WZ XW
YE WG PL UD CO AG BW YO
FK NV WW QR GG KV WX VJ
ZW XW MQ GY XS ER SG QC
JK TK NV FE WK VQ PW IP
KD PO MH UK QD IE MY IL
LF SZ MX OP HP IZ PL LM
RE AR BV SU KR LD KG LY
QD KI JJ DE JN EA DL CO
FH LH FL PO EP MG MS HJ
WY TY JR CF MD IC XP YV
OB GW IK HP OY BE BI FX
;;;;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*               INPUTS                                                                                                   */
/*                                                                                                                        */
/*  SD1.STATES  |        SD1.PROVINCES                                                                                    */
/*  =========== |   =========================                                                                             */
/*    STATE     |    A1  A2  A3  A4  A5  A6  A7  A8                                                                       */
/*              |                                                                                                         */
/*     AL       |    EQ  WQ  WU  RN  OR  CY  MR  MR                                                                       */
/*     AK       |    UI  XV  AK  EB  EJ  XQ  VW  IM                                                                       */
/*     AZ       |    YO  KL  LJ  RU  YU  NO  RJ  RL                                                                       */
/*     AR       |    OU  FP  YM  EV  RT  RO  DY  DW                                                                       */
/*     CA       |    WE  LV  TT  XE  NX  LW  JI  ZO                                                                       */
/*     CO       |    BI  RD  WK  BK  WV  PS  SX  PI                                                                       */
/*     CT       |    IG  DT  DA  GX  FF  XB  MX  SF                                                                       */
/*     DE       |    HA  GZ  JJ  GF  IY  XL  EA  ZX                                                                       */
/*     FL       |    GO  FE  HK  NX  BK  BD  WZ  XW                                                                       */
/*                                   --                                                                                   */
/*     GA       |    YE  WG  PL  UD  CO* AG  BW  YO                                                                       */
/*                                   ==                                                                                   */
/*     HI       |    FK  NV  WW  QR  GG  KV  WX  VJ                                                                       */
/*     ID       |    ZW  XW  MQ  GY  XS  ER  SG  QC                                                                       */
/*     IL       |    JK  TK  NV  FE  WK  VQ  PW  IP                                                                       */
/*     IN       |    KD  PO  MH  UK  QD  IE  MY  IL                                                                       */
/*     IA       |    LF  SZ  MX  OP  HP  IZ  PL  LM                                                                       */
/*     KS       |    RE  AR  BV  SU  KR  LD  KG  LY                                                                       */
/*                                               --                                                                       */
/*     KY       |    QD  KI  JJ  DE  JN  EA  DL  CO* 2                                                                    */
/*                                               --                                                                       */
/*     LA       |    FH  LH  FL  PO  EP  MG  MS  HJ                                                                       */
/*     ME       |    WY  TY  JR  CF  MD  IC  XP  YV                                                                       */
/*     MD       |    OB  GW  IK  HP  OY  BE  BI  FX                                                                       */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*                                  _
/ | __      ___ __  ___   ___  __ _| |
| | \ \ /\ / / `_ \/ __| / __|/ _` | |
| |  \ V  V /| |_) \__ \ \__ \ (_| | |
|_|   \_/\_/ | .__/|___/ |___/\__, |_|
             |_|                 |_|
*/

%array(_vs,values=%utl_varlist(sd1.provinces));

%put &=_vs3 ;  /*  _VS3=A3 */
%put &=_vsn ;  /*  _VSN=8  */

proc datasets lib=sd1 nolist mt=data mt=view nodetails;delete want; run;quit;

%utl_submit_wps64x("
libname sd1 'd:/sd1';
options validvarname=any;
proc sql;
 create
     table sd1.want as
 select
     l.state
    ,count(r.province) as state_count
 from
    sd1.states as l
    left join
       (
         %do_over
          (_vs,phrase=select ? as province
             from sd1.provinces,between=union all)
       ) as r
 on
    l.state = r.province
 group
    by l.state
 having
    count(r.province) > 0
;quit;

proc print;
run;quit;
");

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  The WPS System                                                                                                        */
/*                                                                                                                        */
/*  Obs    STATE    STATE_COUNT                                                                                           */
/*                                                                                                                        */
/*   1      AK           1                                                                                                */
/*   2      AR           1                                                                                                */
/*   3      CO           2                                                                                                */
/*   4      DE           1                                                                                                */
/*   5      FL           1                                                                                                */
/*   6      IL           1                                                                                                */
/*   7      MD           1                                                                                                */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                                          _
|___ \  __      ___ __  ___   _ __   ___  __ _| |
  __) | \ \ /\ / / `_ \/ __| | `__| / __|/ _` | |
 / __/   \ V  V /| |_) \__ \ | |    \__ \ (_| | |
|_____|   \_/\_/ | .__/|___/ |_|    |___/\__, |_|
                 |_|                        |_|
*/

proc datasets lib=sd1 nolist mt=data mt=view nodetails;delete want; run;quit;

options validvarname=any;
libname sd1 "d:/sd1";

%utl_submit_wps64x("
libname sd1 'd:/sd1';
proc r;
export data=sd1.states    r=states   ;
export data=sd1.provinces r=provinces;
submit;
library(sqldf);
want <-sqldf('
 select
     l.state
    ,count(r.province) as state_count
 from
    states as l
    left join
       (
         %do_over
          (_vs,phrase=select ? as province from provinces,between=union all)
       ) as r
 on
    l.state = r.province
 group
    by l.state
 having
    count(r.province) > 0
');
want;
endsubmit;
import data=sd1.want r=want;
");

proc print data=sd1.want;;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS R System                                                                                                       */
/*                                                                                                                        */
/*   STATE state_count                                                                                                    */
/* 1    AK           1                                                                                                    */
/* 2    AR           1                                                                                                    */
/* 3    CO           2                                                                                                    */
/* 4    DE           1                                                                                                    */
/* 5    FL           1                                                                                                    */
/* 6    IL           1                                                                                                    */
/* 7    MD           1                                                                                                    */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*____                                    _   _                             _
|___ /  __      ___ __  ___   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
  |_ \  \ \ /\ / / `_ \/ __| | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
 ___) |  \ V  V /| |_) \__ \ | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
|____/    \_/\_/ | .__/|___/ | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
                 |_|         |_|    |___/                                |_|
*/

%utlfkil(d:/xpt/want.xpt);

/*----                                                                   ----*/
/*----  elimiate issues with datastep and view with the same name        ----*/
/*----                                                                   ----*/

proc datasets lib=sd1 nolist mt=data mt=view nodetails;delete want; run;quit;

%utl_submit_wps64x("
options validvarname=any lrecl=32756;
libname sd1 'd:/sd1';
proc python;
export data=sd1.states      python=states;
export data=sd1.provinces   python=provinces;
submit;
import pyreadstat as ps;
from os import path;
import pandas as pd;
import numpy as np;
from pandasql import sqldf;
mysql = lambda q: sqldf(q, globals());
from pandasql import PandaSQL;
pdsql = PandaSQL(persist=True);
sqlite3conn = next(pdsql.conn.gen).connection.connection;
sqlite3conn.enable_load_extension(True);
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll');
mysql = lambda q: sqldf(q, globals());
want = pdsql('''
 select
     l.state
    ,count(r.province) as state_count
 from
    states as l
    left join
       (
         %do_over
          (_vs,phrase=select ? as province from provinces,between=union all)
       ) as r
 on
    l.state = r.province
 group
    by l.state
 having
    count(r.province) > 0
''');
print(want);
ps.write_xport(want,'d:\\xpt\\want.xpt',table_name='want',file_format_version=5
,column_labels=['STATE','STATE_COUNT']);
endsubmit;
");

/*--- handles long variable names by using the label to rename the variables  ----*/

libname xpt xport "d:/xpt/want.xpt";
proc contents data=xpt._all_;
run;quit;

data want_py_long_names;
  %utl_rens(xpt.want) ;
  set want;
run;quit;
libname xpt clear;

proc print data=want_py_long_names;
run;quit;

/**************************************************************************************************************************/
/*                       |                                                                                                */
/*    The WPS PYTHON     |   WPS  (Note long variable name fro V5 export file)                                            */
/*                       |          STATE_                                                                                */
/*    STATE  state_count |   Obs    STATE     COUNT                                                                       */
/*                       |                                                                                                */
/*  0    AK            1 |    1      AK         1                                                                         */
/*  1    AR            1 |    2      AR         1                                                                         */
/*  2    CO            2 |    3      CO         2                                                                         */
/*  3    DE            1 |    4      DE         1                                                                         */
/*  4    FL            1 |    5      FL         1                                                                         */
/*  5    IL            1 |    6      IL         1                                                                         */
/*  6    MD            1 |    7      MD         1                                                                         */
/*                       |                                                                                                */
/**************************************************************************************************************************/

/*  _                                                         _
| || |   __      ___ __  ___   _ __   _ __   ___    ___  __ _| |
| || |_  \ \ /\ / / `_ \/ __| | `__| | `_ \ / _ \  / __|/ _` | |
|__   _|  \ V  V /| |_) \__ \ | |    | | | | (_) | \__ \ (_| | |
   |_|     \_/\_/ | .__/|___/ |_|    |_| |_|\___/  |___/\__, |_|
                  |_|                                      |_|
*/

proc datasets lib=sd1 nolist mt=data mt=view nodetails;delete want; run;quit;

%utl_submit_wps64('
libname sd1 "d:/sd1";
proc r;
export data=sd1.states    r=states   ;
export data=sd1.provinces r=provinces;
submit;
library(magrittr);
library(dplyr);
statefrq <- data.frame(table(unlist(provinces))) %>% setNames(c("STATE","STATE_COUNT"));
want <- left_join(states, statefrq, by="STATE") %>% filter(STATE_COUNT > 0);
want;
endsubmit;
import r=want data=sd1.want;
run;quit;
');

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  The WPS R System                                                                                                      */
/*                                                                                                                        */
/*    STATE STATE_COUNT                                                                                                   */
/*                                                                                                                        */
/*  1    AK           1                                                                                                   */
/*  2    AR           1                                                                                                   */
/*  3    CO           2                                                                                                   */
/*  4    DE           1                                                                                                   */
/*  5    FL           1                                                                                                   */
/*  6    IL           1                                                                                                   */
/*  7    MD           1                                                                                                   */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
