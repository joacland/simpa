options linesize = 96;

libname home '~'; * The home directory;
libname wrk '~/wrk'; * The working directory;
libname links '~/links'; * My links directory;
libname temp '/sastemp2/joacland'; * My temp directory;

%let FUNDA = comp.FUNDA;
%let FUNDQ = comp.FUNDQ;
%let IBES = ibes.StatSum_EpsUs;

%global BEGINDATE ENDDATE ACTU DETU COMP_LIST LM_FILTER
        IBES_VARS IBES_WHERE1 IBES_WHERE2 COMP_WHERE; 

/*define a set of auxiliary macros;*/  
*%include '/wrds/ibes/samples/cibeslink.sas'; 
*%include '/wrds/ibes/samples/ibes_sample.sas'; 
%include '~/sue.sas'; 
%include '~/size_bm.sas'; 
%include '/wrds/crsp/samples/crspmerge.sas' ;

***** Input Area: ***********************************************************;
*%let BDATE = 01jan1963;  
%let BDATE = 01jan2010;		* start calendar date of fiscal period end;
%let EDATE = 31mar2013;		* end calendar date of fiscal period end;
%let ACTU = ibes.actu_epsus;	* name of IBES dataset containing unadjusted actuals;
%let DETU = ibes.detu_epsus;	* name of IBEs dataset containing unadjusted estimates;
%let BEGINDATE = %sysfunc(putn("&BDATE"d,5.)); 
%let ENDDATE = %sysfunc(putn("&EDATE"d,5.)); 

* Year ranges -- applied to FYEAR (Fiscal Year); 
%let BFYEAR = year(&BEGINDATE);
%let EFYEAR = year(&ENDDATE) - 1;
* Year ranges -- applied to calendar year; 
%let BCYEAR = year(&BEGINDATE);
%let ECYEAR = year(&ENDDATE);

*  variables to extract from FUNDA (gvkey, datadate, fyear and fyr
are always included);

%let VARBASE = indfmt datafmt popsrc consol curcd;

%let VARS = act aoloch apalch at ceq che csho dd1 dlc dltt dp dpc dv dvc
dvp dvpsx_f dvsco dvt gdwl ib ibadj ibc ibcom intan invch itcb ivao lct lse lt
mib mibt mibn naicsh oancf oiadp ppent ppegt prstkc pstk pstkl pstkn pstkr
pstkrv recch sale seq sich sstk txach txdb txdi txditc txp xidoc xint;

*  variables to extract from FUNDQ (gvkey and datadate are always
included);
%let VARQ = datafqtr fyearq fqtr rdq prccq mkvaltq;

/*Variables to extract from Compustat*/
%let COMP_LIST= gvkey fyearq fqtr conm datadate rdq epsfxq epspxq prccq ajexq spiq 
cshoq cshprq cshfdq rdq saleq atq fyr consol indfmt datafmt popsrc datafqtr; 
  
/*Variables to extract from IBES*/
%let IBES_VARS= ticker value fpedats anndats revdats measure fpi estimator 
                analys pdf usfirm; 
  
/*IBES filters. TODO: FPI should not be 6 and 7, but 1 and 2 since 'ANN'.
Should I use FPI>2?
*/
%let IBES_WHERE1=
 where=(measure='EPS' 
	and fpi in ('1','2','3','4','5') 
	and &BEGINDATE <= fpedats <= &ENDDATE); 
%let IBES_WHERE2=
 where=(missing(repdats)=0 
	and missing(anndats)=0); 
*	and 0 < intck('day',anndats, repdats) <= 90); 

* CRSP variables to extract;
%let DSEVARS = TICKER NCUSIP SHRCD EXCHCD;
%let DSFVARS = VOL PRC RET RETX SHROUT CFACPR CFACSHR;

*****  Macro   **************************************************;
%MACRO IBES_DATA (infile=, ibes1_where=, ibes2_where=, ibes_var=);
/* Adapted version of IBES_SAMPLE.SAS 
It uses my own link table rather than WRDS. 
It is based on the 'ANN' estimates rather than 'QTR' 
It gets forecasts not older than 90 days after the annual report
is assumed public (90 days after end of fiscal year)
*/
%put ; %put ; %put ; %put ; %put ; 
%put #### ## # Extracting IBES file with median estimates # ## #### ;
%put #### ### ## # ;
options nonotes;
proc sql; 
    create table ibes (drop=measure)
	as select *
	from &DETU (&IBES1_WHERE keep=&IBES_VAR) as a,
	    /* IBES1_WHERE and IBES_VAR are specified*/
	&INFILE as b   /* prior to invoking IBES_DATA*/
	    where a.ticker = b.ticker
    order by a.ticker, fpedats, estimator, analys, anndats, revdats;
quit;

data ibes;
    set ibes;
    /* No updates to IBES estimates are allowed once the annual report is assumed public.
    Since actuals are published approx a month after fiscal year end, the method below 
    allows for approx two months where the analysts can update their estimates. */
    revdatsend= intnx('day',fpedats,90); 
    /* Moves the date 90 days forward to establish the three month distance
    between fiscal year end and assumed publication date */
    fpbdats = intnx('year',revdatsend,-fpi, "sameday"); 
    /* Moves the date fpi-years back to establish the last date when forecasts can be 
    revised. 
    Since fpedats is the same as fiscal year end, this method also establishes the 
    assumed publication date */
    fpbdats = intnx('month',fpbdats,0, "end"); 
    /* Moves the date to the end of the month (unless it already is at the month end) */
    format revdatsend fpbdats date9.;
run;

data ibes;
    set ibes (drop=revdatsend);
    where revdats <= fpbdats;
    forcdats = coalesce(revdats, anndats);
    /* Uses the latest date since anndats is not updated unless
    there is a change in the forecast. No change in in the forecast 
    will however lead to an updated revdats (last contact between 
    IBES and analyst) */
    format forcdats date9.;
run;

* Select the last fpi-forecasts made within 90 days of the annual report publication date
(forecast period beginning data, fpbdats); 
proc sql;
    create table ibes
	as select *
	from ibes
	where 0 < intck('day',forcdats, fpbdats) <= 90
    order by ticker, fpbdats, fpi, estimator, analys, forcdats;
quit;

*Select the last fpi-estimate for a firm within broker-analyst group at a certain annual 
report publication date;
data ibes; set ibes;
    by ticker fpbdats fpi estimator analys;
    if last.analys;
run;

*How many estimates are reported, per fpbdats and fpi, on primary/diluted basis?;
%put #### ### ## # ;
proc sql; 
    create table ibes 
	as select a.*, sum(pdf='P') as p_count, sum(pdf='D') as d_count
	from ibes as a
    group by ticker,fpbdats,fpi;

/* a. Link unadjusted estimates with unadjusted actuals and CRSP permnos */
/* b. Adjust report and estimate dates to be CRSP trading days 
Note that forcdats below is the latest of the announcement date and the
revision date of the estimates, and that repdats is the annoucement date 
of the actuals. */
    create table ibes1 (where=(missing(repdats)=0 
			and missing(anndats)=0))
	as select a.*, b.anndats as repdats, b.value as actuals, c.permno,
	case when weekday(a.forcdats) = 1 then intnx('day',a.forcdats,-2)
		/* Moves estimates earlier */
		/*if sunday move back by 2 days;*/
	when weekday(a.forcdats) = 7 then intnx('day',a.forcdats,-1) 
	    else a.forcdats
		/*if saturday move back by 1 day*/
	end as estdats1,
	case when weekday(b.anndats) = 1 then intnx('day',b.anndats,1)
		/* Moves actuals forward: est data < act date */
		/*if sunday move forward by 1 day  */
	when weekday(b.anndats) = 7 then intnx('day',b.anndats,2) 
	    else b.anndats
		/*if saturday move forward by 2 days*/
	end as repdats1
	from	ibes as a, 
		&ACTU as b,
		links.iclink as c
	where a.ticker = b.ticker
	    and a.fpedats -5 <= b.pends <= a.fpedats +5 
	    /* Allows some descrepancy (10 days) between the
	    period end dates in the two tables. Probably not 
	    necessary but just adds some precaution */
	    /*and a.fpedats = b.pends */ 
	    and a.usfirm = b.usfirm 
	    and b.pdicity = 'ANN' /* Using ANN instead of QTR? */ 
	    and b.measure = 'EPS'
	    and a.ticker = c.ticker
	    and c.score in (0,1,2);

/* Making sure that estimates and actuals are on the same basis
1. retrieve CRSP cumulative adjustment factor for IBES report and estimate dates */
%put #### ### ## # ;
    create table adjfactor
	as select distinct a.*
    from    crsp.dsf (keep = permno date cfacshr) as a,
	    ibes1 as b
    where a.permno = b.permno 
	and (a.date = b.estdats1 or a.date = b.repdats1);

/* 2.if adjustment factors are not the same, adjust the estimate to be on the same basis with the actual */
    create table ibes1
	as select distinct a.*, b.est_factor, c.rep_factor, 
	case when (b.est_factor ne c.rep_factor) and missing(b.est_factor) = 0 and missing(c.rep_factor) = 0
	then (rep_factor/est_factor)*value else value end as new_value
    from    ibes1 as a, 
	    adjfactor (rename=(cfacshr = est_factor)) as b, 
	    adjfactor (rename=(cfacshr = rep_factor)) as c 
    where (a.permno = b.permno and a.estdats1 = b.date) 
	and (a.permno = c.permno and a.repdats1 = c.date);
quit;

/* Make sure the last observation per fpi and analys at a certain 
forecast period begin date is included */
proc sort data = ibes1; 
    by ticker fpbdats fpi estimator analys forcdats;
run;

data ibes1; 
    set ibes1;
    by ticker fpbdats fpi estimator analys;
if last.analys;
run;

proc sort data = ibes1; by ticker fpedats fpi estimator analys; run;

*Compute the median forecast based on estimates in the 90 days prior the annual report publication date;
proc means data=ibes1 noprint;
    by ticker fpedats fpi;
    var /*value*/ new_value; 
	/* new_value is the estimate appropriately adjusted */
    output out= medest (drop=_type_ _freq_) 
	/* to be on the same basis with the actual reported earnings */
	median=medest n=numest;
run;

/* Merge median estimates with ancillary information on permno, actuals and report dates */
/* Determine whether most analysts are reporting estimates on primary or diluted basis */
/* following the methodology outlined in Livnat and Mendenhall (2006) */
proc sql; 
    create table medest 
    as select distinct a.*, b.repdats, b.actuals, b.permno,
    case when p_count > d_count then 'P' 
	when p_count <= d_count then 'D' 
    end as basis
    from medest as a left join ibes1 as b
	on a.ticker = b.ticker 
	and a.fpedats = b.fpedats
	and a.fpi = b.fpi;
quit;

* Fixing labels;
data medest (label = 'IBES Median Estimates With Actuals');
    set medest;
    fpbdats = intnx('year',fpedats,-fpi, "sameday"); 
    fpbdats = intnx('day',fpbdats,90); 
    format fpbdats date9.;
    label basis = "(P)rimary or (D)iluted basis for estimates"
    medest = "Median estimate"
    numest = "Number of estimates"
    fpi = "Forecast period indicator"
    fpbdats = "Last possible forecast date, SAS Format"
    repdats = "Earnings announcement date, SAS Format";
run;

proc sql; 
    drop table ibes, ibes1;
quit;

/* This is just a temp part designed to save the output into a stable file while developing the code */
data temp.medest;
    set medest;
run;

options notes;
%put #### ## # Done: Dataset medest created! # ## ####;
%put ; 
%MEND;

%MACRO CIBESLINK (begdt=,enddt=);
/* Adapted version of cibeslink.sas 
I use 'lnk' rather than crsp.ccmxpf_lnkhist for 3 reasons:
    1) lnk is the collapsed verision of ccmxpf_lnkhist. It is based on 
    ccm_linktable.sas
    2) I do not have acess to crsp.ccmxpf_lnkhist
    3) According to WRDS using LU + LC + LS is comparable to using usedflag. 
    WRDS has depreciated usedflag as of 2014.
    */
%put ; %put ; %put ; %put ; %put ; 
%put #### ## # Extracting a GVKEY-IBES Ticker link file # ## #### ;
options nonotes;

%put #### ### ## # ;
proc sort data = links.lnk out = lnk;
    where linktype in ("LU", "LC", "LS" /*,"LD", "LF", "LN", "LO", "LX"*/);  
    by gvkey linkdt;
run;

*Creating gvkey-ticker link for CRSP firms, call it CIBESLNK;
proc sql; 
    create table lnk1 (drop = permno score where=(missing(ticker)=0))
    as select *
    from lnk (keep = gvkey lpermno lpermco linkdt linkenddt) as a 
	left join 
	    links.iclink (keep = ticker permno score where=(score in (0,1,2))) as b
	on a.lpermno = b.permno;
quit;

proc sort data = lnk1; 
    by gvkey ticker linkdt;
run;

data fdate ldate; 
    set lnk1;
    by gvkey ticker;
	if first.ticker then output fdate; /* fdate: first date */
	if last.ticker then output ldate;
run;

data temp;
    merge fdate (keep = gvkey ticker linkdt) 
	ldate (keep = gvkey ticker linkenddt); 
    by gvkey ticker;
run;

/*Check for duplicates*/
%put #### ### ## # ;
data dups nodups; 
set temp;
    by gvkey ticker;
    if first.gvkey = 0 or last.gvkey = 0 
	then output dups;
    if not (first.gvkey = 0 or last.gvkey = 0) 
	then output nodups;
run;

proc sort data = dups; 
    by gvkey linkdt linkenddt ticker;
run;

data dups (where = (flag ne 1)); 
    set dups;
    by gvkey;
    if first.gvkey=0 
	and (linkdt <= lag(linkenddt) or missing(lag(linkenddt)) = 1) 
	then flag=1;
run;

/*CIBESLNK contains gvkey-ibes ticker links over non-overlapping time periods*/
data links.cibeslnk; 
    set nodups dups (drop=flag);
run;

proc sql; drop table nodups, dups, fdate, ldate, lnk1; quit;

options notes;
%put #### ## # Done: Dataset links.cibeslnk created! # ## ####;
%put ; 
%MEND;

%MACRO CRSP_DATA (begdt=,enddt=);
/* This macro collects daily price and total return data from crsp, and adjusts the total return for delisting according to Dechow (2008). Price data is adjusted for splits. Data is available as file crsp_data in the working memory */ 
* Step 1) Collect price, volume, and return data per PERMCO and DATE;
%crspmerge(s=d,start=&BDATE egdt,end=&EDATE,sfvars=&DSFVARS,sevars=&DSEVARS,filters=shrcd in (10,11,12) and exchcd in (1,2,3),final_ds=crsp_dta);

* CRSP_DTA is sorted by date and permno and has historical returns
as well as historical share codes and exchange codes
Below I create an index for the various share codes classes;

%put ; %put ; %put ; %put ; %put ; 
%put #### ## # Extracting a CRSP stock file with delist adjusted total returns  # ## #### ;
options nonotes;

data crsp1;
    set crsp_dta;
    * Lines below adjusts cfacpr & cfacshr for possible errors;
    if cfacpr = 0 then cfacpr = 1;
    if missing(cfacpr) = 1 then cfacpr = 1;
    if cfacshr = 0 then cfacshr = 1;
    if missing(cfacshr) = 1 then cfacshr = 1;
run;

* Step 2) Adjust for CRSP delisting info; 
* Identify delisting returns and adjust return data;
%put #### ### ## # ;
data delist;
  set crsp.mseall (keep = permno dlret dlretx dlstcd dlpdt dlamt);
  where dlstcd > 100;
  if missing(dlpdt) = 1 and missing(date) = 0 then do;
      dlpdt = date;
  end;
  shrout2 = shrout;
  keep permno dlret dlretx dlstcd dlpdt dlamt shrout2;
run;

proc sort data = delist nodupkey; by permno dlpdt; run;

* If dlstcd = 500 or 520 <= dlstcd <= 584 and dlret is missing set
dlret to -100 percent. See Dechow (2008). Alternatively look at
http://finance.sauder.ubc.ca/~jasonchen/Growth/stepminus1formportfolios_80.sas;

data delist;
  set delist;
    if missing(dlret) = 1 then do;
      if dlstcd >= 520 and dlstcd <= 584 then do;
	dlret = -1;
	dlretx = -1;
      end;
      else if dlstcd = 500 then do;
	dlret = -1;
	dlretx = -1;
      end;
    end;
run;

proc sql;
create table crsp1 as 
  select a.*, b.dlstcd, b.dlret, b.dlpdt, b.dlamt, b.shrout2
  from crsp1 as a left join delist as b
  on  a.permno = b.permno 
  and a.date ne . 
  and intnx('month',a.date,0,'end') = intnx('month',b.dlpdt,0,'end');
quit;

* Fix missing return data where there is enough info;
data crsp1;
  set crsp1;
  * Stock still actively traded should not have missing values;
  if missing(ret) = 1 and intnx('month',date,0,'E') < intnx('month',dlpdt,0,'E')
    then ret = 0; 
  if missing(retx) = 1 and intnx('month',date,0,'E') < intnx('month',dlpdt,0,'E')
    then retx = 0;
  if missing(ret) = 1 and missing(dlpdt) = 1 and vol >= 0 then ret = 0; 
  if missing(retx) = 1 and missing(dlpdt) = 1 and vol >= 0 then retx = 0; 
  * Just copy return data to new var. Modified below for delisting;
  retadj = ret;
  retxadj = retx;
run;

* Adjust return data, prc and, if necessary shrout for delisting
month;
data crsp1;
set crsp1;
  if dlstcd > 100 then do;
	prc = dlamt; * Add delisting amount;
    if shrout <= 0 then do;
       if missing(shrout2) = 0 then shrout = shrout2;
    end;
    if missing(ret) = 1 then ret = 0;
    if missing(dlret) = 1 then dlret = 0;
    retadj = sum(1,ret) * sum(1,dlret) - 1;
    retxadj = sum(1,retx) * sum(1,dlretx) - 1;
    label retadj = "Total return adjusted for delisting"
	retxadj = "Return without dividends adjusted for delisting"
	dlretx = "Delisting return without dividends";
  end;
  drop shrout2 dlamt;
run;
  
* Adjust missing prc if both lead and lag value exists, and vol also
exists;
proc sort data = crsp1; by permno date; run;

proc expand data = crsp1 out = crsp1 method = none; 
by permno;
    id date; 
    convert prc = lagprc / transformout = (nomiss lag 1); 
    convert prc = leadprc / transformout = (nomiss lead 1); 
run; 

* Fix the missing prc as average over adjacent dates;
data crsp1;
set crsp1;
/*  if missing(prc) = 1 and missing(lagprc) = 0 
	and missing(vol) = 0 then prc = lagprc;*/
    if missing(prc) = 1 and missing(leadprc) = 0 
	and missing(lagprc) = 0 and missing(vol) = 0 
	then prc = (lagprc + leadprc) / 2;
    drop lagprc leadprc;
run;

* Step 3) Get market cap for each permno-date and finish;
%put #### ### ## # ;
proc sql;
create table crsp1
  as select *,
    abs(prc) * shrout as mcap  'Market Value of Equity, in thousands',
    abs(prc) / cfacpr as prcadj  'Split adjusted share price',
    abs(prc) as prc  '(Raw) share price'
    /*abs(prc) * shrout * cfacshr / cfacpr 
	as meadj  'Market Value of Equity, adjusted, in thousands'*/
    from crsp1;
quit;

* Adjust values of select variables so values appear for each
observation;
proc expand data = crsp1 out = crsp1 method = none; 
by permno;
    id date; 
    convert shrout = lshrout   / transformout = (nomiss lag 1); 
    convert exchcd = lexchcd   / transformout = (nomiss lag 1); 
    convert permno = lpermno   / transformout = (nomiss lag 1); 
run; 

data crsp_data; 
    set crsp1;
    if missing(date) = 0 then do;
	if permno = lpermno and missing(shrout) = 1 then shrout = lshrout;
	if permno = lpermno and missing(exchcd) = 1 then exchcd = lexchcd;
    end;
    drop lshrout lexchcd lpermno;
run;

/* This is just a temp part designed to save the output into a stable file while developing the code */
data temp.crsp_data;
    set crsp_data;
run;

options notes;
%put #### ## # Done: Dataset crsp_data created! # ## ####;
%put ; 
%MEND;

%MACRO COMPUSTAT_DATA (begdt=,enddt=);
%put ; %put ; %put ; %put ; %put ; 
%put #### ## # Extracting a Compustat data  # ## #### ;
options nonotes;


/* This is just a temp part designed to save the output into a stable file while developing the code */
data temp.comp1;
    set comp1;
run;

options notes;
%put #### ## # Done: Dataset comp1 created! # ## ####;
%put ; 
%MEND;

*CIBESLINK macro will create a linking table CIBESLNK between IBES ticker and
Compustat gvkey based on IBES ticker-CRSP permno (ICLINK) and CCM link;
%CIBESLINK (begdt=&begindate, enddt=&enddate); 

* CRSP_DATA collects daily total returns adjusted for delisting (retadj) and market cap (meq), and some additional, data, in a file called crsp_data. Delisting returns are added following Dechow (2008);
%CRSP_DATA (begdt=&begindate, enddt=&enddate); 

proc export data = crsp_data 
  outfile = '/sastemp2/joacland/crsp.dta'
  dbms = stata
  replace;
run;

********************************************************************;
*** Extract Compustat company data *********************************;
********************************************************************;

*** Extract Compustat annual company data, add link info ***********;
data funda;
set &FUNDA (keep = gvkey datadate fyear fyr &VARBASE &VARS);
    where year(datadate) between &bfyear and &efyear and
    datafmt = "STD" and
    consol = "C" and
    popsrc = "D" 
    /*and
    indfmt = "INDL"*/
    ;
    * Create calendar date of fiscal period end in Compustat extract;
    format endfyr begfyr pubdats date9.;
    if (1 <= fyr <= 5) then endfyr = intnx('month',mdy(fyr,1,fyear + 1),0,'end'); 
    else if (6 <= fyr <= 12) then endfyr = intnx('month',mdy(fyr,1,fyear),0,'end'); 
    begfyr = intnx('month',endfyr,-11,'beg'); 

    * Create date when info is assumed public.
    The info is assumed public 90 days after the end of fiscal year;
    pubdats = intnx('day',endfyr,90);

    label begfyr = "Calendar date for beginning of fiscal year"
    endfyr = "Calendar date for end of fiscal year"
    pubdats = "Publication date assumed 90 days after endfyr";

    cyear = year(datadate);

    * Create matching year;
    myear = year(datadate);

    * Amend the matching year to allow the info to be assumed public.
    (Assumption is that it is public three months after endfyr).
    Use this to collect end-of-June market values and match it to
    relevant accounting data. This ensures there is at least a
    three month lag between the end of the fyear and end-of-June
    market values;
    if fyr >=4 then do; 
	myear = myear + 1; 
    end; 
run;

proc sort; by gvkey fyear; run;

/* data temp.funda; set funda; run; */

* a) Link gvkey with CRSP permno (and permco);
proc sql;
create table comp1 as
    select a.*, b.lpermno as permno, b.lpermco as permco, 
	b.linktype, b.linkprim, b.linkdt, b.linkenddt
    from funda as a, links.lnk as b
	where a.gvkey = b.gvkey
	and (a.endfyr >= b.linkdt or missing(b.linkdt) = 1)
	and (a.endfyr <= b.linkenddt or missing(b.linkenddt) = 1)
	and b.linktype in ('LU','LC', 'LS'); 
quit;

proc sort data = comp1; by permno datadate linktype; run;
 
* If both LC and LU links exist, choose the former;
data comp1;
set comp1;
    by permno datadate linktype;
    if first.linktype = 1;
run;

* Add ranking based on LINKTYPE;
data comp1;
    set comp1;
    if linktype = 'LC' then do TYPERank = 1; end;
    else do;
	if linktype = 'LU' then TYPERank = 2;
	else if linktype = 'LS' then TYPERank = 3;
	else TYPERank = 999; 
    end;
run;

* Add ranking based on LINKPRIM.; 
data comp1;
    set comp1;
    if linkprim = 'P' then do ISSUERank = 1; end;
    else do;
	if linkprim = 'C' then ISSUERank = 2;
	else if linkprim = 'N' then ISSUERank = 3;
	else if linkprim = 'J' then ISSUERank = 4;
	else ISSUERank = 999;
    end;
run;

* Some companies change fiscal year end in the middle of the calendar year. In these cases, there are more than one annual record for accounting data. I select the last annual record in a given calendar year;
proc sort data = comp1; by permno cyear datadate; run;
 
data comp1;
set comp1;
    by permno cyear datadate;
    if last.cyear = 1;
run;

* Select the most approriate stock based on linktype and linkprim;
proc sort data = comp1; 
    by datadate gvkey 
    descending TYPERank
    descending ISSUERank;
run;

data comp1;
set comp1 (drop=TYPERank ISSUERank);
    by datadate gvkey;
    if first.gvkey then output;
run;

* Sanity check: No duplicates;
proc sort data = comp1 nodupkey ; by gvkey datadate permno; run;

* b) Link gvkey with IBES ticker;
proc sql;
create table comp1 
    as select a.*, b.ticker 
    from comp1 as a left join links.cibeslnk as b 
    on a.gvkey = b.gvkey 
    and ((b.linkdt <= a.endfyr <= b.linkenddt) or 
	(b.linkdt <= a.endfyr and missing(b.linkenddt) = 1) or 
	(missing(b.linkdt) = 1 and a.endfyr <= b.linkenddt)); 

* Get IBES Tickers List;
create table tickers as
    select gvkey, ticker
    from comp1;

* Get publication date list;
create table pubdats as
    select gvkey, ticker, pubdats
    from comp1;
quit;

/* Macro IBES_DATA extracts the estimates from IBES Unadjusted file based on */
/* the user-provided input (SAS set tickers), links them to IBES actuals, puts */
/* estimates and actuals on the same basis by adjusting for stock splits using */
/* CRSP adjustment factor and calculates the median of analyst forecasts made */
/* in the 90 days prior to the earnings announcement date.  Outputs file MEDEST */
/* into work directory*/

%IBES_DATA (infile=tickers, ibes1_where=&IBES_WHERE1, ibes2_where=&IBES_WHERE2,  ibes_var=&IBES_VARS); 

/*outfile = '/sastemp2/joacland/medest.dta';*/
proc export data = medest
    outfile = '~/medest.dta'
    dbms = STATA
    replace;
run;

/* Link IBES analysts' expectations (MEDEST), IBES report dates (repdats) and */
/* actuals (act) with Compustat data. MEDEST table is a result of the IBES_DATA */
/* macro. 'medest' is on per ticker-fpedats-fpi basis. Should I change the */
/* join-order?*/

proc sql;
create table comp1 (drop=cyear myear)
    as select a.*, b.medest, b.numest, b.repdats, b.fpbdats, b.actuals, b.basis, b.fpi
    from comp1 as a left join medest as b 
    on a.ticker = b.ticker 
    and a.endfyr -5 <= b.fpedats <= a.endfyr +5; 
	/* not: put(a.endfyr,yymmn6.) = put(b.fpedats,yymmn6.)*/
quit;
proc sql;
* Adjust report date to be CRSP trading day;
create table comp1
    as select *,
    case when weekday(repdats) = 7 then intnx('day',repdats,2)
	when weekday(repdats) = 1 then intnx('day',repdats,1)
	else repdats
    /* Moves repdats to next trading day if on weekend */
    end as repdats1
    from comp1;
quit;
* Identify day from when to extract prc for sue;
proc sql;
create table comp1
    as select *,
    case when weekday(repdats1) = 2 then intnx('day',repdats1,-4)
    /* If repdats on Mon get prc from prev Thu */
	when weekday(repdats1) = 3 then intnx('day',repdats1,-4)
    /* If repdats on Tue get prc from prev Fri */
	else intnx('day',repdats1,-2)
    /* Or else from two days before repdats */
    end as prcdats1
    from comp1;
quit;
*Extract prc and me for the sue. They should be from a day before the event window [see Easton & Zmijevski (1989)];
proc sql;
create table comp1 
    as select a.*, b.prcadj, b.mcap
    from comp1 as a,
	crsp_data as b
    where a.permno = b.permno
	and a.prcdats1 = b.date;
quit;

* Remove fully duplicate records and pre-sort;
proc sort data=comp1 noduprec; by _all_; run; 
proc sort data=comp1; by gvkey fyear; run;

data comp1 (drop = prcdats1);
    set comp1;
    sue1 = (actuals - medest) / prcadj;
    format sue1 percent7.4;
    format repdats1 date9.;
    label sue1 = "Standardized unexpected earnings, IBES";
run;

proc export data = comp1
    outfile = '~/comp1.dta'
    dbms = STATA
    replace;
run;

