013119000
013119001
080100802
080100801

013116000
080116000

013119898

015950000

create table man_bancos_00 
(
banco varchar(20),
telefono varchar(10)
) distribute on (telefono);

truncate table man_bancos_00 ;

insert into man_bancos_00 values('interbank', '13119000');
insert into man_bancos_00 values('interbank', '13119001');
insert into man_bancos_00 values('interbank', '80100802');
insert into man_bancos_00 values('interbank', '80100801');
insert into man_bancos_00 values('scotiabank', '13116000');
insert into man_bancos_00 values('scotiabank', '80116000');
insert into man_bancos_00 values('bcp', '13119898');
insert into man_bancos_00 values('bbva', '15950000');

SELECT * FROM man_bancos_00

create temp table man_trafico_bancos_00 as
(
SELECT 
a.fechainicio,
a.NUMORIGEN, 
a.NUMDESTINO, 
b.banco,
sum(a.DURACIONSEND) DURACIONSEND, 
COUNT(1) llamadas
FROM PE_PROD_DWH_DATA.DWC_CARGA.NETWORK_ACTIVITY_UNRA_SUBS a
INNER JOIN man_bancos_00 b on a.NUMDESTINO=b.telefono
WHERE 
a.CODTIPOSERVICIO='VOZ' and a.CODSENTIDOTRAFICO='S' 
and a.fechainicio=to_date('20180801','yyyymmdd')
and 1=2
GROUP BY 1,2,3,4
) distribute on (fechainicio,NUMORIGEN,NUMDESTINO);

DROP TABLE MAN_fecha_bancos_00 IF EXISTS; 
CREATE TABLE MAN_fecha_bancos_00
(FECHA DATE,
 TIEMPO TIME); 
 
CREATE OR REPLACE PROCEDURE MAN_extrae_TRAFICO_bancos(DATE, DATE)
RETURNS INTEGER
EXECUTE AS CALLER
LANGUAGE NZPLSQL AS
BEGIN_PROC
	DECLARE
	START_DATE ALIAS FOR $1;
	END_DATE ALIAS FOR $2;
	v_DATE DATE;
BEGIN
v_DATE=START_DATE;
LOOP
IF v_DATE<END_DATE
THEN

INSERT INTO man_trafico_bancos_00
SELECT 
a.fechainicio,
a.NUMORIGEN, 
a.NUMDESTINO, 
b.banco,
sum(a.DURACIONSEND) DURACIONSEND, 
COUNT(1) llamadas
FROM PE_PROD_DWH_DATA.DWC_CARGA.NETWORK_ACTIVITY_UNRA_SUBS a
INNER JOIN man_bancos_00 b on a.NUMDESTINO=b.telefono
WHERE 
a.CODTIPOSERVICIO='VOZ' and a.CODSENTIDOTRAFICO='S' 
and a.fechainicio=v_DATE
GROUP BY 1,2,3,4;

INSERT INTO MAN_fecha_bancos_00
SELECT v_DATE, current_time; 

commit;

v_DATE=v_DATE+1;
ELSE END IF;
EXIT WHEN v_DATE=END_DATE;
END LOOP;
END;
END_PROC; 

EXECUTE MAN_extrae_TRAFICO_bancos(TO_DATE('20180801','YYYYMMDD'), TO_DATE('20180901','YYYYMMDD'));

SELECT FECHAINICIO, COUNT(1) FROM man_trafico_bancos_00 GROUP BY 1 ORDER BY 1 desc;

SELECT * FROM man_trafico_bancos_00

SELECT 
SUBSTR(NUMEROACCESO,3) NUMEROACCESO,  

FROM man_detalle_00



DROP TABLE man_detalle_00 IF EXISTS ;
CREATE TABLE man_detalle_00 
( 
numeroacceso VARCHAR(15),
paginanavegacion VARCHAR(100),
cnt intEGER,
fechainiciosesion DATE 
)
DISTRIBUTE ON (fechainiciosesion,numeroacceso);

SELECT * FROM man_detalle_00

SELECT PAGINANAVEGACION, SUM(CNT) VISITAS FROM man_detalle_00
WHERE PAGINANAVEGACION LIKE '%interbank%'
GROUP BY 1 ORDER BY 2 DESC;

SELECT PAGINANAVEGACION, SUM(CNT) VISITAS FROM man_detalle_00
WHERE PAGINANAVEGACION LIKE '%scotiabank%'
GROUP BY 1 ORDER BY 2 DESC;

SELECT PAGINANAVEGACION, SUM(CNT) VISITAS FROM man_detalle_00
WHERE PAGINANAVEGACION LIKE '%viabcp%'
GROUP BY 1 ORDER BY 2 DESC;

SELECT PAGINANAVEGACION, SUM(CNT) VISITAS FROM man_detalle_00
WHERE PAGINANAVEGACION LIKE '%bbvacontinental%'
GROUP BY 1 ORDER BY 2 DESC;

SELECT substr(numeroacceso,3) FROM man_detalle_00

DROP TABLE man_detalle_01 if exists;
create table man_detalle_01 as
(
SELECT 
substr(numeroacceso,3) numeroacceso,
COUNT(DISTINCT fechainiciosesion) dias,
max(case when PAGINANAVEGACION in (
'bancaporinternet.interbank.com.pe-st.com',
'www.interbank.pe',
'bancaporinternett.interbank.com.pe-ibk.com',
'bancaporinternet.interbank.pe-ikb.com',
'interbank.pe',
'www.interbank.com.pe',
'interbankbenefit.pe'
) then 1 else 0 end) fl_interbank, 
max(case when PAGINANAVEGACION in (
'www.scotiabank.com.pe',
'bancainternetempresas.scotiabank.com.pe',
'scotiaenlinea.scotiabank.com.pe'
) then 1 else 0 end) fl_scotiabank,
max(case when PAGINANAVEGACION in (
'ww3.viabcp.com',
'www.viabcp.com',
'www.viabcp.com.pe'
) then 1 else 0 end) fl_bcp,
max(case when PAGINANAVEGACION in (
'bancamovil.bbvacontinental.pe',
'www.ubicanosbbvacontinental.pe',
'www.bbvacontinental.pe',
'bbvacontinental.pe',
'bancaporinternet.bbvacontinental.pe'
) then 1 else 0 end) fl_bbva,
--
COUNT(DISTINCT (case when PAGINANAVEGACION in (
'bancaporinternet.interbank.com.pe-st.com',
'www.interbank.pe',
'bancaporinternett.interbank.com.pe-ibk.com',
'bancaporinternet.interbank.pe-ikb.com',
'interbank.pe',
'www.interbank.com.pe',
'interbankbenefit.pe'
) then fechainiciosesion end)) dias_interbank, 
COUNT(DISTINCT (case when PAGINANAVEGACION in (
'www.scotiabank.com.pe',
'bancainternetempresas.scotiabank.com.pe',
'scotiaenlinea.scotiabank.com.pe'
) then fechainiciosesion end)) dias_scotiabank,
COUNT(DISTINCT (case when PAGINANAVEGACION in (
'ww3.viabcp.com',
'www.viabcp.com',
'www.viabcp.com.pe'
) then fechainiciosesion end)) dias_bcp,
COUNT(DISTINCT (case when PAGINANAVEGACION in (
'bancamovil.bbvacontinental.pe',
'www.ubicanosbbvacontinental.pe',
'www.bbvacontinental.pe',
'bbvacontinental.pe',
'bancaporinternet.bbvacontinental.pe'
) then fechainiciosesion end)) dias_bbva

FROM 
man_detalle_00
WHERE PAGINANAVEGACION in 
(
'bancaporinternet.interbank.com.pe-st.com',
'www.interbank.pe',
'bancaporinternett.interbank.com.pe-ibk.com',
'bancaporinternet.interbank.pe-ikb.com',
'interbank.pe',
'www.interbank.com.pe',
'interbankbenefit.pe',
'www.scotiabank.com.pe',
'bancainternetempresas.scotiabank.com.pe',
'scotiaenlinea.scotiabank.com.pe',
'ww3.viabcp.com',
'www.viabcp.com',
'www.viabcp.com.pe',
'bancamovil.bbvacontinental.pe',
'www.ubicanosbbvacontinental.pe',
'www.bbvacontinental.pe',
'bbvacontinental.pe',
'bancaporinternet.bbvacontinental.pe'
)
GROUP BY 1
) distribute on (numeroacceso);

SELECT * FROM man_detalle_01

create table man_detalle_02 as
(
SELECT 
NUMEROACCESO, 
DIAS, 
FL_INTERBANK+FL_SCOTIABANK+FL_BCP+FL_BBVA as nro_bancos,
FL_INTERBANK, FL_SCOTIABANK, FL_BCP, FL_BBVA, DIAS_INTERBANK, DIAS_SCOTIABANK, DIAS_BCP, DIAS_BBVA  
FROM man_detalle_01
);

SELECT * FROM man_detalle_02

create table man_trafico_bancos_01 as
(
SELECT
NUMORIGEN NUMEROACCESO, 
COUNT(DISTINCT FECHAINICIO ) dias,
max(case when BANCO='interbank' then 1 else 0 end) FL_INTERBANK,
max(case when BANCO='scotiabank' then 1 else 0 end) FL_SCOTIABANK,
max(case when BANCO='bcp' then 1 else 0 end) FL_BCP,
max(case when BANCO='bbva' then 1 else 0 end) FL_BBVA,
COUNT(DISTINCT case when BANCO='interbank' then FECHAINICIO end) DIAS_INTERBANK,
COUNT(DISTINCT case when BANCO='scotiabank' then FECHAINICIO end) DIAS_SCOTIABANK,
COUNT(DISTINCT case when BANCO='bcp' then FECHAINICIO end) DIAS_BCP,
COUNT(DISTINCT case when BANCO='bbva' then FECHAINICIO end) DIAS_BBVA
FROM 
man_trafico_bancos_00
GROUP BY 1 
);

create table man_trafico_bancos_02 as
(
SELECT 
NUMEROACCESO, 
DIAS, 
FL_INTERBANK+FL_SCOTIABANK+FL_BCP+FL_BBVA as nro_bancos,
FL_INTERBANK, FL_SCOTIABANK, FL_BCP, FL_BBVA, DIAS_INTERBANK, DIAS_SCOTIABANK, DIAS_BCP, DIAS_BBVA  
FROM man_trafico_bancos_01
);

SELECT COUNT(1) FROM man_detalle_02 -- 100,435

SELECT * FROM man_detalle_02

SELECT * FROM man_detalle_02

SELECT FL_bbva, COUNT(1) FROM man_detalle_02
WHERE NRO_BANCOS=1
GROUP BY 1


--25,053

SELECT COUNT(1) FROM man_trafico_bancos_02 -- 337,959

-- Bancarizados, algún producto financiero 35.5%
-- 43% con tarjeta de Credito
-- 89% Debito
-- 
-- Canal por el que Recarga.
-- Quien originó la recarga.
-- 




SELECT PRODUCTO, COUNT(1) FROM man_bancos_consolidado_00
WHERE NRO_BANCOS_W>0
GROUP BY 1 

SELECT * FROM man_planta_convergente
WHERE fechaplantaoficial=to_date('20180801','yyyymmdd')


create temp table man_temp_NUMEROACCESO AS
(
SELECT NUMEROACCESO FROM 
(SELECT NUMEROACCESO FROM man_detalle_02
union
SELECT NUMEROACCESO FROM man_trafico_bancos_02) a
); 

SELECT DIAS_INTERBANK, COUNT(1) FROM man_detalle_02
WHERE FL_INTERBANK=1 
GROUP BY 1 ORDER BY 2 desc

SELECT * FROM man_trafico_bancos_02

SELECT NRO_BANCOS, COUNT(1) FROM man_detalle_02 GROUP BY 1 ORDER BY 2 desc

SELECT COUNT(1) FROM man_temp_NUMEROACCESO -- 413,341

DROP TABLE man_bancos_consolidado_00 if exists;
create table man_bancos_consolidado_00 as
(
SELECT 
a.NUMEROACCESO,
--
nvl(b.DIAS,0) DIAS_w, 
nvl(b.NRO_BANCOS,0) NRO_BANCOS_w, 
nvl(b.FL_INTERBANK,0) FL_INTERBANK_w, 
nvl(b.FL_SCOTIABANK,0) FL_SCOTIABANK_w, 
nvl(b.FL_BCP,0) FL_BCP_w, 
nvl(b.FL_BBVA,0) FL_BBVA_w, 
nvl(b.DIAS_INTERBANK,0) DIAS_INTERBANK_w, 
nvl(b.DIAS_SCOTIABANK,0) DIAS_SCOTIABANK_w, 
nvl(b.DIAS_BCP,0) DIAS_BCP_w, 
nvl(b.DIAS_BBVA,0) DIAS_BBVA_w,  
--
nvl(c.DIAS,0) DIAS_t, 
nvl(c.NRO_BANCOS,0) NRO_BANCOS_t, 
nvl(c.FL_INTERBANK,0) FL_INTERBANK_t, 
nvl(c.FL_SCOTIABANK,0) FL_SCOTIABANK_t, 
nvl(c.FL_BCP,0) FL_BCP_t, 
nvl(c.FL_BBVA,0) FL_BBVA_t, 
nvl(c.DIAS_INTERBANK,0) DIAS_INTERBANK_t, 
nvl(c.DIAS_SCOTIABANK,0) DIAS_SCOTIABANK_t, 
nvl(c.DIAS_BCP,0) DIAS_BCP_t, 
nvl(c.DIAS_BBVA,0) DIAS_BBVA_t, 
--
case when nvl(b.NRO_BANCOS,0)+nvl(c.NRO_BANCOS,0)>0 then 1 else 0 end NRO_BANCOS_gen, 
case when nvl(b.FL_INTERBANK,0)+nvl(c.FL_INTERBANK,0)>0 then 1 else 0 end FL_INTERBANK_gen, 
case when nvl(b.FL_SCOTIABANK,0)+nvl(c.FL_SCOTIABANK,0)>0 then 1 else 0 end FL_SCOTIABANK_gen, 
case when nvl(b.FL_BCP,0)+nvl(c.FL_BCP,0)>0 then 1 else 0 end FL_BCP_gen, 
case when nvl(b.FL_BBVA,0)+nvl(c.FL_BBVA,0)>0 then 1 else 0 end FL_BBVA_gen,
--
case when nvl(b.NRO_BANCOS,0)>0 and nvl(c.NRO_BANCOS,0)>0 then 2 else 1 end fl_fuente,
case when LINEANEGOCIOCD='CP' then 'pre' WHEn LINEANEGOCIOCD in ('CF','CC') then 'pos' end producto
FROM 
man_temp_NUMEROACCESO a
LEFT JOIN man_detalle_02 b on a.NUMEROACCESO=b.NUMEROACCESO
LEFT JOIN man_trafico_bancos_02 c on a.NUMEROACCESO=c.NUMEROACCESO
LEFT JOIN man_planta_convergente d on a.NUMEROACCESO=d.NUMEROTELEFONO and d.fechaplantaoficial=to_date('20180801','yyyymmdd')
) ;

SELECT * FROM man_planta_convergente
WHERE fechaplantaoficial=to_date('20180801','yyyymmdd')

SELECT * FROM man_detalle_02
SELECT * FROM man_detalle_02

