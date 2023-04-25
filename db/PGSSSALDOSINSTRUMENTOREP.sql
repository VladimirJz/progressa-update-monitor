
delimiter ;
drop procedure if exists PGSSALDOSINSTRUMENTOREP;
delimiter $$
CREATE PROCEDURE `PGSSALDOSINSTRUMENTOREP`(
Par_Instrumento CHAR,
Par_InstrumentoID BIGINT,
Par_Consolidado CHAR,

Par_EmpresaID		INT(11), 		
Aud_Usuario			INT(11),		
Aud_FechaActual		DATETIME,		
Aud_DireccionIP		VARCHAR(15),	
Aud_ProgramaID		VARCHAR(50),	
Aud_Sucursal		INT(11),		
Aud_NumTransaccion	BIGINT(20)	
    
)
TerminaStore: BEGIN
	DECLARE Var_Row INT;
    DECLARE Reporte_Global CHAR;
    DECLARE Reporte_Parcial CHAR;
    DECLARE Entero_Cero INT;
    DECLARE Valor_No CHAR;

    DECLARE Instrumento_Cliente CHAR;
    DECLARE Instrumento_Credito CHAR;
    DECLARE Instrumento_Transaccion CHAR;

	DECLARE Var_TransaccionInicio INT;
	DECLARE Var_TransaccionFin INT;
	

	
	DECLARE SQL_QUERY VARCHAR(16000);
	DECLARE CREATE_TABLE VARCHAR(500);
	DECLARE SELECT_FIELDS  VARCHAR(500);
	DECLARE CASE_FIELDS VARCHAR(16000);
	DECLARE FROM_TABLES VARCHAR(100);
	DECLARE Var_FechaSistema DATE;
	DECLARE FACTOR_IVA decimal(10,2);
	DECLARE Var_CierreDia CHAR;

    
	SET Instrumento_Cliente='C';
    SET Instrumento_Credito ='R';
    SET Instrumento_Transaccion='T';
    SET FACTOR_IVA=1.16;

    SET Entero_Cero=0;
    SET Valor_No='N';
    SET Reporte_Global='G';


	SET Var_CierreDia=(select ValorParametro from PARAMGENERALES where LlaveParametro='EjecucionCierreDia');
	SET Par_Consolidado=COALESCE(Par_Consolidado,Valor_No);
	SET Var_FechaSistema=(SELECT FechaSistema FROM PARAMETROSSIS);
	 
	
	IF (Var_CierreDia=Valor_No)THEN
		

		DROP TABLE IF EXISTS lista_creditos;
		CREATE TEMPORARY TABLE lista_creditos
		(
			CreditoID BIGINT NOT NULL,
			PRIMARY KEY (CreditoID)
		);
		
		IF(Par_Tipo=Reporte_Global)THEN
			BEGIN
				INSERT INTO lista_creditos(CreditoID)
					SELECT CreditoID 
					FROM CREDITOS 
					WHERE Estatus NOT IN ('P','K','C');
				
			END;
		ELSE 
			BEGIN
				CASE Par_Instrumento
				WHEN Instrumento_Cliente THEN 
					BEGIN 
						INSERT INTO lista_creditos
							SELECT CreditoID 
							FROM CREDITOS 
							WHERE Estatus NOT IN ('P','K','C') AND ClienteID=Par_InstrumentoID;
					END;
				WHEN Instrumento_Credito THEN 
					BEGIN 
						INSERT INTO lista_creditos
							SELECT CreditoID 
							FROM CREDITOS 
							WHERE Estatus NOT IN ('P','K','C') AND CreditoID=Par_InstrumentoID;
					END;
			ELSE    
					BEGIN
						IF (Par_InstrumentoID = Entero_Cero)THEN
							SET Var_TransaccionInicio =(SELECT COALESCE(Valor,Entero_Cero) FROM microfin.PGSSERVICIOKEY WHERE ServicioID=1  AND KeyID=1);
							SET Var_TransaccionFin =(SELECT CreditosMovsID FROM CREDITOSMOVS WHERE FechaOperacion=Var_FechaSistema ORDER BY CreditosMovsID DESC LIMIT 1 );
							
						END IF;
						
						INSERT INTO lista_creditos
						#select Var_TransaccionFin,Var_TransaccionFin,Var_TransaccionInicio;
							SELECT DISTINCT CreditoID 
							FROM CREDITOSMOVS 
							WHERE FechaOperacion=Var_FechaSistema AND CreditosMovsID>Var_TransaccionInicio AND CreditosMovsID<=Var_TransaccionFin;
					END;
				END CASE;
			END;
		END IF;
		
		
		DROP TABLE IF EXISTS generales_cliente;
		CREATE TEMPORARY TABLE generales_cliente
		(
			ClienteID 		INT NOT NULL,
			IDELEMENTO 		VARCHAR(30),
			IDELEMENTOPSSA 	VARCHAR(30),
            CUENTA			int, -- Cuenta Bodesa
			RFC 			VARCHAR(20),
			CURP 			VARCHAR(20),
			PRIMARY KEY(ClienteID) 
		);

		DROP TABLE IF EXISTS generales_credito;
		CREATE  TEMPORARY TABLE generales_credito
		(	
			CreditoID 		BIGINT NOT NULL,
			ClienteID 		INT,
			IDPDTO	 		INT,
			ORIPDTO 		VARCHAR(50),
			PLAZOMAX 		INT,
			IDPROGSSA 		VARCHAR(20),
			CTAPROGSSA 		VARCHAR(50),
			SERIEPGSSA 		VARCHAR(50),
			TDASUCPGSA 		INT,
			TPOCTAPGSA 		INT default 0,
			LIMITE 			DECIMAL(12,2),
			MONTODIS 		DECIMAL(12,2),
			SDOCTA			DECIMAL(12,2),
			VDO				DECIMAL(12,2),
			IFIN			DECIMAL(12,2),
			IMOR			DECIMAL(12,2),
			FCOMPRA			DATE,
			PLAZO			VARCHAR(300),
			TASA			DECIMAL(12,2),
			ESTATUS			INT,
			NEGOCIOPGSSA    INT,
			NPARCIALIDAD	INT,
			FULTPACAP		DATE,
			FULTPAINT		DATE,
			LINEACREDITO	BIGINT,
			PRIMARY KEY(CreditoID)
			

				
		);
		
		drop TABLE IF EXISTS ultimo_pago;
		CREATE TEMPORARY TABLE ultimo_pago
		(
			CreditoID BIGINT NOT NULL,
			fecha_interes date,
			fecha_capital date,
			PRIMARY KEY(CreditoID)
		);
		create index idx_Creditoid on ultimo_pago(CreditoID);

		DROP TABLE IF EXISTS bandas_vencido;
		CREATE TEMPORARY TABLE bandas_vencido
		(
			banda_id 		INT,
			limite_inferior INT,
			limite_superior INT,
			cabecera 		VARCHAR(50)
		);
		
		DROP TABLE IF EXISTS saldos_amortizacion;
		CREATE TEMPORARY TABLE  saldos_amortizacion
		( 
			CreditoID 		BIGINT NOT NULL, 
			AmortizacionID 	INT NOT NULL, 
			FechaExigible 	DATE,
			Estatus 		CHAR(1),
			SaldoCapital 	DECIMAL(12,2),
			DiasAtraso 		INT,
			PRIMARY KEY(CreditoID,AmortizacionID)
		);
		DROP TABLE IF EXISTS saldos_credito;
		CREATE TEMPORARY TABLE  saldos_credito
		( 
			CreditoID 		BIGINT NOT NULL, 
			banda_id 		INT NOT NULL, 
			cuotas  		INT,
			SaldoCapital 	DECIMAL,
			etiqueta 		VARCHAR(50)
		);
		create index idx_CreditoID on saldos_credito(CreditoID);
		create index idx_banda_id on saldos_credito(banda_id);



		DROP TABLE IF EXISTS saldo_vencido_banda;
		CREATE TEMPORARY TABLE  saldo_vencido_banda
		(
			CreditoID 		BIGINT,
			SaldoCapital 	DECIMAL(11,2),
			banda_id 		INT,
			etiqueta 		VARCHAR(50)
		);


		DROP TABLE IF EXISTS credito_incumplimiento;
		CREATE TEMPORARY TABLE credito_incumplimiento
		(
			CreditoID 			BIGINT NOT NULL, 
			FechaIncumplimiento DATE,
			FechaVencidoActual 	DATE,
			DIASVDO				INT,
			PAGOSVENCIDOS		INT,
			NPARCIALIDAD		INT,
			PRIMARY KEY(CreditoID)
		);
		DROP TABLE IF EXISTS  credito_cuotas_activas;
		CREATE TEMPORARY TABLE credito_cuotas_activas
		(
			CreditoID BIGINT NOT NULL, 
			CuotaActual INT,
			CuotasAtraso INT,
			PRIMARY KEY(CreditoID)
		);


			
		drop table if exists tmp_fecha_atraso_actual;
		create temporary table tmp_fecha_atraso_actual
		(
		CreditoID BIGINT NOT NULL,
		FechaAtraso DATE,
		PRIMARY KEY(CreditoID)
		);


		drop table if exists banda_pago_mensual;
		create TEMPORARY table banda_pago_mensual
		(
		banda_id INT NOT NULL,
		fecha_inicio DATE,
		fecha_termina DATE,
		cabecera VARCHAR(50),
		PRIMARY KEY(banda_id)
		);


		DROP TABLE IF EXISTS  pago_mes_banda;
		CREATE TEMPORARY TABLE pago_mes_banda
		(
		banda_id INT,
		CreditoID BIGINT,
		cabecera VARCHAR(50),
		Capital DECIMAL(12,2),
		Interes DECIMAL(12,2),
		Accesorios DECIMAL(12,2)
		);

		
		INSERT  into bandas_vencido 
			VALUES 	(1,1,30,'VDO30'),
					(2,31,60,'VDO60'),
					(3,61,90,'VDO90'),
					(4,91,99999999,'VDOM90');


		INSERT INTO generales_credito
			(CreditoID, 	ClienteID, 	IDPDTO, 	
			PLAZOMAX, 		IDPROGSSA,	TDASUCPGSA, 	
			LIMITE, 		MONTODIS, 	SDOCTA, 	
			VDO,			IFIN, 		IMOR, 		
			FCOMPRA, 		PLAZO, 		TASA,
			ESTATUS,        CTAPROGSSA, NPARCIALIDAD,
			LINEACREDITO,	ORIPDTO)
			
			SELECT  
			c.CreditoID,										c.ClienteID, 														c.ProductoCreditoID , 							
			DATEDIFF(c.FechaVencimien,c.FechaInicio ), 			cast(c.ClienteID as CHAR),				c.SucursalID,		
			COALESCE( l.Autorizado ,c.MontoCredito ), 			COALESCE( l.SaldoDisponible  ,0  ), 								(c.SaldoCapVigent + c.SaldoCapAtrasad +c.SaldoCapVencido +c. SaldCapVenNoExi ),
			(c.SaldoCapVencido + c.SaldoCapAtrasad ) ,			(SaldoInterProvi+SaldoInterAtras+SaldoInterVenc+SaldoIntNoConta),	(SaldoMoratorios+SaldoMoraVencido+SaldoMoraCarVen), 
			c.FechaInicio,  									upper(DescInfinitivo),												TasaFija,
			(CASE WHEN c.Estatus='P' THEN 2  
			WHEN c.Estatus='C' THEN 8  ELSE 1 END) ,  			cast(c.CreditoID as CHAR), 											datediff(c.FechaVencimien,c.FechaInicio) ,
			l.LineaCreditoID,									'PROGRESSA'
			FROM  EQU_CLIENTES  ec 
			RIGHT JOIN  (PRODUCTOSCREDITO p  
			INNER JOIN ( CATFRECUENCIAS cf 
			INNER JOIN(((CREDITOS c 
			LEFT JOIN  (migracionProgressa.EQU_CREDITOSPROGRESSA ecp 
			INNER JOIN migracionProgressa.EQU_CREDITOS ecr ON ecp.CreditoIDCte=ecr.CreditoIDCte  )  ON c.CreditoID=ecr.CreditoIDSAFI)
			INNER JOIN lista_creditos lc ON c.CreditoID=lc.CreditoID)
			LEFT JOIN LINEASCREDITO l  ON c.LineaCreditoID=l.LineaCreditoID ) ON cf.FrecuenciaID=c.FrecuenciaCap) ON  c.ProductoCreditoID=p.ProducCreditoID) ON ec.ClienteIDSAFI =c.ClienteID;


		
		INSERT INTO generales_cliente( ClienteID,	RFC,	CURP, IDELEMENTO, IDELEMENTOPSSA , CUENTA)
				SELECT  DISTINCT  c.ClienteID, 		c.RFC,c.CURP , IdElementoProgressa, IdElementoProgressa,  Cuenta
				FROM CLIENTES c  
				INNER JOIN generales_credito gc
				ON gc.ClienteID=c.ClienteID;
			


	insert into  ultimo_pago(CreditoID,fecha_capital)
	with pago_capital as(

	select  distinct CreditoID, FechaPago, RANK() over (partition by CreditoID order by FechaPago desc ) Pos
	from  DETALLEPAGCRE
	where (MontoCapOrd+MontoCapOrd+MontoCapVen+MontoCapAtr)>0

	) select CreditoID,FechaPago  from pago_capital where Pos=1;



	with pago_capital as(
	select  distinct CreditoID, FechaPago, RANK() over (partition by CreditoID order by FechaPago desc ) Pos
	from  DETALLEPAGCRE
	where ( MontoIntOrd+MontoIntAtr+MontoIntVen)>0

	) UPDATE pago_capital p, ultimo_pago u
	SET u.fecha_interes=p.FechaPago
	where 
	p.CreditoID=u.CreditoID 
	and p.Pos=1;

	UPDATE generales_credito gc left join ultimo_pago up on gc.CreditoID=up.CreditoID
	SET gc.FULTPACAP=coalesce(fecha_capital,FCOMPRA),
		gc.FULTPAINT= coalesce(fecha_interes,FCOMPRA);
		




		INSERT INTO credito_incumplimiento(CreditoID,FechaIncumplimiento)
			SELECT c.CreditoID, FechaAtrasoCapital from CREDITOS c inner join lista_creditos l on c.CreditoID=l.CreditoID ;
			

		INSERT INTO credito_cuotas_activas(CreditoID,CuotaActual,CuotasAtraso)
			SELECT a.CreditoID,MAX(AmortizacionID),SUM(CASE WHEN Estatus in ('A','B') THEN 1 ELSE 0 END) 
			FROM AMORTICREDITO a 
			INNER JOIN lista_creditos lc  ON a.CreditoID=lc.CreditoID
			WHERE a.FechaInicio<=Var_FechaSistema
			AND Estatus <>'P'
			GROUP BY a.CreditoID;
			



		insert into tmp_fecha_atraso_actual(CreditoID,FechaAtraso)
			SELECT a.CreditoID,MIN(FechaVencim) FROM AMORTICREDITO a INNER JOIN lista_creditos lc ON a.CreditoID=lc.CreditoID
			WHERE Estatus in ('A','B')
			GROUP BY CreditoID;

		UPDATE tmp_fecha_atraso_actual fa INNER JOIN credito_incumplimiento fi ON fa.CreditoID=fi.CreditoID
		SET fi.FechaVencidoActual=FechaAtraso  where fi.CreditoID=fa.CreditoID;

		UPDATE credito_cuotas_activas ca 
		INNER JOIN credito_incumplimiento ci ON ca.CreditoID=ci.CreditoID
		SET ci.PAGOSVENCIDOS=CuotasAtraso,
			ci.NPARCIALIDAD=CuotaActual,
			ci.DIASVDO=DATEDIFF(Var_FechaSistema,COALESCE(FechaVencidoActual,Var_FechaSistema))
		WHERE ca.CreditoID=ci.CreditoID;


				
		INSERT INTO saldos_amortizacion (CreditoID, AmortizacionID, 											FechaExigible,
										Estatus,	SaldoCapital,												DiasAtraso)
				
				SELECT   a.CreditoID,	AmortizacionID ,														FechaExigible,
						Estatus, 		(SaldoCapVigente +SaldoCapAtrasa +SaldoCapVencido+SaldoCapVenNExi),	DATEDIFF( Var_FechaSistema,FechaExigible )  
				FROM  AMORTICREDITO a  
				INNER JOIN lista_creditos lc ON a.CreditoID=lc.CreditoID 
				WHERE  DATEDIFF( Var_FechaSistema,FechaExigible  )>0;





		INSERT INTO saldos_credito (CreditoID,				banda_id,	cuotas,
									SaldoCapital,			etiqueta)
				SELECT 	CreditoID,			banda_id,	count(*)Cuotas,
						SUM(SaldoCapital),	MAX(cabecera) 
				FROM saldos_amortizacion  sa , bandas_vencido  bv 
				WHERE  DiasAtraso BETWEEN  limite_inferior AND limite_superior
				GROUP  BY CreditoID,banda_id
				ORDER BY CreditoID;



			
		drop table if exists saldos_credito2;
		create temporary table saldos_credito2 
			SELECT * FROM saldos_credito;



		insert into saldo_vencido_banda (CreditoID,				SaldoCapital,				banda_id,
										etiqueta)
			SELECT  	COALESCE( sc.CreditoID,bv.CreditoID) ,	COALESCE(SaldoCapital,0),	bv.banda_id,
						bv.cabecera  
						FROM saldos_credito sc RIGHT  JOIN 
							(SELECT  DISTINCT CreditoID,b.banda_id  ,b.cabecera 
							FROM saldos_credito2  s
							INNER JOIN bandas_vencido b  ) bv 
						ON sc.banda_id=bv.banda_id  AND sc.CreditoID=bv.CreditoID
						ORDER BY sc.CreditoID,bv.banda_id;





		DROP TABLE IF EXISTS saldo_dias_vencido;
								
		SET CREATE_TABLE ="CREATE  TEMPORARY TABLE  saldo_dias_vencido"	;				
		SET SELECT_FIELDS=" SELECT CreditoID, ";
		SET FROM_TABLES =" FROM saldo_vencido_banda GROUP BY CreditoID;";
		SET CASE_FIELDS=( SELECT  
									GROUP_CONCAT( 
									concat(" SUM(CASE WHEN banda_id=",banda_id," THEN SaldoCapital ELSE 0 END) AS  " ,cabecera) )
									FROM bandas_vencido);
						
		SET @SQL_QUERY = CONCAT(CREATE_TABLE, SELECT_FIELDS	,CASE_FIELDS, FROM_TABLES);
		PREPARE QUERY FROM @SQL_QUERY;
		EXECUTE QUERY;
		DEALLOCATE PREPARE QUERY;



		INSERT INTO  banda_pago_mensual (banda_id)
			SELECT  @row := @row + 1 AS id
			FROM    CLIENTES  p   
			JOIN    (SELECT @row := -1) r
			WHERE   @row<18;
				
			
		UPDATE banda_pago_mensual SET cabecera = CONCAT('PAGOMES', LPAD(cast(banda_id as CHAR ),2,'00'));
			
		UPDATE banda_pago_mensual 
		SET fecha_termina=LAST_DAY(date_add(Var_FechaSistema, INTERVAL banda_id MONTH) );

		UPDATE banda_pago_mensual 
		SET fecha_inicio=DATE_SUB(fecha_termina, INTERVAL  DAYOFMONTH(fecha_termina) -1 DAY );




		INSERT INTO pago_mes_banda (banda_id,		CreditoID,		cabecera,
									Capital,		
									Interes,		
									Accesorios)
			
			SELECT MAX(banda_id)mes_id ,		a.CreditoID, 		MAX(cabecera)cabecera, 
			SUM(CASE WHEN  FechaVencim < Var_FechaSistema THEN  SaldoCapVigente+SaldoCapAtrasa+SaldoCapVencido+SaldoCapVenNExi ELSE  Capital END  ) as Capital,
			(SUM(CASE WHEN  FechaVencim > Var_FechaSistema THEN  SaldoInteresPro + SaldoInteresAtr + SaldoInteresVen + SaldoIntNoConta ELSE  (SaldoInteresPro + SaldoInteresAtr + SaldoInteresVen + SaldoIntNoConta)+ (Interes-ProvisionAcum) END) * FACTOR_IVA)as Interes,
			0 as Accesorios
			FROM banda_pago_mensual 
			INNER JOIN ( AMORTICREDITO a USE INDEX(fk_AMORTICREDITO_1,IDX_AMORTICREDITO_6_FecVen) 
			INNER JOIN lista_creditos lc ON a.CreditoID=lc.CreditoID)   
			WHERE  a.FechaVencim BETWEEN fecha_inicio AND fecha_termina
			AND Estatus!='P' 
			GROUP  BY a.CreditoID,banda_id
			ORDER BY a.CreditoID, banda_id;

		DROP TABLE IF EXISTS saldo_pago_mes;


		SET CREATE_TABLE ="CREATE  TEMPORARY TABLE  saldo_pago_mes"	;				
		SET SELECT_FIELDS=" SELECT CreditoID, ";
		SET  FROM_TABLES =" FROM pago_mes_banda  GROUP BY CreditoID;";
									
		SET CASE_FIELDS=( SELECT  
									GROUP_CONCAT( 
									concat(" SUM(CASE WHEN banda_id=",banda_id," THEN (Capital + Interes) ELSE 0 END) AS  " ,cabecera) )
									FROM banda_pago_mensual);
		
		SET @SQL_QUERY= CONCAT(CREATE_TABLE, SELECT_FIELDS	,CASE_FIELDS, FROM_TABLES);


		PREPARE QUERY  FROM  @SQL_QUERY;
		EXECUTE QUERY;
		DEALLOCATE PREPARE QUERY;




	
		
		SELECT '' as FolioActualizacion, 		'SAFI' as SistemaOriginaActividad, 		'SAFI' UsuarioOriginaActividad , 
		''  FechaHoraCreaRegistro,  			IDELEMENTO NumeroPersonaElemento,		IDELEMENTOPSSA  NumeroPersonaElementoProgressa,
		'DETALLE' NivenAcumulado , 				'PROD-FIN' Segmento , 					ORIPDTO OrigenProducto, 
		(CASE WHEN IDPDTO= 1007 THEN 'L24' ELSE (CASE WHEN IDPDTO=1008 THEN 'L25' ELSE IDPDTO END)END ) IdProducto,							0 Negocio, 								CUENTA Cuenta, 
		coalesce(SERIEPGSSA,'') Serie, 						TDASUCPGSA NumeroTienda, 				'' TipoCuenta,
		'' GrupoTasa , 							PLAZOMAX PlazoMaximo, 					IDPROGSSA IdentificadorProgressa, 
		CTAPROGSSA CuentaProgressa,				'' SerieCuentaProgressa,				0 NumeroSucursalProgressa, 
		cast(coalesce(TPOCTAPGSA,0) as char)TipoCuentaProgressa, 		lpad(cast(ESTATUS as char),4,'0') EstatusCuenta, 					LIMITE LimiteCredito, 
		'0.00' PorcentajeLimiteDisponible,		'0.0' CapacidadPago, 					'0.0' CapacidadDisponible, 
		'0.0' CapacidadPagoProductoFinanciero,	'0.0' CapacidadPagoPFUsado, 			'0.0' MotoParaDisponer,
		SDOCTA SaldoCuenta, 					COALESCE(VDO,0) VencidoCapital, 		COALESCE(VDO30,0) VencidoCapital30Dias,
		COALESCE(VDO60,0) VencidoCapital60Dias, COALESCE(VDO90,0) VencidoCapital90Dias, COALESCE(VDOM90,0) VencidoCapitalMas90Dias, 
		IFIN SaldoInteresFinanciero, 			IMOR InteresMoratorio, 					DATEDIFF(Var_FechaSistema,COALESCE(FechaVencidoActual,Var_FechaSistema))DiasVencido, 
		coalesce(PAGOMES00,0)PagoMes00,		coalesce(PAGOMES01,0)PagoMes01,	coalesce(PAGOMES02,0)PagoMes02,
		coalesce(PAGOMES03,0)PagoMes03,		coalesce(PAGOMES04,0)PagoMes04,	coalesce(PAGOMES05,0)PagoMes05,
		coalesce(PAGOMES06,0)PagoMes06,		coalesce(PAGOMES07,0)PagoMes07,	coalesce(PAGOMES08,0)PagoMes08,
		coalesce(PAGOMES09,0)PagoMes09,		coalesce(PAGOMES10,0)PagoMes10,	coalesce(PAGOMES11,0)PagoMes11,
		coalesce(PAGOMES12,0)PagoMes12,		coalesce(PAGOMES13,0)PagoMes13,	coalesce(PAGOMES14,0)PagoMes14, 
		coalesce(PAGOMES15,0)PagoMes15,		coalesce(PAGOMES16,0)PagoMes16,	coalesce(PAGOMES17,0)PagoMes17, 
		coalesce(PAGOMES18,0)PagoMes18, 	'0' NumeroTransaccion, 					'' IdParcialidad,
		FULTPACAP FechaUltimoPagoCapital,		FULTPAINT FechaUltimoPagoInteres,		ci.FechaIncumplimiento FechaPrimerIncumplimiento, 
		CTAPROGSSA PrestamoId, 				'' FolioArchivoMaestro,					 date_format(now(),"%Y-%m-%d %H:%i:%s.%f")FechaHoraDatos, 
		c.RFC Rfc,								c.CURP  Curp
		FROM 
		saldo_pago_mes spm  
		RIGHT JOIN  (generales_credito gc 
		INNER JOIN generales_cliente c ON gc.ClienteID=c.ClienteID )  ON spm.CreditoID=gc.CreditoID  
		LEFT JOIN saldo_dias_vencido sv ON gc.CreditoID=sv.CreditoID 	 
		LEFT JOIN credito_incumplimiento ci ON gc.CreditoID=ci.CreditoID;
		
		UPDATE microfin.PGSSERVICIOKEY SET Valor=Var_TransaccionFin  WHERE ServicioID=1  AND KeyID=1;


	
	
    DROP TABLE IF EXISTS lista_creditos;
    DROP TABLE IF EXISTS generales_cliente;
	DROP TABLE IF EXISTS generales_credito;
	DROP TABLE IF EXISTS ultimo_pago;
	DROP TABLE IF EXISTS bandas_vencido;
	DROP TABLE IF EXISTS saldos_amortizacion;
	DROP TABLE IF EXISTS saldos_credito;
	DROP TABLE IF EXISTS saldo_vencido_banda;
	DROP TABLE IF EXISTS credito_incumplimiento;
	DROP TABLE IF EXISTS credito_cuotas_activas;
	DROP TABLE IF EXISTS tmp_fecha_atraso_actual;
	DROP TABLE IF EXISTS banda_pago_mensual;
	DROP TABLE IF EXISTS pago_mes_banda;	
	DROP TABLE IF EXISTS saldos_credito2;
	DROP TABLE IF EXISTS saldo_dias_vencido;
	DROP TABLE IF EXISTS saldo_pago_mes;
	END IF;
--


END TerminaStore$$