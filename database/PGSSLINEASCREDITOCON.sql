delimiter ;
drop PROCEDURE if exists PGSSLINEASCREDITOCON ;
CREATE DEFINER=`root`@`%` PROCEDURE `PGSSLINEASCREDITOCON`(
Par_FechaInicio date,
Par_FechaFin date,
Par_TipoConsulta char,
Par_Instrumento  bigint,
Par_EmpresaID				INT(11),				
Aud_Usuario					INT(11),				
Aud_FechaActual				DATETIME,				
Aud_DireccionIP				VARCHAR(15),			
Aud_ProgramaID				VARCHAR(50),			
Aud_Sucursal				INT(11),				
Aud_NumTransaccion			BIGINT(20)	   
)
TerminaStore: BEGIN

    DECLARE Entero_Cero INT;
    DECLARE Valor_No CHAR;
    DECLARE Var_FechaSistema DATE;
	DECLARE Var_CierreDia CHAR;
	DECLARE Estatus_inactivo char;
	DECLARE Var_InicioMes date;
	DECLARE Filtro_Pago char;
	DECLARE Filtro_Fecha char;
	
	DECLARE NumErr int;
	DECLARE ErrMen varchar(200);
	DECLARE Control varchar(20);
	DECLARE Consecutivo int;
	DECLARE Busqueda char;
    DECLARE CONTROL_LINEAS int;
    DECLARE Valor_Key bigint;
    DECLARE Var_Transaccion int;
    DECLARE Var_TransaccionFinal int;
    DECLARE TRANSACCION int;
   	DECLARE POR_TRANSACCION char;
   	
   	SET Var_Transaccion=0;
	SET POR_TRANSACCION='T';
	SET CONTROL_LINEAS=3;
    SET TRANSACCION=1;
    SET Entero_Cero=0;
    SET Valor_No='N';
	SET Estatus_Inactivo='I';
	SET Filtro_Pago='N';

	drop table if exists tmp_lista_creditos;
  	CREATE temporary TABLE tmp_lista_creditos
  	(
  		CreditoID bigint,
  		FechaInicio date
  	);

  	
  	SET Valor_Key=(SELECT coalesce(Valor,0) from PGSSERVICIOKEY where ServicioID=CONTROL_LINEAS and KeyID=TRANSACCION);

  	IF (Par_Instrumento >0 and Par_TipoConsulta = POR_TRANSACCION )THEN
  		SET Var_Transaccion=Par_Instrumento;
  	ELSE 
  		SET Var_Transaccion=Valor_Key;
  	END IF;
  	
  
  	

  	SET Var_FechaSistema=(SELECT FechaSistema FROM PARAMETROSSIS);
  	SET Var_CierreDia=(select ValorParametro from PARAMGENERALES where LlaveParametro='EjecucionCierreDia');
  	SET Var_InicioMes= DATE_ADD(LAST_DAY(DATE_ADD(Var_FechaSistema,INTERVAL -1  MONTH)),INTERVAL 1 DAY);
    IF(Par_FechaInicio='1900-01-01')THEN
		SET Par_FechaInicio=Var_FechaSistema;
        SET Par_FechaFin=Var_FechaSistema;
    END IF;

	SELECT Var_FechaSistema, Par_FechaFin, Par_FechaInicio;
	ManejoErrores: BEGIN
  	
	IF (Var_CierreDia=Valor_No)THEN
		
		SET Var_TransaccionFinal=(select NumTransaccion from LINEASCREDITO order by NumTransaccion desc limit 1);

		DROP TABLE IF EXISTS lista_creditos;
		CREATE TEMPORARY TABLE lista_creditos
		(
			CreditoID BIGINT NOT NULL,
			PRIMARY KEY (CreditoID)
		);
		
		DROP TABLE IF EXISTS lineas_credito;
		CREATE temporary table lineas_credito
		(
			LineaCreditoID bigint,
			IdElemento int,
			ClienteID int,
			ProductoCreditoID int,
			CuentaAhoID bigint,
			Estatus char,
			Cuenta int,
			RFC varchar(13),
			CURP varchar(28),
			FechaRegistro DATE,
			FechaInicio DATE,
			FechaVencimiento DATE,
			FechaAutoriza DATE,
			Autorizado DECIMAL(12,2)	,
			PRIMARY KEY (LineaCreditoID)
		);
		
		
		insert into lista_creditos(CreditoID)
			SELECT LineaCreditoID FROM LINEASCREDITO 
			where ((FechaAutoriza between Par_FechaInicio and Par_FechaFin) or
				(FechaCancelacion between Par_FechaInicio and Par_FechaFin) or 
				(FechaVencimiento between Par_FechaInicio and Par_FechaFin))  
				and NumTransaccion>Var_Transaccion and NumTransaccion<=Var_TransaccionFinal;
		
		insert into lineas_credito(LineaCreditoID,IdElemento,ClienteID,ProductoCreditoID,Estatus,
									Cuenta,RFC,CURP,FechaRegistro,FechaInicio,
									FechaVencimiento,FechaAutoriza,Autorizado,CuentaAhoID)
		
			select LineaCreditoID,cl.IdElementoProgressa,cl.ClienteID,li.ProductoCreditoID,li.Estatus,
				cl.Cuenta,RFC,CURP,FechaRegistro,FechaInicio,
				FechaVencimiento,FechaAutoriza,Autorizado , CuentaAhoID 
			from (LINEASCREDITO li inner join (CLIENTES cl inner join CUENTASAHO c on cl.ClienteID=c.ClienteID and c.EsPrincipal='S') on li.ClienteID=cl.ClienteID ) inner join lista_creditos lc on li.LineaCreditoID=lc.CreditoID;
		


		
		select IdElemento as numeroElementoTitularBodesa,
				ClienteID as numeroElementoTitularProgressa,
				"SAFI" as Origen,
				cast(ProductoCreditoID as char) as idProducto,
				(case when Estatus='A' then '001' else (case when Estatus='C' then '008' else '000' end)end ) as EstatusCuenta,
				8 as NegocioBodesa,
				Cuenta as cuentaBodesa,
				cast(ClienteID as char) as identificadorProgressa,
				cast(CuentaAhoID as char) as cuentaProgressa,
				cast(0 as char) as serieProgressa,
				cast(LineaCreditoID as char) as idLineaCredito,
				RFC as Rfc,
				CURP as Curp,
				FechaInicio as FechaOtorga,
				FechaVencimiento as FechaVencimiento,
				Autorizado as MontoLinea	 
				 from lineas_credito;
			UPDATE PGSSERVICIOKEY set Valor=Var_TransaccionFinal where ServicioID=CONTROL_LINEAS and KeyID=TRANSACCION;
		LEAVE TerminaStore;
		
	
	END IF;
--
	END ManejoErrores;
	Select NumErr, ErrMen;
	

END TerminaStore$$