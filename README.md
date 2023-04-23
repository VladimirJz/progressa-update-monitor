PROGRESSA UPDATES MONITOR (pgssum).

Servicio initd para monitorear la actualización de saldos sobre productos Progressa.


1 : Instalar el servicio initd
copiar:
    
    pgssum.service 
    (Controla el inicio y administración del servicio Linux)

    
a:
    /etc/systemd/system/

2: Copiar los Scripts de Ejecución
    pgssum.py
    (Es ejecutado al iniciar el servicio pgssum.service,administra la ejecución de un script python cada determinado tiempo, como parte del loop del servicio.)

    update_monitor.py
    (Es la rutina principal, todo cambio en la lógica de ejecución debe ser realizado aqui o mediante el archivo de configuración safi.cfg)


Inciar servicio

#> service pgssum start

Verificar el estatus del servicio:

#> service pgssum status

        ● pgssam.service - Progressa - Update Monitor Service
            Loaded: loaded (/etc/systemd/system/pgssam.service; disabled; vendor preset: enabl>
            Active: active (running) since Wed 2023-04-19 12:16:54 CST; 3 days ago
        Main PID: 2546875 (python3)
            Tasks: 1 (limit: 19115)
            Memory: 19.9M
            CGroup: /system.slice/pgssum.service
                    └─2546875 /opt/progressa/srv/update-monitor/dev/bin/python3 /opt/progres>


