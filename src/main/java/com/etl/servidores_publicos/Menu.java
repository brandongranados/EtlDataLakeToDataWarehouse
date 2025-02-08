package com.etl.servidores_publicos;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.concurrent.Semaphore;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Menu extends Thread {

    //CONCURRENCIA SE MANEJA DE MANERA GLOBAL
    private static String estructura;
    private static String datos;
    private static int hilosConcurrentes = 0;
    private static int hilosTerminan = 0;
    private static final Semaphore semaforo = new Semaphore(1);

    public static void ejecutarQuery(String query)
    {
        BufferedWriter salida = null;
        
        if( query == null )
            return;

        try {

            semaforo.acquire();

            salida = new BufferedWriter(new FileWriter(estructura, true));
            salida.write(query+"\n");
            salida.close();

            semaforo.release();
        } catch (Exception e) {}
        finally{    semaforo.release();     }
    }
    public static void guardarDatos(String query)
    {
        BufferedWriter salida = null;
        
        if( query == null )
            return;

        try {

            semaforo.acquire();

            salida = new BufferedWriter(new FileWriter(datos, true));
            salida.write(query+"\n");
            salida.close();

            semaforo.release();
        } catch (Exception e) {}
        finally{    semaforo.release();     }
    }
    public static void setDecreHilosConcu()
    {
        try {
            semaforo.acquire();
            hilosConcurrentes--;
            hilosTerminan++;
            System.out.println("TERMINARON: "+hilosTerminan);
            System.out.println("CONCURRENTES: "+hilosConcurrentes);
            semaforo.release();
        } catch (Exception e) {
            try {   semaforo.release();  } catch (Exception e1) {}
        }
        finally{    semaforo.release();     }
    }
    public static void verificaConcurrecia()
    {
        while(true)
            try {
                semaforo.acquire();

                if( hilosConcurrentes >= 1000 )
                {
                    semaforo.release();
                    sleep(5000);
                }
                else
                {
                    hilosConcurrentes++;
                    semaforo.release();
                    throw new Exception();
                }
                
            } catch (Exception e) {
                try {   semaforo.release();  } catch (Exception e1) {}
                break;
            }
            finally{    semaforo.release();     }
    }


    @Value("${sql.estructura}")
    private String rutaEstructura;
    @Value("${sql.datos}")
    private String rutaDatos;
    @Value("${json.leer.raiz}")
    private String directorio;
    @Value("${sqlServer.tiposDatos.int}")
    private String integer;
    @Value("${sqlServer.tiposDatos.bigint}")
    private String bigint;
    @Value("${sqlServer.tiposDatos.varchar}")
    private String varchar;
    @Value("${sqlServer.tiposDatos.datetime}")
    private String datetime;
    @Value("${sqlServer.tiposDatos.date}")
    private String date;
    @Value("${sqlServer.tiposDatos.time}")
    private String time;
    @Value("${sqlServer.tiposDatos.bit}")
    private String bit;

    @Bean
    int inicioProyecto()
    {
        File raiz = new File(directorio);
        estructura = rutaEstructura;
        datos = rutaDatos;
        
        return raiz.isFile()                            ? 
                this.crearHiloLecturaJson(raiz)      :
                this.lecturaCarpetasRecursiva(raiz)     ;
    }
    private int lecturaCarpetasRecursiva(File raiz)
    {
        File sub[] = raiz.listFiles();

        if( raiz.isFile() )
            return this.crearHiloLecturaJson(raiz);

        if( sub.length <= 0 )
            return 0;

        for( int i=0; i<sub.length; i++ )
            this.lecturaCarpetasRecursiva(sub[i]);

        return 0;
    }
    private int crearHiloLecturaJson(File raiz)
    {
        verificaConcurrecia();

        new HiloCrearEstructura(
                raiz,
                integer,
                bigint,
                varchar,
                datetime,
                date,
                time,
                bit
            )
            .start();
        
        return 0;
    }
}
