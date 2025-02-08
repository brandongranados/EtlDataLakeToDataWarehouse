package com.etl.servidores_publicos;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Map;

import com.google.gson.Gson;

public class HiloCrearEstructura extends Thread {
    private ArrayList<Map<String, Object>> recorre;
    private File raiz;
    private Gson json;
    private String integer;
    private String bigint;
    private String varchar;
    private String datetime;
    private String date;
    private String time;
    private String bit;

    public HiloCrearEstructura
    (
        File raiz,
        String integer,
        String bigint,
        String varchar,
        String datetime,
        String date,
        String time,
        String bit
    )
    {
        this.raiz = raiz;
        this.integer = integer;
        this.bigint = bigint;
        this.varchar = varchar;
        this.datetime = datetime;
        this.date = date;
        this.time = time;
        this.bit = bit;
        this.json = new Gson();
    }

    public void run()
    {
        try {
            Object lista = json.fromJson(this.getCadena(), Object.class);
            String clase = this.getTipoClaseJson(lista);
            int tam = 0;

            if( clase.equals("java.util.ArrayList") )
            {
                this.recorre = (ArrayList<Map<String, Object>>) lista;
                tam = this.recorre.size();
                this.recursivo(tam-1);
                this.recorre.clear();
            }
            else
                new HIloAtiendeJSON(
                    (
                        Map<String, Object>)lista,
                        this.integer,
                        this.bigint,
                        this.varchar,
                        this.datetime,
                        this.date,
                        this.time,
                        this.bit
                    ).
                    start();
        } catch (Exception e) {}

        Menu.setDecreHilosConcu();
        return;
    }
    private String getTipoClaseJson(Object obj)throws Exception
    {
        Class<?> tipo = obj.getClass();
        return tipo.getCanonicalName();
    }
    private int recursivo(int iterador) throws Exception
    {
        if( iterador <= 0 )
            return 0;

        Menu.verificaConcurrecia();
        new HIloAtiendeJSON
            (
                this.recorre.get(iterador),
                this.integer,
                this.bigint,
                this.varchar,
                this.datetime,
                this.date,
                this.time,
                this.bit
            ).
            start();

        return this.recursivo(--iterador);
    }
    private String getCadena()throws Exception
    {
        String salida = null;
        FileInputStream ent = null;
        byte crudo[] = null;

        try {
            ent = new FileInputStream(this.raiz);
            crudo = new byte[ent.available()];
            
            ent.read(crudo);
            salida = new String(crudo);
        } catch (Exception e) {
            salida = null;
        }
        finally{   try {   ent.close();    } catch (Exception e1) {}    }

        return salida;
    }
}
