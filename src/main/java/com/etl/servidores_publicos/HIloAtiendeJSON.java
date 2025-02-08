package com.etl.servidores_publicos;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;

public class HIloAtiendeJSON extends Thread {
    private Map<String, Object> json;
    private String id;
    private String integer;
    private String bigint;
    private String varchar;
    private String datetime;
    private String date;
    private String time;
    private String bit;

    public HIloAtiendeJSON
    (
        Map<String, Object> json,
        String integer,
        String bigint,
        String varchar,
        String datetime,
        String date,
        String time,
        String bit
    )
    {                                    
        this.json = json;
        this.integer = integer;
        this.bigint = bigint;
        this.varchar = varchar;
        this.datetime = datetime;
        this.date = date;
        this.time = time;
        this.bit = bit;
    }
    public void run()
    {
        try {
            this.getIdPersona();
            this.crearTabla(this.json, null);
        } catch (Exception e) {}
        Menu.setDecreHilosConcu();
    }
    
    private int getIdPersona() throws Exception
    {
        this.id = (String)this.json.get("id");
        return 0;
    }
    private int crearTabla(Map<String, Object> elemento, String tabla)throws Exception
    {
        String alter = "ALTER TABLE ";
        String insert = "INSERT INTO ";
        String values = "";
        ArrayList<String> claves = new ArrayList<String>(elemento.keySet());

        if( tabla != null )
        {
            Menu.ejecutarQuery(
                "CREATE TABLE "+tabla+
                " ( id_"+tabla+" BIGINT PRIMARY KEY IDENTITY, "+
                "id_persona BIGINT);"
            );
            alter += tabla+" ";
            insert += tabla+" (id_persona, ";
            values += this.id+",";
        }

        for( int i=0; i<claves.size(); i++ )
        {
            String crearColumna = claves.get(i);
            Object valorInsert = elemento.get(crearColumna);

            crearColumna = crearColumna.toLowerCase();
            
            if( valorInsert != null )
            {
                String tipoClase = this.getTipoClaseJson(valorInsert);

                if( tipoClase.equals("java.util.ArrayList") )
                    this.recorrerArreglo((ArrayList<Map<String, Object>>)valorInsert);
                else if( tipoClase.equals("com.google.gson.internal.LinkedTreeMap") )
                    this.crearTabla((Map<String, Object>)valorInsert, crearColumna);
                else if( tabla != null )
                {
                    Menu.ejecutarQuery
                    (
                        alter + "ADD "                                  +
                        crearColumna+" "                                  +
                        this.tipoDato(valorInsert)+"; "    
                    );
                    insert += crearColumna+",";
                    values += this.convertToCadena(valorInsert, tipoClase)+",";
                }
            }
        }

        if( tabla != null )
        {
            insert = this.eliminaUltimaComa(insert.trim())+") VALUES ( ";
            values = this.eliminaUltimaComa(values.trim())+");";
            Menu.guardarDatos(insert+values);
        }

        claves.clear();
        elemento.clear();
        return 0;
    }
    private String eliminaUltimaComa(String cadena) throws Exception
    {
        return cadena.substring(0, cadena.length()-1);
    }
    private int recorrerArreglo(ArrayList<Map<String, Object>> lista)throws Exception
    {
        for(int i=0; i<lista.size(); i++)
        {
            Object elemento = lista.get(i);

            if( elemento != null )
            {
                String temp = this.getTipoClaseJson(elemento);

                if( temp.equals("java.util.ArrayList") )
                    this.recorrerArreglo((ArrayList<Map<String, Object>>)elemento);
                else
                    this.crearTabla((Map<String, Object>)elemento, null);
            }
        }

        lista.clear();
        return 0;
    }
    private String tipoDato (Object val)throws Exception
    {
        ZonedDateTime fechHor= null;
        LocalDate fecha = null;
        LocalTime tiempo = null;
        Long numero = null;
        String valor = null;

        try {
            valor = (String) val;
        } catch (Exception e) {
            valor = null;
        }

        try {
            Boolean bool = (Boolean)val;
            return this.bit;
        } catch (Exception e) {}

        try {
            numero = Long.parseLong(valor);
        } catch (Exception e) {
            numero = null;
        }

        try {
            fechHor = ZonedDateTime.parse(valor, DateTimeFormatter.ISO_DATE_TIME);
            return this.datetime;
        } catch (Exception e) {}

        try {
            fecha = LocalDate.parse(valor);
            return this.date;
        } catch (Exception e) {}

        try {
            tiempo = LocalTime.parse(valor);
            return this.time;
        } catch (Exception e) {}

        if( numero != null && numero >= -2147483648 && numero <= 2147483647 )
            return  this.integer;
        else if( numero != null && numero >= new Long("-9223372036854775808") && numero <= new Long("9223372036854775807") )
            return this.bigint;
         
        return this.varchar;
    }

    private String convertToCadena(Object objeto, String clase)throws Exception
    {
        ZonedDateTime fechHor= null;
        LocalDate fecha = null;
        LocalTime tiempo = null;

        if( clase.equals("java.lang.Boolean") )
            return (Boolean)objeto ? "1" : "0";

        if( clase.equals("java.lang.Double") )
            return String.valueOf((Double)objeto);

        try {
            fechHor = ZonedDateTime.parse((String)objeto, DateTimeFormatter.ISO_DATE_TIME);
            return "'"+(String)objeto+"'";
        } catch (Exception e) {}

        try {
            fecha = LocalDate.parse((String)objeto);
            return "'"+(String)objeto+"'";
        } catch (Exception e) {}

        try {
            tiempo = LocalTime.parse((String)objeto);
            return "'"+(String)objeto+"'";
        } catch (Exception e) {}

        return "'"+(String)objeto+"'";
    }
    private String getTipoClaseJson(Object obj)throws Exception
    {
        Class<?> tipo = obj.getClass();
        return tipo.getCanonicalName();
    }
}
