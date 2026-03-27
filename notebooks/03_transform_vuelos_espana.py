import json
import pandas as pd

file_path = "/lakehouse/default/Files/bronze_vuelos_raw.json"

try:
    with open(file_path, "r") as f:
        data = json.load(f)
    
    if data.get('states') is None or len(data['states']) == 0:
        print("⚠️ No hay datos nuevos para procesar.")
    else:
        col_names = [
            "icao24", "callsign", "pais_origen", "time_position", "last_contact", 
            "longitud", "latitud", "altitud_baro", "en_tierra", "velocidad", 
            "direccion", "vertical_rate", "sensors", "altitud_geo", "squawk", 
            "spi", "position_source"
        ]

        # 1. Crear DataFrame y limpiar columnas problemáticas
        df_vuelos = pd.DataFrame(data['states'], columns=col_names)
        df_vuelos = df_vuelos.drop(columns=['sensors', 'spi', 'position_source'])

        # 2. Filtrar por España
        df_vuelos = df_vuelos[df_vuelos['pais_origen'] == 'Spain']

        # 3. CONVERSIONES MATEMÁTICAS
        # Velocidad: de m/s a km/h (multiplicar por 3.6)
        df_vuelos['velocidad_kmh'] = df_vuelos['velocidad'] * 3.6
        
        # Altitud: de pies (generalmente) a metros (multiplicar por 0.3048 si viniera en pies, 
        # pero OpenSky ya da metros, así que solo redondeamos para que quede limpio)
        df_vuelos['altitud_metros'] = df_vuelos['altitud_baro'].round(0)

        # 4. Limpieza final
        df_vuelos['callsign'] = df_vuelos['callsign'].str.strip()
        df_vuelos = df_vuelos.dropna(subset=['latitud', 'longitud'])

       # 5. Guardar en el Lakehouse forzando el nuevo esquema
        spark_df = spark.createDataFrame(df_vuelos)
        
        (spark_df.write 
            .format("delta") 
            .mode("overwrite") 
            .option("overwriteSchema", "true") # <--- Esta es la llave que abre el candado
            .saveAsTable("vuelos_espana"))

        print(f"✅ ¡Tabla 'vuelos_espana' actualizada con el nuevo esquema!")

        print(f"✅ Tabla 'vuelos_espana' actualizada.")
        print(f"📊 Datos listos: {len(df_vuelos)} aviones con velocidad en km/h.")

except Exception as e:
    print(f"❌ Error: {e}")
