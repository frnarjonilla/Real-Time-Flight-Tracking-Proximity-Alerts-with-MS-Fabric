import requests
import json
import time
from azure.eventhub import EventHubProducerClient, EventData
from pyspark.sql import Row

# --- TUS DATOS DEL EVENTSTREAM (Copiados de la pestaña SAS) ---
CONNECTION_STR = "Endpoint=sb://[TU_NAMESPACE].servicebus.windows.net/;SharedAccessKeyName=[TU_KEY_NAME];SharedAccessKey=[TU_KEY];EntityPath=[TU_PATH]"
EVENTHUB_NAME = "[TU_EVENTHUB_NAME]"

# Área de España
URL = "https://opensky-network.org/api/states/all?lamin=35.0&lamax=44.0&lomin=-10.0&lomax=4.0"


def radar_con_guardado_parquet():
    producer = EventHubProducerClient.from_connection_string(CONNECTION_STR, eventhub_name=EVENTHUB_NAME)
    
    # Lista para acumular datos antes de guardar en Parquet
    acumulado_vuelos = []
    # Marca de tiempo para controlar los 5 minutos (300 segundos)
    ultima_escritura = time.time()

    print("🚀 Radar activo. Enviando a Event Hub y acumulando para Parquet...")

    while True:
        try:
            response = requests.get(URL, timeout=15)
            if response.status_code == 200:
                data = response.json()
                states = data.get('states', [])
                
                if states:
                    with producer:
                        batch = producer.create_batch()
                        for s in states:
                           vuelo = {
                                "icao24": str(s[0]),
                                "vuelo": str(s[1]).strip() if s[1] else "N/A",
                                "lat": float(s[6]) if s[6] is not None else 0.0,
                                "lon": float(s[5]) if s[5] is not None else 0.0,
                                "altitud": float(s[7]) if s[7] is not None else 0.0, # <--- Forzamos float
                                "velocidad": float(s[9] * 3.6) if s[9] is not None else 0.0, # <--- Forzamos float
                                "timestamp": int(time.time())
                                }
                            # 1. Añadimos al lote de Event Hub
                        batch.add(EventData(json.dumps(vuelo)))
                            # 2. Añadimos a la lista para el archivo Parquet
                        acumulado_vuelos.append(vuelo)
                        
                        producer.send_batch(batch)
                        print(f"✅ {len(states)} aviones enviados a Event Hub.")

            # --- LÓGICA DE GUARDADO CADA 2 MINUTOS ---
            tiempo_actual = time.time()
            if tiempo_actual - ultima_escritura >= 120: # 120 segundos = 2 min
                if acumulado_vuelos:
                    print("💾 Guardando lote de 2 minutos en Parquet...")
                    
                    # Convertimos la lista de diccionarios en DataFrame de Spark
                    df = spark.createDataFrame([Row(**v) for v in acumulado_vuelos])
                    print("hola 1")
                    # Guardamos en la carpeta 'Files' del Lakehouse
                    # Usamos 'append' para no borrar lo anterior
                    df.write.mode("append").parquet("Files/historico_vuelos_parquet")
                    print("hola 2")
                    # Limpiamos la lista y actualizamos el cronómetro
                    acumulado_vuelos = []
                    ultima_escritura = tiempo_actual
                    print("✨ Archivo guardado correctamente en Lakehouse.")

            time.sleep(25) # Pausa para no saturar la API

        except Exception as e:
            print(f"❌ Error: {e}")
            time.sleep(10)

radar_con_guardado_parquet()
