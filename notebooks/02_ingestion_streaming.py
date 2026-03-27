import requests
import json
import os

# 1. URL de la API
url = "https://opensky-network.org/api/states/all"

# 2. Definir la ruta usando la variable de entorno de Fabric para no fallar
# Esto busca automáticamente el ID de tu Lakehouse activo
lakehouse_path = "/lakehouse/default/Files"
file_name = "bronze_vuelos_raw.json"
full_path = os.path.join(lakehouse_path, file_name)

try:
    response = requests.get(url)
    data = response.json()
    
    # 3. Escribir el archivo
    with open(full_path, "w") as f:
        json.dump(data, f)
    
    print(f"✅ ¡Éxito! Archivo guardado en: {full_path}")
    
    # Verificación extra: listar archivos en la carpeta
    print("Contenido actual de la carpeta Files:", os.listdir(lakehouse_path))

except Exception as e:
    print(f"❌ Error: {e}")
