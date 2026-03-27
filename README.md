# ✈️ Real-Time Flight Radar & Data Engineering Pipeline
### End-to-End Architecture: Python + Azure Event Hubs + Microsoft Fabric + Power BI

Este proyecto implementa una arquitectura de datos híbrida (Lambda) para el seguimiento de activos aéreos en tiempo real. Combina la ingesta masiva (Batch) con el procesamiento de eventos por segundo (Streaming), culminando en un entorno de análisis avanzado en Microsoft Fabric.

---

## 🏗️ Arquitectura del Sistema

El flujo de datos está diseñado para garantizar la integridad y la escalabilidad:

1.  **Capa Bronze (Raw):** Ingestión directa de la API OpenSky a archivos JSON en el Lakehouse (`ingestion_batch.py`).
2.  **Capa Silver (Transform):** Limpieza, filtrado geográfico y conversión de unidades (m/s a km/h) usando Spark y Delta Lake (`transform_vuelos_espana.py`).
3.  **Capa Speed (Streaming):** Transmisión de telemetría viva mediante **Azure Event Hubs** para visualización instantánea (`ingestion_streaming.py`).
4.  **Capa Gold (Analytics):** Modelado en Power BI para detección de proximidad y KPIs operativos.



---

## 🛠️ Stack Tecnológico

* **Lenguajes:** Python 3.10, PySpark (Spark SQL).
* **Ingestión:** Azure Event Hubs (Kafka Interface).
* **Orquestación y Almacenamiento:** Microsoft Fabric (Lakehouse & Delta Tables).
* **Formatos:** JSON, Apache Parquet.
* **BI:** Power BI (DirectQuery para tiempo real).

---

## 🚀 Desafíos Técnicos Resueltos

### 1. Manejo de Esquemas Dinámicos (Schema Evolution)
Se implementó `.option("overwriteSchema", "true")` en las tablas Delta para permitir que el pipeline se adapte a cambios en la API de origen sin romper los procesos de carga, garantizando la continuidad del negocio.

### 2. Optimización de Escritura (Small Files Problem)
Para evitar la degradación del rendimiento en el Data Lake, el script de streaming acumula mensajes en memoria y realiza una escritura física en Parquet cada 120 segundos, reduciendo la latitud de metadatos en Spark.

### 3. Integridad de Tipos (Data Casting)
Se resolvió el conflicto de mezcla de tipos (`DoubleType` vs `LongType`) mediante el casting explícito de métricas críticas (altitud y velocidad), asegurando un esquema consistente para el motor de Power BI.

---

## 📂 Estructura del Repositorio

* `notebooks/01_ingestion_batch.py`: Script de captura masiva a capa Bronze.
* `notebooks/02_ingestion_streaming.py`: Productor de eventos para Azure Event Hubs.
* `notebooks/03_transform_vuelos_espana.py`: Lógica de transformación y limpieza en Spark.
* `docs/`: Capturas del Dashboard y diagramas de arquitectura.

---

## 🔧 Configuración Rápida

1.  **Requisitos:** Tener un Workspace en Microsoft Fabric y un Namespace de Azure Event Hubs.
2.  **Librerías:** Instalar `azure-eventhub` en el entorno de Fabric.
3.  **Seguridad:** Las claves de conexión han sido omitidas. Asegúrate de configurar tus variables de entorno para `CONNECTION_STR` y `EVENTHUB_NAME`.

---

## 📊 Dashboard Preview
* **Mapa de Calor:** Densidad de tráfico en espacio aéreo español.
* **Alertas:** Indicadores dinámicos de proximidad (Radio < 20km).
* **Métricas:** Velocidades promedio y altitudes máximas por aerolínea.
