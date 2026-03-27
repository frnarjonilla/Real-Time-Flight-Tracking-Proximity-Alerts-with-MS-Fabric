# ✈️ Real-Time Flight Radar & Proximity Alarms
### End-to-End Data Engineering Project with Microsoft Fabric & Power BI

Este proyecto implementa una arquitectura de datos en tiempo real (Streaming) y procesamiento por lotes (Batch) para monitorear el tráfico aéreo utilizando la API de OpenSky, Azure Event Hubs y Microsoft Fabric.

---

## 🏗️ Arquitectura del Proyecto

El flujo de datos se divide en dos capas principales (Arquitectura Lambda):

1.  **Ingestión (Streaming):** Un script de Python extrae datos de OpenSky y los envía a **Azure Event Hubs**.
2.  **Procesamiento (Fabric):** Un Notebook en Microsoft Fabric procesa los mensajes y:
    * Alimenta un reporte de **Power BI** en tiempo real.
    * Agrupa los datos y los guarda en un **Lakehouse** en formato **Parquet** cada 5 minutos para análisis histórico.
3.  **Visualización:** Dashboard en Power BI con alertas de proximidad y KPIs de tráfico.



---

## 🛠️ Tecnologías Utilizadas

* **Lenguaje:** Python 3.10 (PySpark)
* **Ingestión:** Azure Event Hubs
* **Plataforma de Datos:** Microsoft Fabric (Lakehouse)
* **Almacenamiento:** Apache Parquet / Delta Table
* **Visualización:** Power BI (DirectQuery & Import Mode)

---

## 🚀 Desafíos Técnicos Resueltos

### 1. Optimización de Almacenamiento (Small Files Problem)
Para evitar la creación de miles de archivos pequeños que ralentizan las consultas, implementé un sistema de **micro-batching** en Python que acumula los datos en memoria y realiza una escritura sólida en Parquet cada 300 segundos.

### 2. Consistencia de Tipos (Schema Enforcement)
Gestioné errores de mezcla de tipos (`DoubleType` vs `LongType`) mediante el casting explícito de datos en el proceso de ingesta, asegurando que columnas críticas como `altitud` y `velocidad` mantengan un esquema consistente en el Lakehouse.

---

## 📊 Dashboard Preview

* **Mapa en vivo:** Trayectorias de vuelo actualizadas por segundo.
* **Alertas:** Indicadores visuales cuando un avión entra en el radio de 20km.
* **Histórico:** Análisis de altitudes promedio y densidad de tráfico por franja horaria.

---

## 🔧 Configuración

1.  Clonar el repositorio.
2.  Configurar un **Environment** en Fabric con la librería `azure-eventhub`.
3.  Añadir tus credenciales en el script de Python (ver sección de seguridad).
4.  Ejecutar el Notebook en Microsoft Fabric.

---

## 📄 Licencia
Este proyecto es de código abierto bajo la licencia MIT.
