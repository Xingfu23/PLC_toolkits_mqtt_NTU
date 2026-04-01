# PLC_toolkits_mqtt

It is a toolkit for the thermal-chamber subsystem of the Multi-Module Test System (MMTS) for the high-granularity calorimeter (HGCAL) at NTU. The main functions of this toolkit include:

1. **Automated Data Pipeline:** Establishes a reliable data stream from PLC-connected sensors to a local PostgreSQL database via MQTT, enabling real-time telemetry and long-term storage.

2. **Remote Thermal Control:** Provides a PC-based interface to control the chiller and thermal chamber subsystems via PLC, offering a flexible alternative to traditional HMI operations.

3. **Modular Architecture:**: Features a dedicated I/O abstraction layer to ensure stable and reusable communication with Siemens S7-1200 PLCs.

---
## Environment Setup
### 1. **Hardware Configuration**
The system is built on a **Siemens SIMATIC S7-1200 (CPU 1214C)** platform with the following module sequence:
- **CPU**: SIMATIC S7-1200 CPU 1214C.
- **Analogue Input Module (SM 1231):** Equipped with 8 **PT1000** Resistance Temperature Detectors (RTDs) for high-precision thermal monitoring.
- **Analogue I/O Module (SM 1234):** Connected to the right of SM 1231, supporting 2 **Vaisala DMT143** dew point sensors for humidity and condensation control. 

### 2. **Software Prerequisites**
This toolkit manages dependencies using **[uv](https://docs.astral.sh/uv/#highlights)** but remains compatible with other virtual environment frameworks, such as Miniconda.
- **Python 3.11+:** Recommended environment.
- **Docker & Docker Compose:** Required for deploying the data infrastructure.

### 3. **Environment Setup**
**Python Dependencies:**  
To install the necessary Python packages (including snap7, psycopg2, and paho-mqtt), run:
```
# Using uv (Recommended)
uv sync

# Using pip with requirements.txt
pip install -r requirements.txt
```
**Infrastructure Deployment (Docker):**
The project uses a containerised stack to handle the data pipeline, including PostgreSQL for storage, Eclipse Mosquitto for MQTT messaging, and Grafana for visualisation.
To launch the entire backend infrastructure, navigate to the project root and execute:
```
docker-compose up -d
```
This command will automatically initialise the database schema and start all required services in the background.  
Check out `docker-compose.yml` for the details

---

## Database Setup (Manual Initialisation).
Currently, the PostgreSQL database must be initialised manually before running the data collection scripts. The schema definition and default sensor configurations are located in `local_database/sql/init_db.sql`.

### 1. Execute the Initialisation Script. 
Ensure you have a PostgreSQL instance running (e.g., via your local installation or a standalone Docker container) and an empty database created (e.g., `plc_collect`). Run the following command in your terminal:  
```
bash psql -U your_username -d your_database_name -f local_database/sql/init_db.sql
```
### 2. Database Schema Overview
The database consists of two main tables:
- **Sensors:** Stores metadata about each sensor, including its name, type, and PLC address.
- **Fields:** id (Auto-increment), sensor_id (Foreign Key), metric (e.g., 'temperature_C', 'system_C'), value (Numeric), measured_at (Timestamp).
- **Indexes:** Optimised with composite indexes on (sensor_id, measured_at) for lightning-fast Grafana queries.

### Data Pipline(`plc_to_db.py`)
The core of the telemetry system is handled by plc_to_db.py. It establishes a robust, asynchronous data pipeline from the physical sensors to the local database.

**How the Data Flows**
The telemetry system follows a decoupled architecture to ensure high reliability and precise sampling intervals. The process is divided into four main stages:

1. **Data Acquisition:** Scheduled jobs routinely poll the Siemens S7-1200 PLC via the python-snap7 library. The system reads specific memory offsets for PT1000 RTDs, Vaisala DMT143 sensors, and Chiller operating statuses.

2. **PLC Communication Logic:** To ensure data integrity during concurrent operations, all PLC read/write actions are wrapped in a thread lock (plc_lock). The following core functions handle different data types:
    - `read_sensor_real(offset)`: Reads a 4-byte Floating Point (Real) value from a specific Data Block (DB). Used for analogue measurements like temperature and dew point.
    - `read_sensor_bool(byte_offset, bit_index)`: Accesses a single bit within a Data Block, typically used for system status flags.
    - `read_m_bool(byte_offset, bit_index)`: Directly reads from the PLC's internal memory area (Bit Memory / Markers) to monitor logic states not stored in DBs.
    
        Configuration & Offsets: Specific memory addresses and variable mappings are documented in the /PLC_CPU_info directory. Refer to these files to align the script with your TIA Portal project settings.

3. **Payload Packaging & MQTT Publishing:** Raw values (including boolean states converted to integers) are packaged into a comprehensive JSON payload and published to an MQTT Broker (Eclipse Mosquitto).

    Decoupling: This stage separates the time-critical data-reading process from the potentially slower database-writing process, preventing database latency from affecting PLC sampling precision.

4. **Multi-Threaded Consumption (PostgreSQL):** The script subscribes to its own MQTT topic to ingest the data:  
    - Message Queue: Incoming payloads are placed into a thread-safe queue.
    - Worker Pool: A pool of dedicated worker threads (utilizing psycopg2 connection pooling) continuously pulls from the queue.
    - Execution: Workers efficiently execute INSERT statements into the measurement table, ensuring high-throughput data logging.

### Execution
Once your database is initialised and the environment is ready, you can initiate the telemetry pipeline.  
Using uv (Recommended):
```
uv run plc_to_db.py
```
Using Standard Python/Conda:
```
python plc_to_db.py
```
---
## Chiller Control System (`control_hmi.py`)
This script serves as a **PC-based HMI alternative**, allowing users to monitor and control the **Julabo Chiller** thermal cycles directly from the terminal via the Siemens S7-1200 PLC.  
### Hardware Requirements
Before execution, ensure the **Julabo Analogue Module** is properly interfaced with the PLC's Analogue I/O Module (**SM 1234**). The control logic relies on this physical voltage/current loop to regulate temperature.
### Configuration (.yml)
The control parameters are defined in YAML files (e.g., HMI_Control.yml). You can customise the following settings within the file:  
- **Targt Temperature (°C):** High(`temp_high`) and low(`temp_low`) setpoints for the thermal cycle.  
- **Soak Time:** Duration to maintain the temperature at each stage.
- **Cycle Counts:** Total number of thermal repetitions.
### System Status Codes
The script uses the same status coding system as the telemetry pipeline (`plc_to_db.py`). Running the script without arguments will display the current operational state:

| Code | Status | Description |
| :--- | :--- | :--- |
| 0 | System Alert | The chamber door is open or a safety interlock is triggered. Check hardware. |
| 1 | Standby | System is idle and ready for commands. |
| 2 | Countdown (Warming) | Waiting for the programmed delay before the warming stage. |
| 3 | Warming Up | Chiller is actively increasing the temperature. |
| 4 | Countdown (Cooling) | Waiting for the programmed delay before the cooling stage. |
| 5 | Cooling Down | Chiller is actively decreasing the temperature. |

### Usage & Arguments
You can control the chiller's behaviour using the following command-line flags:  
**1. Check Status (Default)**  
Displays the current system state without taking any action.
```
uv run control_hmi.py
```
**2. Start Thermal Cycle (`-f`)**  
The `-f` (Force Start) flag is required to initiate movement. The script will attempt to send the start command up to 10 times. If the chiller fails to respond after 10 attempts, it usually indicates a hardware issue (e.g., the chamber door is not properly locked).
```
# Execute thermal cycles using a specific configuration
uv run control_hmi.py -c HMI_Control.yml -f
```
**3. Stop Operations (`-s`)**  
Immediately sends a **stop** signal to the PLC to terminate the current chiller activity.
```
uv run control_hmi.py -s
```
**4. Custom Configuration (`-c`)**  
Specify a different YAML file for specific testing scenarios.
```
uv run control_hmi.py -c HMI_Control_test.yml -f
```

Again, the examples above use `uv run` for automatic dependency management. However, if you are using a standard Python environment (such as Miniconda, venv, or a global install), replace `uv run` with `python`:  
-With uv: `uv run control_hmi.py -c HMI_Control.yml -f`  
-With Python: `python control_hmi.py -c HMI_Control.yml -f`