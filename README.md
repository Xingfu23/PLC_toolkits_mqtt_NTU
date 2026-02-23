# PLC_toolkits_mqtt_NTU
 
It is a toolkit for the PLC control system.
For now, the data flow structure is:  
The data from the sensors is published to the MQTT server (using Eclipse Mosquitto).   
Next, the PostgreSQL database subscribes to the MQTT server and records the data in the database.  
For execute the programme, please execute the script ```plc_to_db.py```.  

TODO LIST:  
- Upload the docker compose file.
- Merge configuration file ```config.ini``` into ```Control_HMI.yml``` and modify the main program based on this.
- Add variables that describe the running status of thermal cycle into database.
