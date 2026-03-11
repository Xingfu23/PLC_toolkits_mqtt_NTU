import snap7
from snap7.util import set_int, set_real, set_bool, get_bool
import time
import argparse
import yaml
import os
import sys

# Loading yaml configuration file
def load_config(config_path:str)->None:
    if not os.path.exists(config_path):
        print(f"Error, there is no configuration file: '{config_path}'")
        sys.exit(1)
    
    with open(config_path, 'r', encoding='utf-8') as f:
        try:
            return yaml.safe_load(f)
        except yaml.YAMLError as exc:
            print(f"Error: {exc}")
            sys.exit(1)

def parse_args():
    parser = argparse.ArgumentParser(description='PLC Thermal Cycle Controller')
    parser.add_argument('-c', '--config', default='HMI_Control.yml', help='Path to the YAML config file')
    parser.add_argument('-f', '--force-run', action='store_true', help='Override yaml dry_run setting to execute immediately')
    parser.add_argument('-s', '--stop', action='store_true', help='Send STOP signal to the PLC immediately')
    return parser.parse_args()

def create_client(plc_config):
    client = snap7.client.Client()
    try:
        client.connect(plc_config['ip'], plc_config['rack'], plc_config['slot'])
        return client
    except Exception as e:
        print(f"PLC connection failed: {e}")
        return None

def read_sensor_real(client, db_number, offset):
    data = client.db_read(db_number, offset, 4) # real occupied 4 bytes
    return snap7.util.get_real(data, 0)

def write_temp_setpoint(client, db_number, offset, value):
    data = bytearray(4) 
    set_real(data, 0, value)
    client.db_write(db_number, offset, data)

def write_int_value(client, db_number, offset, value):
    data = bytearray(2)
    set_int(data, 0, value)
    client.db_write(db_number, offset, data)

def press_hmi_button(client, db_number, byte_offset, bit_index, button_name):
    # 1. Reading current status
    data = client.db_read(db_number, byte_offset, 1)

    # 2. Press the button 
    set_bool(data, 0, bit_index, True)
    client.db_write(db_number, byte_offset, data)
    print(f"  -> Press {button_name} button")

    time.sleep(0.3) # Simulate pressing button

    # 3. Release the button
    data = client.db_read(db_number, byte_offset, 1)
    current_bit_state = get_bool(data, 0, bit_index)
    if current_bit_state:
        set_bool(data, 0, bit_index, False)
        client.db_write(db_number, byte_offset, data)
        print(f"  -> Release {button_name} button")
    else:
        pass 

def smart_start(client, db_number):
    print("\n[System Check & Start Sequence]")
    
    # 0. Actulalize the setting value (by configuration file)
    press_hmi_button(client, db_number, 557, 5, "ACT_DATA (Actualize Data)")
    time.sleep(0.5)

    # 1. Reset the Alarm
    press_hmi_button(client, db_number, 556, 5, "ALM_RES")
    time.sleep(0.5)
    
    # 2. Make sure the PLC is in the AUTO mode
    data = client.db_read(db_number, 556, 1)
    if not get_bool(data, 0, 1):
        print("  -> System not in AUTO, setting AUTO mode...")
        press_hmi_button(client, db_number, 556, 1, "AUTO")
        time.sleep(0.5)

    # 3. Sending start signal
    press_hmi_button(client, db_number, 556, 0, "START")
    print("  => Start sequence completed.\n")

def main():
    args = parse_args()
    cfg = load_config(args.config)
    
    plc_cfg = cfg['plc']
    exp_cfg = cfg['experiment']
    db_num = plc_cfg['db_number']

    high_temp_limit = exp_cfg['temp_high_limit']
    low_temp_limit = exp_cfg['temp_low_limit']
    high_temp_target = exp_cfg['temp_high']
    low_temp_target = exp_cfg['temp_low']

    if (high_temp_target > high_temp_limit) or (low_temp_target < low_temp_limit):
        print(f"The target temprature is outside the safty region...")
        return
    if (high_temp_target <= low_temp_target):
        print(f"The lower target temperature is higher than high target temperature...")
        return

    is_dry_run = cfg['execution']['dry_run']
    if args.force_run:
        is_dry_run = False
    
    client = create_client(plc_cfg)
    if not client or not client.get_connected():
        print("PLC connection failed, abort!")
        return
    
    try:
        max_retries = 10
        retry_count = 0
        is_stop = False
        is_success = False

        if args.stop:
            print("Sending STOP signal to PLC...")

            # Execution
            while retry_count <= max_retries and not is_stop:
                print(f"=== Sending STOP command, no of retry:{retry_count}===\n")
                press_hmi_button(client, db_num, 556, 2, "STOP")
                current_target = read_sensor_real(client, db_num, 418)
                if current_target == 20.0: 
                    print(f"current_target: {current_target}")
                    is_stop = True
                    return
                else:
                    print("System do not stop, we will try again later.")
                    retry_count += 1
                    time.sleep(1.0)
            
            if not is_stop:
                print("Reach retry number limit, system can not be stopped. Please stop the system manually.")

        while retry_count <= max_retries and not is_success:
            print(f"=== Sending command, no of retry:{retry_count}===\n")

            # Writing.
            write_temp_setpoint(client, db_num, 468, exp_cfg['temp_low'])
            write_temp_setpoint(client, db_num, 472, exp_cfg['temp_high'])
            write_int_value(client, db_num, 548, exp_cfg['cycles'])
            write_int_value(client, db_num, 536, exp_cfg['idle_cold_min'])
            write_int_value(client, db_num, 538, exp_cfg['idle_warm_min'])

            # Execution
            if not is_dry_run:
                print("Proceeding thermal cycle...")
                smart_start(client, db_num)
                time.sleep(2.0)
                current_target = read_sensor_real(client, db_num, 418)
                print(f"current_target: {current_target}")
                print(f"low_temp_limit: {low_temp_target}")
                if abs(current_target) - abs(low_temp_target) < 0.01:
                    print("New setting is loaded.")
                    is_success = True
                else:
                    print("Setting is not loaded in, we will try again later.")
                    retry_count += 1
                    time.sleep(1.0)
            else:
                print('Simulation mode: Check bypassed.')
                is_success = True
        if not is_success:
            print("Reach maximum retry number, please check the PLC status.")
    except Exception as e:
        print(f"Error Occur: {e}")
    finally:
        client.disconnect()
        print('PLC is disconnected.')

if __name__ == "__main__":
    main()