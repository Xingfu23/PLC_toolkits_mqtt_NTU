import snap7
from snap7.util import set_int, set_real, set_bool
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
    parser.add_argument('-c', '--config', default='Control_HMI.yml', help='Path to the YAML config file')
    parser.add_argument('-f', '--force-run', action='store_true', help='Override yaml dry_run setting to execute immediately')
    return parser.parse_args()

def create_client(plc_config):
    client = snap7.client.Client()
    try:
        client.connect(plc_config['ip'], plc_config['rack'], plc_config['slot'])
        return client
    except Exception as e:
        print(f"PLC connection failed: {e}")
        return None

def write_temp_setpoint(client, db_number, offset, value):
    data = bytearray(4) # Real occupies 4 bytes
    set_real(data, 0, value)
    client.db_write(db_number, offset, data)
    print(f"Input temperature: {value}℃")

def write_int_value(client, db_number, offset, value, description):
    data = bytearray(2)
    set_int(data, 0, value)
    client.db_write(db_number, offset, data)
    print(f"Setting {description}: {value}")

def press_start_buttom(client, db_number, byte_offset, bit_index):
    # 1. Reading the status 
    data = client.db_read(db_number, byte_offset, 1)

    # 2. Press the buttom
    set_bool(data, 0, bit_index, True)
    client.db_write(db_number, byte_offset, data)
    print("  -> Press the buttom (Start=True)")

    time.sleep(0.5) # Pressing time

    # 3. Release the buttom
    data = client.db_read(db_number, byte_offset, 1)
    set_bool(data, 0, bit_index, False)
    client.db_write(db_number, byte_offset, data)
    print("  -> Release the buttom (Start=False)")

def main():
    args = parse_args()
    cfg = load_config(args.config)

    plc_cfg = cfg['plc']
    exp_cfg = cfg['experiment']

    # Checking the setting in the config file:
    high_temp_limit = exp_cfg['temp_high_limit']
    low_temp_limit = exp_cfg['temp_low_limit']
    high_temp_target = exp_cfg['temp_high']
    low_temp_target = exp_cfg['temp_low']

    if (high_temp_target > high_temp_limit) or (low_temp_target < low_temp_limit):
        print(f"The target temprature is outside the safty region, please check the coniguration file: {args.config}")
        return
    if (high_temp_target <= low_temp_target):
        print(f"The lower target temperature is higher than high target temperature, please check the coniguration file: {args.config}")
        return

    is_dry_run = cfg['execution']['dry_run']
    if args.force_run:
        is_dry_run = False
        print("⚠️Attention: Using --force-run mode, the procedure will proceed.")
    
    print(f"=== loading configuration file:{args.config} ===")
    print(f"High target temperature: {exp_cfg['temp_high']}℃")
    print(f"Low target temperature: {exp_cfg['temp_low']}℃")
    print(f"Number of Cycles: {exp_cfg['cycles']}")
    print(f"Idle time (warm): {exp_cfg['idle_warm_min']} min")
    print(f"Idle time (cold): {exp_cfg['idle_cold_min']} min")
    print(f"Simulation(dry run)" if is_dry_run else "Proceeding the procedure.")

    client = create_client(plc_cfg)
    if not client or not client.get_connected():
        print("PLC connection failed, abort!")
        return
    try:
        db_num = plc_cfg['db_number']

        write_temp_setpoint(client, db_num, 468, exp_cfg['temp_low']) # Setting low target temperature
        write_temp_setpoint(client, db_num, 472, exp_cfg['temp_high']) # Setting low target temperature
        write_int_value(client, db_num, 548, exp_cfg['cycles'], 'Number of Cycles')
        write_int_value(client, db_num, 536, exp_cfg['idle_cold_min'], 'Idle time (cold)')
        write_int_value(client, db_num, 538, exp_cfg['idle_warm_min'], 'Idle time (warm)')

        print('Loading Complete!')

        # Execution
        if not is_dry_run:
            print("Proceeding thermal cycle")
            time.sleep(3)
            press_start_buttom(client, db_num, 556, 0)
        else: 
            print('Simulation mode:')
            print("Change the yaml to change the status of 'dryrun'")
    
    except Exception as e:
        print(f"Error Occur: {e}")
    finally:
        client.disconnect()
        print('PLC is disconnected.')

if __name__ == "__main__":
    main()