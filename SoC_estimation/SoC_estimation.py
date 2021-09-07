from kubernetes import client, config
from kubernetes.client import configuration
from multiprocessing import Process, Manager
import socket
import serial
import binascii
import time
import datetime
import csv
import os

initial_message = binascii.unhexlify('f0')  # 'f0' is the starting message for making UM24C transmit its measurements
sampling_frequency = 5 # in seconds

scheduler_ip = ""
own_ip = ""

def get_temp():
    temp = os.popen("vcgencmd measure_temp").readline()
    return temp.replace("temp=", "")

def get_clock():
    clock = os.popen("vcgencmd measure_clock arm").readline()
    return clock.replace("frequency(45)=", "")

def write_to_file(initial_message, sampling_frequency, initial_state_of_charge, socket_device):

    # reader1 = csv.reader(ser, delimiter=',')
    # states1 = []
    # for row in reader1:
    #     states1.append(row)
    # i = 1
    first_entry = True
    updated_state_of_charge = 0

    while True:
        try:
            ser.write(initial_message)
            returned_data = ser.read(130)  # Size of the response is 130 bytes

            returned_data = returned_data.hex()  # Conversion of the response from bytes to hex
            hex_returned_data = []
            hex_returned_data = returned_data  # Creating a list for easy slicing

            voltage = int(hex_returned_data[4:8],
                              16) / 100  # position of voltage measurements in the returned stream is [4:8],
                # then converting to decimal and dividing by 100 for actual value
       
            current = int(hex_returned_data[8:12],
                          16) / 1000  # position of current measurements in the returned stream
            # is [8:12], then converting to decimal and dividing with 1000 for actual value

            env_temperature = int(hex_returned_data[20:24],
                                      16)  # position of temperature measurement in celsius in the
            #  returned stream is [20:24], then converting to decimal

            ampere_hours = int(hex_returned_data[204:212],
                               16)  # position of ampere-hours measurement in mAh in the
            #  returned stream is [172:180], then converting to decimal

            watt_hours = int(hex_returned_data[212:220], 16)  # position of watt-hours measurement in mWh in the
            #  returned stream is [180:188], then converting to decimal

            # Initially convert amperes to ampere-hours
            ah = current * (sampling_frequency / 3600)

            # For calculating SOC, the documented efficiency is taken into account
            # Initial capacity is 10Ah. The documented efficiency is 0.2 for a new battery
            # Thus the division should be done by Qrated = 10 * 0.2 = 2
            if first_entry:
                updated_state_of_charge = initial_state_of_charge - ((ah / 2) * 100)
                first_entry = False
            else:
                updated_state_of_charge = updated_state_of_charge - ((ah / 2) * 100)

            #updated_state_of_charge = states1[i][-1]

            # Generating the timestamp
            timestamp = time.time()
            timestamp_formatted = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

            # Getting the core frequency
            #core_freq = str(get_clock()).strip('\n')

            # Getting the core temperature
            #core_temp = str(get_temp()).strip('\n')

            # Writing to file
            writer.writerow({'Timestamp': str(timestamp_formatted),
                              'Voltage_(Volts)': str(voltage),
                              'Current_(Ampere)': str(current),
                              'Environment_Temperature_(Celsius)': str(env_temperature),
                              'Ampere-Hours_(mAh)': str(ampere_hours),
                              'Watt-Hours_(mWh)': str(watt_hours),
                              'SOC_(%)': str(round(updated_state_of_charge, 3))})

            #writer.writerow({'Timestamp': str(timestamp_formatted),
            #                 'SOC_(%)': str(updated_state_of_charge)})

            socket_device.send(str(updated_state_of_charge).encode())

            # Updating cache if significant difference in SOC is observed

            print(voltage, current, updated_state_of_charge)#, core_freq, core_temp)
            #print(updated_state_of_charge)
            #i += 1
            time.sleep(sampling_frequency)

        except KeyboardInterrupt:
            print('\n')
            print("Exiting...")
            csvfile.close()
            ser.close()
            socket_device.close()
            exit()
            print('\n')
        except ValueError:
            print('\n')
            print('Corrupted data transmitted, skipping writing...')
            continue

    ser.close()

    # Opening of the serial port
with serial.Serial(port='/dev/rfcomm1', baudrate=9600, bytesize=serial.EIGHTBITS, timeout=1, parity=serial.PARITY_NONE,
                   stopbits=serial.STOPBITS_ONE, rtscts=True, dsrdtr=True) as ser:
#with open('Electric_measurements1.csv', 'r+', buffering=1) as ser:

    aToken = "eyJhbGciOiJSUzI1NiIsImtpZCI6IjBGTzVLTS1qdHo4Y2ljUng0NTdzMWhFT2g5RktDeGllZGo2d2ZacFJiQ1UifQ.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJkZWZhdWx0Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZWNyZXQubmFtZSI6ImRlZmF1bHQtdG9rZW4tcTI2dzciLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC5uYW1lIjoiZGVmYXVsdCIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VydmljZS1hY2NvdW50LnVpZCI6IjA5YTY5OWQwLWYyZWEtNDk5OS1iYmE4LTU1OWRkYmNhYmNlMSIsInN1YiI6InN5c3RlbTpzZXJ2aWNlYWNjb3VudDpkZWZhdWx0OmRlZmF1bHQifQ.RKKZg_U_Lh1J8MKXjFBUF0MeeaKZyEwRnaA51lrSkE8qx3HwK0bKjwBrNrKUNTz8W9JrelCXHEkrlnBZOKG8_jMnIWGFVEWLfEq6W12Chq8b3seJqmdSKsh4BdrInwYqMizk9sjCrAlftB95Auz71uXWqCpPUfpu25nLLoWQjC5TO9PD-yw4rlzjQJ2g0C7ipyXgYkWrxRW7VqwikKK-Kj_he1t5XfMvRr5xiDkAO5bom8644bstEHK16Afq8E4P89NNCyHqnoFKoAh_0-cSp9vMJqRK9JckRtNoRljTGZX3gmz7R5R9ONLUUI0yC7snjv2eL5oVRZ13S56tVmDZ4g"

    aConfiguration = client.Configuration()

    aConfiguration.host = "https://172.16.2.230:6443"   #IP address of Master node

    aConfiguration.verify_ssl = False

    aConfiguration.api_key = {"authorization": "Bearer " + aToken}

    aApiClient = client.ApiClient(aConfiguration)

    hostname = socket.gethostname()
    own_ip = socket.gethostbyname(hostname)

    print("Hostname: " + hostname)
    print("IP: " + own_ip)

    v1 = client.CoreV1Api(aApiClient)
    print("Listing pods with their IPs:")
    ret = v1.list_pod_for_all_namespaces(watch=False)
    for item in ret.items:
        if str(item.metadata.name).startswith('my-scheduler'):
            scheduler_ip = item.status.pod_ip
            print(
                "%s\t%s\t%s" %
                (scheduler_ip,
                item.metadata.namespace,
                item.metadata.name))

    if own_ip != scheduler_ip:
        slave_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        #connect to the master
        while True:
            try:
                slave_client.connect((scheduler_ip, 8080))
            except socket.error as msg:
                print("Error while connecting to Master node")
                time.sleep(5)
                continue
            break

    while True:

        # Checking for previously stored SOC of the battery
        answer = input("Is the battery fully charged? [y/n]: ")

        fieldnames = ['Timestamp',
                       'Voltage_(Volts)',
                       'Current_(Ampere)',
                       'Environment_Temperature_(Celsius)',
                       'Ampere-Hours_(mAh)',
                       'Watt-Hours_(mWh)',
                       'SOC_(%)']

        # fieldnames = ['Timestamp',
        #               'SOC_(%)']

        if answer == "yes" or answer == "y" or answer == "Y":
            with open('Electric measurements.csv', 'w', buffering=1) as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                state_of_charge = 100
                write_to_file(initial_message, sampling_frequency, state_of_charge, slave_client)
            break
        elif answer == "no" or answer == "n" or answer == "N":
            with open('Electric measurements.csv', 'r+', buffering=1) as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                reader = csv.reader(csvfile, delimiter=',')
                states = []
                for row in reader:
                    states.append(row)

                state_of_charge = states[-1][-1]
                print("Previous SOC: %s" % str(state_of_charge))
                write_to_file(initial_message, sampling_frequency, float(state_of_charge), slave_client)
            break
        else:
            print('Wrong input, try again')
