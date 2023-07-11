import time
import subprocess
import urllib.request
import socket


def connect(host='http://google.com'):
    try:
        urllib.request.urlopen(host, timeout=8) # Set timeout to 1 second
        return True
    except urllib.error.URLError:
        return False
    except socket.timeout:
        return False
    
def check_wifi_connection():
    print("Checking WiFi connection")
    output = subprocess.check_output(['iwconfig', 'wlo1']).decode('utf-8')
    return 'ESSID:"' in output

def check_iitd_wifi():
    print("Checking connectivity with IITD_WIFI")
    try:
        output = subprocess.check_output(['iwgetid'], text=True)
        ssid = output.strip().split(':')[1].strip('"')
        return ssid == 'IITD_WIFI'
    except subprocess.CalledProcessError:
        return False  # iwgetid command failed, not connected to any network

def connect_to_iitd_wifi():
    print("Connecting to IITD_WIFI")
    try:
        # Check if Wi-Fi is already disconnected
        if check_wifi_connection():
            subprocess.call(['nmcli', 'd', 'disconnect', 'wlo1'])

        # Turn off Wi-Fi
        subprocess.run(['nmcli', 'radio', 'wifi', 'off'])

        # Sleep for 1 minute
        time.sleep(60)

        # Turn on Wi-Fi
        subprocess.run(['nmcli', 'radio', 'wifi', 'on'])

        result = subprocess.run(['nmcli', 'device', 'wifi', 'connect', 'IITD_WIFI'], capture_output=True)
        # Check if the return code is 0, indicating success
        if result.returncode == 0:
            print("Connected to IITD_WIFI successfully.")
        else:
            print("Failed to connect to IITD_WIFI.")
    except Exception as e:
        print("Exception: " + str(e))
        # Check for Firestore Deadline Exceeded error
        if "firestoreDeadline of" in str(e):
            print("Firestore Deadline Exceeded error occurred. Waiting for 30 seconds before trying again...")
            time.sleep(30)

def check_network():
    connected = False

    while not connected:
        if check_iitd_wifi():
            if connect():
                print("Connected to IITD_WIFI and internet is also found")
                print("Checking WiFi IP")
                devices = subprocess.check_output(['nmcli', 'device', 'show', 'wlo1'])
                devices = devices.decode('ascii')
                devices = devices.split('\n')
                devices = [d for d in devices if 'IP4.ADDRESS' in d]
                if len(devices) > 0:
                    print("IP address found. Connected to IITD_WIFI")
                    connected = True
                else:
                    print("Connected to IITD WiFi but IP not found")
                    print("Disconnecting from IITD WiFi")
                    try:
                        subprocess.call(['nmcli', 'd', 'disconnect', 'wlo1'])
                    except Exception as e:
                        print("Error: " + str(e))

                    # Turn off Wi-Fi
                    print("Turning off Wi-Fi")
                    subprocess.run(['nmcli', 'radio', 'wifi', 'off'])

                    # Sleep for 1 minute
                    time.sleep(30)

                    # Turn on Wi-Fi
                    print("Turning on Wi-Fi")
                    subprocess.run(['nmcli', 'radio', 'wifi', 'on'])

            else:
                print("No internet!")  
                connect_to_iitd_wifi()        

        elif check_wifi_connection():
            print("Connected to other network, disconnecting...")
            try:
                subprocess.call(['nmcli', 'd', 'disconnect', 'wlo1'])
            except Exception as e:
                print("Error: " + str(e))
            connect_to_iitd_wifi()
        else:
            print("Not connected to any network, connecting to IITD_WIFI...")
            connect_to_iitd_wifi()

        if connected:
            return True
        else:
            # Sleep for 1 minute before attempting to connect again
            time.sleep(10)