import asyncio
import threading
import datetime
import time
import firebase_admin
import read_json
import conn_network_iit
import os
from queue import Queue
from firebase_admin import credentials
from firebase_admin import firestore
from asyncua import Client
from google.api_core.exceptions import DeadlineExceeded
from pymongo import MongoClient
from pymongo.errors import OperationFailure
from copy import deepcopy
import pymongo


global nodes_dict_lab
global db


def init_mongo():
    try:
        print('inside init mongo')
        client = MongoClient('mongodb://localhost:27017')
        db = client['labautonomy']
        return db
    except Exception as e:
        print(e)

async def get_latest_values_from_plc():
    time.sleep(1)
    print("Getting latest data from the PLC")
    # getting current value from PLC
    for key in nodes_dict_lab:
        if key in admin_room_dict.keys():
            lab_dict['admin_room'][key] = await nodes_dict_lab[key].get_value()
        elif key in cpl_dict.keys():
            lab_dict['cpl'][key] = await nodes_dict_lab[key].get_value()
        elif key in kits_dict.keys():
            lab_dict['kits'][key] = await nodes_dict_lab[key].get_value()
        elif key in machines_dict.keys():
            lab_dict['machines'][key] = await nodes_dict_lab[key].get_value()
        elif key in meeting_room_dict.keys():
            lab_dict['meeting_room'][key] = await nodes_dict_lab[key].get_value()
        elif key in other_dict.keys():
            lab_dict['other'][key] = await nodes_dict_lab[key].get_value()
        elif key in sensors_dict.keys():
            lab_dict['sensors'][key] = await nodes_dict_lab[key].get_value()


def push_updates_to_mongodb():
    global db
    print("push_updates_to_mongodb at", datetime.datetime.now())
    try:
        conn_network_iit.check_network()
        print('in push updates')
        print("MongoDB Object Created")

        print("Uploading to MongoDB")
        for room in lab_dict:
            for switch in lab_dict[room]:
                collection = db['cp_lab']  # Replace with your collection name
                query = {"_id": room}
                update = {"$set": {switch: lab_dict[room][switch]}}
                collection.update_one(query, update, upsert=True)

        print("MongoDB Updated")
    except DeadlineExceeded as ex:
        print("Deadline exceeded error: ", str(ex))
        conn_network_iit.check_network()
        time.sleep(1)
        # push_updates_to_mongodb()  # retry
    except Exception as ex:
        print("exception in push update MongoDB: " + str(ex))
        conn_network_iit.check_network()


async def watch_collection():
    print('inside watch_collection')
    global db
    collection = db['cp_lab']
    pipeline = [{'$match': {'operationType': {'$in': ['insert', 'update', 'delete']}}}]
    with collection.watch(pipeline) as stream:

        for change in stream:
            document_id = change['documentKey']['_id']
            operation_type = change['operationType']
            document = collection.find_one({'_id': document_id})
            if operation_type == 'insert':
                print(document)
            elif operation_type == 'update':
                update_description = change['updateDescription']
                updated_fields = update_description['updatedFields']
                document_copy = deepcopy(document)

                for field, value in updated_fields.items():
                    document_copy[field] = value
                   
                document_copy.pop('_id', None)
                print(document_copy)
                plc_queue.put(document_copy)

            elif operation_type == 'delete':
                print(f"Document with _id {document_id} has been deleted.")


async def plc_execute():
    await asyncio.sleep(10)
    print("in plc execute")
    while True:
        try:
            await asyncio.sleep(0.01)
            if not plc_queue.empty():
                doc_dir = plc_queue.get()
                print(doc_dir)
                for key in doc_dir:
                    await nodes_dict_lab[key].set_value(doc_dir[key])
        except Exception as e:
            print("An error occurred in plc_execute:", str(e))

async def main():
    global nodes_dict_lab
    global db

    case = 0
    while True:
        if case == 1:
            conn_network_iit.check_network()
            try:
                print("\nConnecting with LAB PLC")
                await client_lab.connect()
                print("LAB PLC Connected!")
                case = 2
            except Exception as e:
                print("Connection error! Exception:" + str(e))
                case = 1
                await asyncio.sleep(2)
        elif case == 2:
            print("Case 2 Run")
            try:
                nodes_dict_lab = read_json.get_lab_nodes(client_lab)
                case = 5
            except Exception as e:
                print("Exception: " + str(e))
                case = 2
                await asyncio.sleep(2)
        elif case == 5:
            print("Case 5")
            try:
                await get_latest_values_from_plc()
                case = 6
            except Exception as e5:
                print("Case 5: " + str(e5))
                case = 4
        elif case == 6:
            print("Case 6")
            try:
                service_level = await nodes_dict_lab['State'].get_value()
                if service_level == 0:
                    push_updates_to_mongodb()
                    case = 3
                else:
                    print("PLC Check node off")
                    case = 4
            except Exception as e6:
                print("Case 5: " + str(e6))
                case = 4
        elif case == 3:
            print("in case 3")
            try:
                service_level = await nodes_dict_lab['State'].get_value()
                print("Service Level PLC: " + str(service_level))
                if service_level==0:
                    collection = db['code_tracking']  # Replace with your collection name
                    query = {"_id": "python"}
                    update = {"$set": {"last_update": datetime.datetime.now()}}
                    collection.update_one(query, update, upsert=True)

                    print("Code status pushed to MongoDB at", datetime.datetime.now())

                    conn_network_iit.check_network()
                    case = 3
                    await asyncio.sleep(15)
                else:
                    print("in case 3 else")
                    case = 4
                await asyncio.sleep(2)
            except Exception as e:
                print("LAB PLC is Off!! Exception: " + str(e))
                case=4
        elif case == 4:
            print("in case 4")
            print("Disconnecting...")
            try:
                await client_lab.disconnect()
            except Exception as e:
                print("Disconnection Error! Exception: " + str(e))
            case = 0
        else:
            # wait
            case = 1
            await asyncio.sleep(2)



def watch_collection_thread():
    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(watch_collection())
    loop.close()

if __name__ == '__main__':

    admin_room_dict = {'AC_A': True, 'Fan_A': True, 'Lights_A': True, 'Main_Door': True}
    cpl_dict = {'AC1_W': True, 'AC2_W': True, 'Fan1_W': True, 'Fan2_W': True, 'Fan3_W': True, 'Fan4_W': True,
                'Lights1_W': True, 'Lights2_W': True}
    kits_dict = {'Kit1': True, 'Kit2': True, 'Kit3': True, 'Kit4': True, 'Kit5': True, 'Kit6': True, 'Kit7': True,
                 'Kit8': True, 'T1_A': True, 'T2_A': True}
    machines_dict = {'MPRC_W': True, 'Station1_W': True, 'Station2_W': True, 'Station3_W': True, 'printer_W': True}
    meeting_room_dict = {'AC': True, 'Fan': True, 'Light': True}
    other_dict = {'Compressor_I': False}
    sensors_dict = {'IITD_Mains': True}

    lab_dict = {
        'admin_room': admin_room_dict,
        'cpl': cpl_dict,
        'kits': kits_dict,
        'machines': machines_dict,
        'meeting_room': meeting_room_dict,
        'other': other_dict,
        'sensors': sensors_dict,
    }

    nodes_dict_lab = {}
    update_firestore_flag = False
    plc_queue = Queue(maxsize=0)

    if conn_network_iit.check_network():
        client_lab = Client("opc.tcp://10.226.52.207:4840")
    else:
        print('Network Issue')

   
    db = init_mongo() # db name : lab

    watch_thread = threading.Thread(target=watch_collection_thread)
    watch_thread.start()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(main(), plc_execute()))
    loop.close()

    # Wait for the watch_thread to finish
    watch_thread.join()
