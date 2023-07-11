from pymongo import MongoClient

ip = '127.0.0.1'
port = 27017

client = MongoClient(f'mongodb://{ip}:{port}')
db = client.get_database('labautonomy')
collection = db.get_collection('R_cp_lab')

documents = [
    {
        '_id': 'admin_room',
        'AC_A': False,
        'Fan_A': False,
        'Lights_A': False,
        'Main_Door': False,
        'normal_temp':False,
        'temperature_A':29
    },

    {
        '_id': 'cpl',
        'AC1_W': False,
        'AC2_W': False,
        'Fan1_W': False,
        'Fan2_W': False,
        'Fan3_W': False,
        'Fan4_W': False,
        'Lights1_W': False,
        'Lights2_W': False,

    },

    {
        '_id': 'kits',
        'Kit1': False,
        'Kit2': False,
        'Kit3': False,
        'Kit4': False,
        'Kit5': False,
        'Kit6': False,
        'Kit7': False,
        'Kit8': False,
        'T1_A': False,
        'T2_A': False

    },

    {
        '_id': 'machines',
        'MPRC_W': False,
        'Station1_W': False,
        'Station2_W': False,
        'Station3_W': False,
        'printer_W': False,

    },

    {
        '_id': 'meeting_room',
        'AC': False,
        'Fan': False,
        'Light': False,
    },

    {
        '_id': 'other',
        'Compressor_I': False,

    }

    

]

if __name__ == '__main__':
    collection.insert_many(documents)

    # pipeline = [{'$match': {'operationType': 'update'}}]
    # with collection.watch(pipeline) as stream:
    #     for update_change in stream:
    #         print('------ : ',update_change)



