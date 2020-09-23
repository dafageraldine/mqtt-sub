import paho.mqtt.client as mqttclient
import pymysql.cursors
import time

def on_connect(client,userdata,flags,rc):
    if rc == 0:
        print('connect')
        global connected
        connected = True
    else:
        print('not connect')

def on_message(client,userdata,message):
    messagerecv = True
    data_ = str(message.payload.decode("utf-8"))
    # print(data_)
    insertdata(data_)

def insertdata(data):
    ####format=====>voltdc#amperedc#powerdc
    conv =  data.split('#')
    vdc = float(conv[0])
    adc = float(conv[1])
    pdc = float(conv[2])
    vac = float(conv[3])
    aac = float(conv[4])
    pac = float(conv[5])
    # print(vdc)

    try:
        with dbconn.cursor() as cursor:
            sqlQuery = "INSERT INTO dashboard(`volt_dc`,`ampere_dc`,`power_dc`,`volt_ac`,`ampere_ac`,`power_ac`) VALUES (%s,%s,%s,%s,%s,%s)"
            cursor.execute(sqlQuery,(vdc,adc,pdc,vac,aac,pac))
            dbconn.commit()
    finally:
        pass

connected = False
messagerecv = False

######dbconnection
dbconn = pymysql.connect(
    host='localhost',
    user='inno',
    port= 3306,
    password = 'password',
    db = 'roof_inno',
    cursorclass=pymysql.cursors.DictCursor
)

#####broker##address
host = '202.157.186.43'
port = 1883

##mqtt###setup
client = mqttclient.Client("MQTT")
client.on_connect = on_connect
client.on_message = on_message
client.connect(host,port=port)
client.subscribe("datadashboard$Ay90*iXEajax^")
client.loop_start()
# print(client.connect)
while connected != True:
    time.sleep(0.1)
while messagerecv != True:
    time.sleep(0.1)

client.loop_stop()