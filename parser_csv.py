#Be ready to enjoy your python code--UTB
#Created by jinsanity

import struct
import pandas as pd 
import json


def buff_size(str_type :str,):
    if str_type == "unsigned integer4":
        buff , fmt = 4,'I'
    elif str_type == "signed integer4":
        # b signed char integer
        buff , fmt = 4,'i'
    elif str_type == "unsigned integer2":
        # H unsigned short integer
        buff , fmt = 2,'H'
    elif str_type == "unsigned integer1":
        # B signed char integer
        buff , fmt = 1,'B'
    elif str_type == "signed integer1":
        # B signed char integer
        buff , fmt = 1,'b'

    return  buff , fmt

# df = pd.read_csv('source/message_content.csv',)
df_veh = pd.read_excel('doc/config/message_content.xls', sheet_name='4.1 Vehicle Message')
veh_head = df_veh[0:17]
veh_attb = df_veh[18:36]

df_sig = pd.read_excel('doc/config/message_content.xls', sheet_name='4.3 Signal Message')
sig_head = df_sig[0:11]
sig_attb = df_sig[12:18]

df_com = pd.read_excel('doc/config/message_content.xls', sheet_name='4.6 Complete Message')
row = 6
veh_attb.iloc[row,1] + str(int(veh_attb.iloc[row,2]))


binary_file = "input/CapOkland.ts0" 
file_name = binary_file.split('/')[1].split('.')[0]
out_file    = open("output/%s.csv" %file_name,"w")

with open(binary_file, mode='rb') as file: # b is important -> binary
    fileContent = file.read()

## The first 16 bytes  compose the file header
decode = struct.unpack('ccccccccccccccxc', fileContent[0:16])
# print (decode)
decode_ls = [tups.decode("utf-8") for tups in decode] 
print(''.join(decode_ls))


# curB = 16
# ## Vehicle Message [0,6]: unsigned int

head1l = ['link_info', 'Message Name', 'Message Length', 'Simulation Time', 'Request Type', 'Request Handle', 'Class ID', 'Action ID', 'Attribute ID Count', 'Number of Aggregate Classes', 'Class ID', 'Action ID', 'Attribute ID Count', 'Attribute ID', 'Number of Aggregate Classes', 'Instance ID Count', 'Instance ID', 'Instance ID Count']
head1 = ",".join(head1l) + "\n"

curB = 16
dict_all =  {}
obj_list = []
percent = 0
threshold = 5



out_file.write(head1)
while percent < threshold:
    head = struct.unpack("5I", fileContent[curB:curB+20])
    # print (head)
    percent = float(curB/len(fileContent)*100)
    print ("head: %s percent: %.2f " % (head[0],percent) )
    if head[0] == 3001:
        if head[3] == 14000:
            timestep = "TS%s" %head[2]
            dict_all.update({timestep:obj_list})
            link_obj = [{"link_info":[]},{"vehicle":[]}]
            lkinfo,lkveh = ["link_info"],["vehivle"]
            # print (timestep+" Link")
            infoID = 0
            vlinfo= [str(infoID)]
            for index,row in veh_head.iterrows():
                # print (row[0])
                str_type = row[1]+ str(int(row[2]))
                buff , fmt = buff_size(str_type)
                head = struct.unpack(fmt, fileContent[curB:curB+buff])
                # print (head[0])
                # link_obj[0]['link_info'].append({row[0]:head[0]})
                lkinfo.append(row[0])
                vlinfo.append(str(head[0]))
                # head[0]
                curB  = curB+buff
            # print (lkinfo)
            out_file.write(",".join(vlinfo) + "\n")

            for i in range(head[0]):
                veh_num = "veh%s"%i
                veh_obj = { veh_num:[] }
                for index,row in veh_attb.iterrows():
                    # print (row[0])
                    str_type = row[1]+ str(int(row[2]))
                    buff , fmt = buff_size(str_type)
                    veh = struct.unpack(fmt, fileContent[curB:curB+buff])
                    # print (veh[0])
                    veh_obj[veh_num].append({row[0]:veh[0]})
                    curB  = curB+buff
                link_obj[1]['vehicle'].append(veh_obj)
                lkveh.append(row[0])
            dict_all[timestep].append(link_obj)

        elif head[3] == 14200:
            timestep = "TS%s" %head[2]
            print (timestep + " signal")
            dict_all.update({timestep:obj_list})
            sig_obj = [{"sign_info":[]},{"code":[]}]
            for index,row in sig_head.iterrows():
                # print (row[0])
                str_type = row[1]+ str(int(row[2]))
                buff , fmt = buff_size(str_type)
                head = struct.unpack(fmt, fileContent[curB:curB+buff])
                # print (head[0])
                sig_obj[0]['sign_info'].append({row[0]:head[0]})
                curB  = curB+buff
            for i in range(head[0]):
                links_num = "code%s"%i
                links_obj = { links_num:[] }
                for index,row in sig_attb.iterrows():
                    # print (row[0])
                    str_type = row[1]+ str(int(row[2]))
                    buff , fmt = buff_size(str_type)
                    link = struct.unpack(fmt, fileContent[curB:curB+buff])
                    # print (veh[0])
                    links_obj[links_num].append({row[0]:veh[0]})
                    curB  = curB+buff
                sig_obj[1]['code'].append(links_obj)
            dict_all[timestep].append(sig_obj)

    elif head[0] == 3003:
        # print ("message")
        for index,row in df_com.iterrows():
        # print (row[0])
            str_type = row[1]+ str(int(row[2]))
            buff , fmt = buff_size(str_type)
            head = struct.unpack(fmt, fileContent[curB:curB+buff])
            curB  = curB+buff
    else:
        pass
out_file.close()

print ("parsing completed")
