import socket
import os
from turtle import position
import time 
import random
import shutil

path = 'database/serverdata1/'
backup_path = 'backup/serverdata1/'

SERVER_ADDRESS = ('localhost', 8881)
DATA_DIR = path

SERVER_ADDRESSES = {
    '1': ('localhost', 8881),
    '2': ('localhost', 8882),
    '3': ('localhost', 8883)
    # '4': ('localhost', 8885),
    # '5': ('localhost', 8886),
    # '6': ('localhost', 8887)
}

db_SERVER_ADDRESS = ('localhost', 9090)
backup_db_SERVER_ADDRESS = ('localhost', 9092)
active_server = ('localhost', 8890)

backup_active_server = ('localhost', 8892)

def copy_files(source_folder, destination_folder):
    try:
        files1 = os.listdir(destination_folder)
        for file_name in files1:
            file_path = os.path.join(destination_folder, file_name)
            os.remove(file_path)
        # Create the destination folder if it doesn't exist
        if not os.path.exists(destination_folder):
            os.makedirs(destination_folder)

        # Get a list of files in the source folder
        files = os.listdir(source_folder)

        # Copy each file to the destination folder
        for file_name in files:
            source_path = os.path.join(source_folder, file_name)
            destination_path = os.path.join(destination_folder, file_name)

            # Perform the copy
            shutil.copy2(source_path, destination_path)
            
        print("Files copied in backup successfully!")
    except Exception as e:
        print(f"An error occurred: {e}")

def copy_and_delete_files(source_folder, destination_folder):
    try:
        # Create the destination folder if it doesn't exist
        if not os.path.exists(destination_folder):
            os.makedirs(destination_folder)

        # Get a list of files in the source folder
        files = os.listdir(source_folder)

        # Copy only files that are not already in the destination folder
        for file_name in files:
            source_path = os.path.join(source_folder, file_name)
            destination_path = os.path.join(destination_folder, file_name)

            # Check if the file already exists in the destination folder
            if not os.path.exists(destination_path):
                # Perform the copy
                shutil.copy2(source_path, destination_path)
                print(f"File '{file_name}' copied to destination.")

        # Delete the files in the source folder
        for file_name in files:
            file_path = os.path.join(source_folder, file_name)
            os.remove(file_path)

        print("Files copied to server and backup files deleted successfully!")
    except Exception as e:
        print(f"An error occurred: {e}")

# Function to replicate a file to other servers
def replicate_file(file_name, file_data, target_servers):
    print("executing replicate_file function")
    for server in target_servers:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(server)
                request = f"REPLICATE\n{file_name}\n{file_data}"
                s.send(request.encode())
        except ConnectionError:
            print(f"Failed to connect to server {server}")

# Function to handle incoming replication requests
def handle_replication_request(file_name, file_data):
    print("executing handle_replication_request function")
    file_path = os.path.join(DATA_DIR, file_name)
    with open(file_path, 'a') as file:
        file.write(file_data)

def handle_OriginalWrite_request(filename,data):
    print("executing handle_OriginalWrite_request function")
    with open(os.path.join(DATA_DIR, filename), 'a') as file:
        file.write(data)

def handle_OriginalDelete_request(filename):
    print("executing handle_OriginalDelete_request function")
    file_path = os.path.join(DATA_DIR, filename)
    if os.path.exists(file_path):
        os.remove(file_path)
    delete_db_socket = create_socket()
    db_server_delete_send(delete_db_socket,filename,'w')

def handle_replication_delete(filename):
    print("executing handle_replication_delete function")
    file_path = os.path.join(DATA_DIR, filename)
    if os.path.exists(file_path):
        os.remove(file_path)
    delete_db_socket = create_socket()
    db_server_delete_send(delete_db_socket,filename,'r')

def create_socket():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    return s

def db_server_create_send(db_socket,filename,primary,address):

    msg = "ADD" + "|" + filename + "|" + address[0] + "|" + str(address[1]) + "|"+ primary
    try:
        db_socket.connect(db_SERVER_ADDRESS)
        db_socket.send(msg.encode())
        response=db_socket.recv(1024)
        print(response.decode())
        db_socket.close()
    except ConnectionError:
        db_socket.connect(backup_db_SERVER_ADDRESS)
        db_socket.send(msg.encode())
        response=db_socket.recv(1024)
        print(response.decode())
        db_socket.close()

def db_server_delete_send(db_socket,filename,copy):
    msg = "DELETE" + "|" + filename + "|" + copy
    try:
        db_socket.connect(db_SERVER_ADDRESS)
        db_socket.send(msg.encode())
        response=db_socket.recv(1024)
        print(response.decode())
        db_socket.close()
    except ConnectionError:
        db_socket.connect(backup_db_SERVER_ADDRESS)
        db_socket.send(msg.encode())
        response=db_socket.recv(1024)
        print(response.decode())
        db_socket.close()

def db_server_search_send(search_socket,filename,purpose):
    msg = "SEARCH" + "|" + filename + "|" + purpose
    try:
        search_socket.connect(db_SERVER_ADDRESS)
        search_socket.send(msg.encode())
        response=search_socket.recv(1024)
        search_socket.close()
        return response.decode()
    except ConnectionError:
        search_socket.connect(backup_db_SERVER_ADDRESS)
        search_socket.send(msg.encode())
        response=search_socket.recv(1024)
        search_socket.close()
        return response.decode()

def otherserverconnect(read_socket,address,port_number,filename,purpose,position,data):
    msg=""
    if purpose == "READ":
        msg = f"READ\n{filename}\n"
    elif purpose == "SEEK":
        msg=f"SEEK\n{filename}\n{position}\n"
    elif purpose == "WRITE":
        msg = f"WRITE\n{filename}\n{data}\n"
    elif purpose == "DELETE":
        msg = f"DELETE\n{filename}\n"
    elif purpose == "ORIGINALDELETE":
        msg = f"ORIGINALDELETE\n{filename}\n"
    elif purpose == "REPLICATEDELETE":
        msg = f"REPLICATEDELETE\n{filename}\n"
    elif purpose == "ORIGINALWRITE":
        msg = f"ORIGINALWRITE\n{filename}\n{data}\n"
    try:
        read_socket.connect((address,int(port_number)))
        read_socket.send(msg.encode())
        response=read_socket.recv(1024)
        read_socket.close()
        return response.decode()
    except ConnectionError:
        return "Unable to connect to server"


def create_file(filename):
    with open(os.path.join(DATA_DIR, filename), 'a') as file:
        pass

def delete_file(filename):
    # file_path = os.path.join(DATA_DIR, filename)
    # if os.path.exists(file_path):
    #     os.remove(file_path)
    search_socket = create_socket()
    response = db_server_search_send(search_socket,filename,"w")
    if response == "FILE_DOES_NOT_EXIST":
        return None
    address=response.split('|')[1]
    port_number=response.split('|')[2]
    if (address,int(port_number)) == SERVER_ADDRESS:
        file_path = os.path.join(DATA_DIR, filename)
        if os.path.exists(file_path):
            os.remove(file_path)
        delete_db_socket = create_socket()
        db_server_delete_send(delete_db_socket,filename,'w')
        return "Deletion Done"
    else:
        write_socket = create_socket()
        filewritten=otherserverconnect(write_socket,address,port_number,filename,'ORIGINALDELETE',0,"")
        if filewritten == "Unable to connect to server":
            pass
        return "Deletion Done"


def open_file(filename):
    try:
        with open(os.path.join(DATA_DIR, filename), 'r') as file:
            return file.read()
    except FileNotFoundError:
        search_socket = create_socket()
        response = db_server_search_send(search_socket,filename,"w")
        if response == "FILE_DOES_NOT_EXIST":
            return None
        address=response.split('|')[1]
        port_number=response.split('|')[2]
        read_socket = create_socket()
        filedata=otherserverconnect(read_socket,address,port_number,filename,'READ',0,'')
        if filedata == "Unable to connect to server":
            search_socket = create_socket()
            response = db_server_search_send(search_socket,filename,"r")
            if response == "FILE_DOES_NOT_EXIST":
                return None
            address=response.split('|')[1]
            port_number=response.split('|')[2]
            read_socket = create_socket()
            filedata=otherserverconnect(read_socket,address,port_number,filename,'READ',0,'')
            return filedata
        else:
            return filedata

def close_file(filename):
    filename.close()

def write_file(filename, data):
    search_socket = create_socket()
    response = db_server_search_send(search_socket,filename,"w")
    if response == "FILE_DOES_NOT_EXIST":
        return None
    address=response.split('|')[1]
    port_number=response.split('|')[2]
    if (address,int(port_number)) == SERVER_ADDRESS:
        with open(os.path.join(DATA_DIR, filename), 'a') as file:
            file.write(data)
        return "writing done"
    # try:
    #     with open(os.path.join(DATA_DIR, filename), 'a') as file:
    #         file.write(data)
    # except FileNotFoundError:
    #     search_socket = create_socket()
    #     response = db_server_search_send(search_socket,filename,"w")
    #     address=response.split('|')[1]
    #     port_number=response.split('|')[2]
    #     read_socket = create_socket()
    #     filedata=otherserverconnect(read_socket,address,port_number,filename,'WRITE',0,data)

    else:
        write_socket = create_socket()
        filewritten=otherserverconnect(write_socket,address,port_number,filename,'ORIGINALWRITE',0,data)
        return "writing done"
        # print(filewritten)
    

def seek_file(filename, offset):
    try:
        with open(os.path.join(DATA_DIR, filename), 'r') as file:
            file.seek(offset, 0) # Seek to the specified offset from the beginning
            return file.read()
    except FileNotFoundError:
        search_socket = create_socket()
        response = db_server_search_send(search_socket,filename,"w")
        if response == "FILE_DOES_NOT_EXIST":
            return None
        address=response.split('|')[1]
        port_number=response.split('|')[2]
        read_socket = create_socket()
        filedata=otherserverconnect(read_socket,address,port_number,filename,'SEEK',offset,'')
        if filedata == "Unable to connect to server":
            search_socket = create_socket()
            response = db_server_search_send(search_socket,filename,"r")
            if response == "FILE_DOES_NOT_EXIST":
                return None
            address=response.split('|')[1]
            port_number=response.split('|')[2]
            read_socket = create_socket()
            filedata=otherserverconnect(read_socket,address,port_number,filename,'SEEK',offset,'')
            return filedata
        else:
            return filedata


def main():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(SERVER_ADDRESS)
    server_socket.listen(15)
    print("Server is listening on", SERVER_ADDRESS)
    copy_and_delete_files(backup_path,path)
    msg = "ADD" + "|" + "1" +"|" + "yes"
    try:
        active_socket = create_socket()
        active_socket.connect(active_server)
        active_socket.send(msg.encode())
        # b=active_socket.recv(1024)
        active_socket.close()
    except ConnectionError:
        active_socket = create_socket()
        active_socket.connect(backup_active_server)
        active_socket.send(msg.encode())
        # b=active_socket.recv(1024)
        active_socket.close()
    while True:
        client_socket, client_address = server_socket.accept()
        print("Accepted connection from", client_address)

        request = client_socket.recv(1024).decode()
        command, filename, data = request.split("\n", 2)

        if command == "CREATE":
            create_file(filename)
            db_socket = create_socket()
            db_server_create_send(db_socket,filename,'yes',SERVER_ADDRESS)
            client_socket.send("File created.".encode())

            # Determine target servers for replication
            other_server_addresses = [(address, port) for address, port in SERVER_ADDRESSES.values() if (address,port) != SERVER_ADDRESS]
            random_servers = random.sample(other_server_addresses,1)
            # Replicate the file
            db_socket = create_socket()
            db_server_create_send(db_socket,filename,'no',random_servers[0])
            file_data = ""  # As the file is just created, it's empty
            replicate_file(filename, file_data, random_servers)
            

        elif command == "DELETE":
            if delete_file(filename) != None:
                delete_file(filename)
                client_socket.send("Deletion done.".encode())
                delete_socket = create_socket()
                response = db_server_search_send(delete_socket,filename,"r")
                address=response.split('|')[1]
                port_number=response.split('|')[2]
                replicatedelete_socket = create_socket()
                otherserverconnect(replicatedelete_socket,address,port_number,filename,'REPLICATEDELETE',0,"")
            else:
                response = "file does not exist"
                client_socket.send(response.encode())
        elif command == "OPEN":
            response = open_file(filename)
            if response != None:
                client_socket.send(response.encode())
            else:
                response = "file does not exist"
                client_socket.send(response.encode())
        elif command == "CLOSE":
            close_file(filename)
            client_socket.send("File closed.".encode())
        elif command == "READ":
            response = open_file(filename)
            # client_socket.send(response.encode())
            if response != None:
                client_socket.send(response.encode())
            else:
                response = "file does not exist"
                client_socket.send(response.encode())
        elif command == "WRITE":
        # Perform write operation
            response = write_file(filename, data)
            if response != None:
                client_socket.send("Data written.".encode())
                file_data = data  # As the file is just created, it's empty
                replicate_socket = create_socket()
                response = db_server_search_send(replicate_socket,filename,"r")
                address=response.split('|')[1]
                port_number=response.split('|')[2]
                x=int(port_number)
                random_servers=(address,x)
                replicate_file(filename, file_data, [random_servers])
                client_socket.send("Write successful".encode())
            else:
                response = "file does not exist"
                client_socket.send(response.encode())

        elif command == "SEEK":
            response = seek_file(filename, int(data))
            if response != None:
                client_socket.send(response.encode())
            else:
                response = "file does not exist"
                client_socket.send(response.encode())
        elif command == "REPLICATE":
            handle_replication_request(filename, data)
        elif command == 'REPLICATEDELETE':
            handle_replication_delete(filename)
        elif command == 'ORIGINALWRITE':
            handle_OriginalWrite_request(filename,data)
        elif command == 'ORIGINALDELETE':
            handle_OriginalDelete_request(filename)
        elif command == 'SWITCHOFF':
            copy_files(path, backup_path)
            response = "Backup done"
            client_socket.send(response.encode())
            client_socket.close()
            break
        #server_socket.close()
        copy_files(path, backup_path)
        client_socket.close()
        

if __name__ == "__main__":
    main()
