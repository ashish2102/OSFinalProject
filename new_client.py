import socket
import time
from time import gmtime, strftime

SERVER_ADDRESSES = {
    '1': ('localhost', 8881),
    '2': ('localhost', 8882),
    '3': ('localhost', 8883)
    # '4': ('localhost', 8885),
    # '5': ('localhost', 8886),
    # '6': ('localhost', 8887)
}
active_server = ('localhost', 8890)
backup_active_server = ('localhost', 8892)
client_id=strftime("%Y%m%d%H%M%S", gmtime())
db_SERVER_ADDRESS = ('localhost',9090)

def create_socket_lock():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    return s

def is_server_active(host, port):
    try:
        # Create a socket to check if the server is reachable
        with socket.create_connection((host, port), timeout=1) as s:
            return True
    except (socket.timeout, ConnectionRefusedError):
        return False

def send_request(SERVER_ADDRESS,request):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            client_socket.connect(SERVER_ADDRESS)
            client_socket.send(request.encode())
            start_time = time.time()
            total_data_received = 0
            # while True:
                # if time.time() - start_time > 10:
                #     print("Write operation timed out.")
                #     break
            response = client_socket.recv(1024).decode()
            end_time = time.time()
            response_time = end_time - start_time
            total_data_received += len(response)
            print("Server response:", response)
            if response_time != 0:
                throughput = total_data_received / response_time
                print("Response time:", response_time, "seconds")
                print("Throughput:", throughput, "bytes/second")
                
            else:
                print("Error: Response time is zero, cannot calculate throughput.")
                
    except ConnectionError:
                print(f"Failed to connect to server {SERVER_ADDRESS}")


def lock_unlock_file(client_socket, client_id, filename, lock_or_unlock):   

    serverName = 'localhost'
    serverPort = 4040   # port of directory service
    client_socket.connect((serverName,serverPort))

    if lock_or_unlock == "lock":
        msg = client_id + "_1_:" + filename  # 1 = lock the file
    elif lock_or_unlock == "unlock":
        msg = client_id + "_2_:" + filename   # 2 = unlock the file
    elif lock_or_unlock == "status":
        msg = client_id + "status" + filename

    # send the string requesting file info to directory service
    client_socket.send(msg.encode())
    reply = client_socket.recv(1024)
    reply = reply.decode()

    return reply

def create_socket():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    return s


if __name__ == "__main__":
    
    msg = "LISTPLEASE"
    response1 = ''
    try:
        active_socket = create_socket()
        active_socket.connect(active_server)
        active_socket.send(msg.encode())
        response1 = active_socket.recv(1024).decode()
        active_socket.close()
    except ConnectionError:
        active_socket = create_socket()
        active_socket.connect(backup_active_server)
        active_socket.send(msg.encode())
        response1 = active_socket.recv(1024).decode()
        active_socket.close()
    SERVER_ADDRESS = SERVER_ADDRESSES[response1]
    print("Client will connect to:",SERVER_ADDRESS,"after choosing any below operations from 1-8")
    print("Client will connect to:",db_SERVER_ADDRESS,"if the option chosen is 9")
    # server_choice = input("Enter the server number you want to connect to (1-4): ")
    while True:

        # if server_choice not in SERVER_ADDRESSES:
        #     print("Invalid server choice.")
        # else:
        #     SERVER_ADDRESS = SERVER_ADDRESSES[server_choice]
        print("Options:")
        print("1. CREATE")
        print("2. DELETE")
        print("3. OPEN")
        print("4. CLOSE")
        print("5. READ")
        print("6. WRITE")
        print("7. SEEK")
        print("8. SWITCHOFF CONNECTED SERVER")
        print("9. LIST OF FILES IN THE SYSTEM")
        print("10. EXIT")

        choice = input("Enter your choice (1-9): ")

        if choice == '1':
            filename = input("Enter filename to create: ")
            request = f"CREATE\n{filename}\n"
            response = send_request(SERVER_ADDRESS, request)
            # print(response)
            # send_request(request)
        elif choice == '2':
            filename = input("Enter filename to delete: ")
            request = f"DELETE\n{filename}\n"
            send_request(SERVER_ADDRESS,request)
        elif choice == '3':
            filename = input("Enter filename to open: ")
            lock_socket=create_socket_lock()
            lock_status = lock_unlock_file(lock_socket, client_id, filename, "status")
            lock_socket.close()
            while lock_status != "file_granted":
                    print("File not granted, polling again...")
                    client_socket = create_socket_lock()
                    lock_status = lock_unlock_file(client_socket, client_id, filename, "status")
                    client_socket.close()
                    time.sleep(3)
            print("You are granted the file...")
            request = f"OPEN\n{filename}\n"
            send_request(SERVER_ADDRESS,request)
        elif choice == '4':
            filename = input("Enter filename to close: ")
            request = f"CLOSE\n{filename}\n"
            send_request(SERVER_ADDRESS,request)
        elif choice == '5':
            filename = input("Enter filename to read: ")
            lock_socket=create_socket_lock()
            lock_status = lock_unlock_file(lock_socket, client_id, filename, "status")
            lock_socket.close()
            while lock_status != "file_granted":
                print("File not granted, polling again...")
                client_socket = create_socket_lock()
                lock_status = lock_unlock_file(client_socket, client_id, filename, "status")
                client_socket.close()
                time.sleep(3)
            print("You are granted the file...")
            request = f"READ\n{filename}\n"
            send_request(SERVER_ADDRESS,request)
        elif choice == '6':
            filename = input("Enter filename to write: ")
            lock_socket=create_socket_lock()
            grant_lock = lock_unlock_file(lock_socket, client_id, filename, "lock")
            lock_socket.close()
            while grant_lock != "file_granted":
                print("File not granted, polling again...")
                client_socket = create_socket_lock()
                grant_lock = lock_unlock_file(client_socket, client_id, filename, "lock")
                client_socket.close()
                time.sleep(3)     # wait 3 sec if lock not available and request it again
            print("You are granted the file...")
            data = input("Enter data to write: ")
            request = f"WRITE\n{filename}\n{data}\n"
            send_request(SERVER_ADDRESS, request)
            client_socket = create_socket_lock()
            reply_unlock = lock_unlock_file(client_socket, client_id, filename, "unlock")
            client_socket.close()
            print (reply_unlock)
        elif choice == '7':
            filename = input("Enter filename to seek: ")
            position = input("Enter the position to seek to: ")
            lock_socket=create_socket_lock()
            lock_status = lock_unlock_file(lock_socket, client_id, filename, "status")
            lock_socket.close()
            while lock_status != "file_granted":
                    print("File not granted, polling again...")
                    client_socket = create_socket_lock()
                    lock_status = lock_unlock_file(client_socket, client_id, filename, "status")
                    client_socket.close()
                    time.sleep(3)
            print("You are granted the file...")
            request = f"SEEK\n{filename}\n{position}\n"
            send_request(SERVER_ADDRESS,request)
        elif choice == '8':
             request = f"SWITCHOFF\n\n"
             send_request(SERVER_ADDRESS, request)
        elif choice == '9':
             msg = "LIST||"
             send_request(db_SERVER_ADDRESS, msg)
        elif choice == '10':
            break
        else:
            print("Invalid choice. Please try again.")
