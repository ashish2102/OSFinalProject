from socket import *
from collections import defaultdict	

serverAddr = "localhost"
serverPort = 4040
serverSocket = socket(AF_INET,SOCK_STREAM)
serverSocket.bind((serverAddr, serverPort))
serverSocket.listen(10)
print ('Locking service is ready ...')

def check_if_unlocked(filename, lock_map):
	if filename in lock_map:	
		if lock_map[filename] == "unlocked":
			return True
		else:
			return False
	else:
		lock_map[filename] = "unlocked"
		return True

def main():

	lock_map = {}
	clients_map = defaultdict(list)
	is_client_waiting = False
	expiry_map = {}


	while 1:
		connectionSocket, _ = serverSocket.accept()
		response = connectionSocket.recv(1024)
		response = response.decode()

		print(f'response is {response}' )

		if "1:" in response:
			client_id = response.split("1:")[0]
			filename = response.split("1:")[1]
			is_client_waiting = False


			unlocked = check_if_unlocked(filename, lock_map)
			if unlocked == True:
				counter = 0	
				if len(clients_map[filename]) == 0:	
					lock_map[filename] = "locked"	
					grant_message = "file_granted"
					connectionSocket.send(grant_message.encode())

				elif filename in clients_map:			
					for filename,values in clients_map.items():	
						for v in values:									
							if v == client_id and counter == 0:			
								clients_map[filename].remove(v)	
								lock_map[filename] = "locked"	
								grant_message = "file_granted"			
								print("SENT: " + grant_message +" ---- " + client_id)	
								connectionSocket.send(grant_message.encode())	
							counter += 1

			else:				
				grant_message = "file_not_granted"

				if client_id in expiry_map:		
					expiry_map[client_id] = expiry_map[client_id] + 1		
				else:
					expiry_map[client_id] = 0	


				if expiry_map[client_id] == 100:	
					timeout_msg = "TIMEOUT"
					for filename,values in clients_map.items():	
						for v in values:									
							if v == client_id:		
								clients_map[filename].remove(v)	
					del expiry_map[client_id]			
					connectionSocket.send(timeout_msg.encode())	
				else:

					if filename in clients_map:						
						for filename,values in clients_map.items():	
							for v in values:							
								if v == client_id:					
									is_client_waiting = True			
					if is_client_waiting == False:			
						clients_map[filename].append(client_id)	

					connectionSocket.send(grant_message.encode())	

		elif "2:" in response:		
			client_id = response.split("2:")[0]
			filename = response.split("2:")[1]

			lock_map[filename] = "unlocked"		
			grant_message = "File unlocked..."
			connectionSocket.send(grant_message.encode())	
		elif "status" in response:
			client_id = response.split("status")[0]
			filename = response.split("status")[1]
			if check_if_unlocked(filename, lock_map):
				grant_message = "file_granted"
				connectionSocket.send(grant_message.encode())	
			else:
				grant_message = "file_not_granted"
				connectionSocket.send(grant_message.encode())	
		connectionSocket.close()



if __name__ == "_main_":
	main()