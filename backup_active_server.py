import os
import csv      #To work with csv file
from socket import *
import random

path='active_server/'
file_mappings_path = path + "servers_status.csv"

serverPort = 8892
serverSocket = socket(AF_INET,SOCK_STREAM)
serverSocket.bind(('localhost', serverPort))
serverSocket.listen(10)
print ('Active Server SERVICE is ready to receive...')

def write_mapping(client_msg):
	server_no = client_msg.split('|')[1]
	Active = client_msg.split('|')[2]
	with open(file_mappings_path, 'a', newline='') as csvfile:
		fieldnames = ['server_no', 'Active']
		writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
		writer.writerow({
            'server_no': server_no,
            'Active': Active})
	return "Server status entered"

def check_mappings():
	server_list = []
	with open(file_mappings_path,'rt') as infile:        # open the .csv file storing the mappings
		d_reader = csv.DictReader(infile, delimiter=',')    # read file as a csv file, taking values after commas
		_ = d_reader.fieldnames    	# skip header of csv file
		for row in d_reader:
			server_no = row['server_no']
			server_list.append(server_no)
		return random.sample(server_list,1)
	return None

def main():
	if not os.path.exists(file_mappings_path):
		with open(file_mappings_path, 'w', newline='') as csvfile:# If file_mappings.csv doesn't exist, create it with the header
			fieldnames = ['server_no', 'Active']
			writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
			writer.writeheader()
	while 1:
		connectionSocket, addr = serverSocket.accept()

		response = ""
		recv_msg = connectionSocket.recv(1024)
		recv_msg = recv_msg.decode()
		if "ADD" in recv_msg:
			response = write_mapping(recv_msg)
			print("RESPONSE: \n" + response)
			print("\n")
		elif "LISTPLEASE" in recv_msg:
			res = check_mappings()
			response = res[0]
		if response is None:	# for existance of file
			print("RESPONSE: No Server is Active")
			print("\n")
		connectionSocket.send(response.encode())	# send the file information or non-existance message to the client
		connectionSocket.close()
if __name__ == "__main__":
	main()