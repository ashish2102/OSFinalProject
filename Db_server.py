import os
import csv      
from socket import *
import shutil

path='db_data/'
file_mappings_path = path + "file_mappings.csv"

serverPort = 9090
serverSocket = socket(AF_INET,SOCK_STREAM)
serverSocket.bind(('localhost', serverPort))
serverSocket.listen(10)
print ('DIRECTORY SERVICE is ready to receive...')


def delete_mapping(client_msg):
	actual_filename = client_msg.split('|')[1]
	RW = client_msg.split('|')[2]
	rows_to_delete = []
	rows_to_keep = []
	with open(file_mappings_path, 'rt') as infile:
		reader = csv.DictReader(infile,delimiter=',')
		fieldnames = reader.fieldnames

		for row in reader:
			rows_to_keep.append(row)
			# Check the condition to decide whether to keep the row
			primary_copy = row['primary']
			if RW == 'w':
				if row['actual_filename'] == actual_filename and primary_copy == 'yes':
					rows_to_delete.append(row)
			elif RW == 'r':
				if row['actual_filename'] == actual_filename and primary_copy == 'no':
					rows_to_delete.append(row)
	final_list = [x for x in rows_to_keep if x not in rows_to_delete]
	# Write updated data back to the file
	with open(file_mappings_path, 'w', newline='') as outfile:
		writer = csv.DictWriter(outfile, fieldnames=fieldnames)
		writer.writeheader()
		for row in final_list:
			writer.writerow(row)

	return "Row deleted successfully."


def write_mapping(client_msg):
	actual_filename = client_msg.split('|')[1]
	server_addr = client_msg.split('|')[2]
	server_port = int(client_msg.split('|')[3])
	primary_copy = client_msg.split('|')[4]
	with open(file_mappings_path, 'a', newline='') as csvfile:
		fieldnames = ['actual_filename', 'server_addr', 'server_port', 'primary']
		writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
		writer.writerow({
            'actual_filename': actual_filename,
            'server_addr': server_addr,
            'server_port': server_port,
            'primary': primary_copy})
	return "details updated in db server"

def check_mappings(recv_msg,list_files):
	filename = recv_msg.split('|')[1]
	RW = recv_msg.split('|')[2]

	with open(file_mappings_path,'rt') as infile:        
		d_reader = csv.DictReader(infile, delimiter=',') 
		_ = d_reader.fieldnames    	
		file_row = ""
		unique_filenames = set()
		for row in d_reader:
			if list_files == False:
				actual_filename = row['actual_filename']
				primary_copy = row['primary']
				if actual_filename == filename and RW == 'w'and primary_copy == 'yes':		
					
					actual_filename = row['actual_filename']	
					server_addr = row['server_addr']			
					server_port = row['server_port']			

					return actual_filename + "|" + server_addr + "|" + server_port	
				elif actual_filename == filename and RW == 'r' and primary_copy == 'no':
					
					actual_filename = row['actual_filename']	
					server_addr = row['server_addr']			
					server_port = row['server_port']			

					return actual_filename + "|" + server_addr + "|" + server_port	
			else:
				user_filename = row['actual_filename']
				if user_filename not in unique_filenames:
					unique_filenames.add(user_filename)
					file_row = file_row + user_filename +  "\n"		
		if list_files == True:
			return file_row
	return None 	

def main():
	if not os.path.exists(file_mappings_path):
		with open(file_mappings_path, 'w', newline='') as csvfile:
			fieldnames = [ 'actual_filename', 'server_addr', 'server_port', 'primary']
			writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
			writer.writeheader()
	while 1:
		connectionSocket, addr = serverSocket.accept()

		response = ""
		recv_msg = connectionSocket.recv(1024)
		recv_msg = recv_msg.decode()

		if "ADD" in recv_msg:
			response = write_mapping(recv_msg)
		elif "SEARCH" in recv_msg:
			response=check_mappings(recv_msg,False)
		elif "DELETE" in recv_msg:
			delete_mapping(recv_msg)
		elif "LIST" in recv_msg:
			response = check_mappings(recv_msg, True)

		if response is not None:	
			response = str(response)
			print("RESPONSE: \n" + response)
			print("\n")
		else:
			response = "FILE_DOES_NOT_EXIST"
			print("RESPONSE: \n" + response)
			print("\n")

		connectionSocket.send(response.encode())	
		connectionSocket.close()

if __name__ == "__main__":
	main()