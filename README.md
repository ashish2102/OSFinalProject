Run all the servers(.py) files and clients on different command prompt windows.
First start running all the 3 core servers Active_Server.py, Db_server.py, lock_server.py.
Then run the backup_active_server, backup_Db_server followed by server1.py, server2.py, server3.py. 
Then run as many clients on different command prompt windows(Load test).
Test the system functionality for file operations create, delete, open, close, read, write, seek. 
Check for file replication by writing into a file created using other server.(Note: Replication is pessimistic)
Now try to down any server(1, 2, 3) to test the system for node failure.
