import socket

# Use this as a template
IP = ...
PORT = ...

# sudo nc <IP> <PORT>


server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind((IP, PORT))
server.listen(1)
client_socket, client_adress = server.accept()

print(f'Connected with {client_adress}')
tempFileName = 'testFile.txt'

tempFileHolder = open(tempFileName, 'rb')
data = client_socket.send(tempFileHolder.read())

client_socket.close()
