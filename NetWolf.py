import threading
import socket
import time
import os.path
from timeit import default_timer as timer



UDP_MESSAGE_LENGTH_SIZE = 1024
ENCODING = 'utf-8'
DISCOVERY_TIMEOUT = 5
MAXIMUM_NUMBER_OF_TCP_CONNECTIONS = 5
SELFISH_BEHAVIOR_DELAY = 2


def read_initial_clusters(file_name):
    # Reading Initial clusters for each node from their respective files
    with open("{}\\cluster-list.txt".format(file_name)) as file_in:
        lines = file_in.readlines()
        return convert_to_list(lines)


def convert_to_list(lines):
    clist = []
    for line in lines:
        x1 = line.split()
        x2 = x1[1].split(":")
        elem = (x1[0], x2[0], int(x2[1]))
        clist.append(elem)
    return clist


def find_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))
    return s.getsockname()[1]


class Node:
    def __init__(self, name, address, udp_port):
        self.name = name
        self.address = address
        self.udp_port = udp_port
        self.cluster_list = read_initial_clusters(name)
        self.prior_com = []

    def start_running(self):
        self.udp_server_thread = threading.Thread(target=self.udp_server_connection)
        self.udp_server_thread.start()
        self.udp_client_discovery_thread = threading.Thread(target=self.udp_client_discovery)
        self.udp_client_discovery_thread.start()


    def udp_client_discovery(self):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        while True:
            # DIS at the first line --> Discovery message
            message = "DIS\n" + self.clusters_to_string()
            # Adding its own name and address to the message
            message += "\n{} {}:{}".format(self.name, self.address, self.udp_port)
#            print("{}: {}".format(self.name, message))
            for elem in self.cluster_list:
                client_socket.sendto(bytes(message, ENCODING), (elem[1], elem[2]))
            time.sleep(DISCOVERY_TIMEOUT)

    def udp_client_get(self, file_name):
        threads = [None] * len(self.cluster_list)
        results = [None] * len(self.cluster_list)
        dataList = [None] * len(self.cluster_list)
        for i in range(len(threads)):
            threads[i] = threading.Thread(target=self.get_response, args=(file_name, self.cluster_list[i], results, dataList, i))
            threads[i].start()
        # do some other stuff
        for i in range(len(threads)):
            threads[i].join(timeout=1)
        shortestTime = 100
        shortestIndex = 0
        for i in range(len(results)):
            if results[i] != 0 and results[i] < shortestTime:
                shortestTime = results[i]
                shortestIndex = i
        if shortestTime != 100:
            # get data through tcp connection from cluster_list[shortestIndex]
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            client_socket.bind((self.address, 0))
            # GEG at the first line --> Telling cluster_list[shortestIndex] to establish a tcp connection and send the file
            message = "GEG\n{}".format(file_name)
            tcp_port = int(dataList[shortestIndex].split("\n")[1].split(":")[1])
            tcp_addr = dataList[shortestIndex].split("\n")[1].split(":")[0]
            # Adding its own name and address at the end of the message
            message += "\n{} {}".format(self.name, tcp_port)
            client_socket.sendto(bytes(message, ENCODING), (self.cluster_list[shortestIndex][1], self.cluster_list[shortestIndex][2]))
            recv_tcp_thread = threading.Thread(target=self.recv_tcp, args=(tcp_addr, tcp_port, file_name))
            recv_tcp_thread.start()
            print("Getting {} from node {}".format(file_name, self.cluster_list[shortestIndex][0]))
            if self.cluster_list[shortestIndex][0] not in self.prior_com:
                self.prior_com.append(self.cluster_list[shortestIndex][0])
        else:
            print('No peer has the file "{}"'.format(file_name))




    def get_response(self, file_name, cluster, results, dataList, i):
        results[i] = 0
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client_socket.bind((self.address, 0))
        # GET at the first line --> GET message
        message = "GET\n{}".format(file_name)
        # Adding its own name and address at the end of the message
        message += "\n{} {}:{}".format(self.name, self.address, client_socket.getsockname()[1])
        start = timer()
        client_socket.sendto(bytes(message, ENCODING), (cluster[1], cluster[2]))
        data = client_socket.recv(UDP_MESSAGE_LENGTH_SIZE)
        end = timer()
        data = data.decode(ENCODING)
        if data.split("\n")[0] == "GER":
            results[i] = end - start
            dataList[i] = data

    def recv_tcp(self, server_address, server_port, file_name):
        socket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket1.connect((server_address, server_port))
        downloaded_file = "{}\\{}".format(self.name, file_name)
        with open(downloaded_file, 'wb') as file_to_write:
            while True:
                data = socket1.recv(1024)
                # print data
                if not data:
                    break
                # print data
                file_to_write.write(data)
        file_to_write.close()
        print("File received successfuly. Saved to {}".format(downloaded_file))
        socket1.close()



    def clusters_to_string(self):
        s = ""
        for elem in self.cluster_list:
            s += elem[0] + " " + elem[1] + ":" + str(elem[2]) + "\n"
        return s[:-1]

    def udp_server_connection(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((self.address, self.udp_port))
        while True:
            data, addr = sock.recvfrom(UDP_MESSAGE_LENGTH_SIZE)  # buffer size is 1024 bytes
            data = data.decode(ENCODING)
            data_list = data.split("\n")
#            print("{}: received message from {}:".format(self.name, addr))
#            print("****d1: {}******".format(str(data[4:])))
            if data[0:3] == "DIS":
                #  This is a DISCOVERY Message
                discovered_list = convert_to_list(data[4:].split("\n"))
                self.merge_cluster_list(discovered_list)
            elif data[0:3] == "GET":
                #  This is a GET request
                file_name = data_list[1]
#                print("data",data_list)
                req_ip = data_list[2].split()[1].split(":")[0]
                req_port = int(data_list[2].split()[1].split(":")[1])
                rel_file_name = "{}\\{}".format(self.name, file_name)
                available = os.path.isfile(rel_file_name)
#               print("In {}: GET Request for file <{}> from {}:{} - Available: {}".format(self.name, file_name, req_ip, req_port, available))
                if available:
                    # File is available for transmission
                    tcp_port = find_free_port()
#                    print("In {}: GET Request for file <{}> from {}:{} - Available: {}, ip {} and port {}".format(self.name, file_name, req_ip, req_port, available, tcp_sock.getsockname()[0], tcp_port))
                    # GER is the first word of message -> File is available at this node
                    message = "GER\n{}:{}".format(self.address, tcp_port)
                    sock.sendto(bytes(message, ENCODING), (req_ip, req_port))
            elif data[0:3] == "GEG":
                file_name = data_list[1]
                tcp_port = int(data_list[2].split()[1])
                requested_by = data_list[2].split()[0]
                print(requested_by)
                selfish_behaviour = False
                if requested_by not in self.prior_com:
                    selfish_behaviour = True
                rel_file_name = "{}\\{}".format(self.name, file_name)
#                print(rel_file_name, req_port)
                tcp_send_thread = threading.Thread(target=self.send_tcp, args=(tcp_port, rel_file_name, selfish_behaviour))
                tcp_send_thread.start()

    def send_tcp(self, tcp_port, file_name, selfish_behaviour):
        server_tcp = socket.socket()  # Create a socket object
        server_tcp.bind((self.address, tcp_port))  # Bind to the port
        server_tcp.listen()
        conn, addr = server_tcp.accept()
        if(selfish_behaviour):
            print("Selfish behaviour detected by the client. Delaying for {} seconds".format(SELFISH_BEHAVIOR_DELAY))
            time.sleep(SELFISH_BEHAVIOR_DELAY)
        with open(file_name, 'rb') as file_to_send:
            for data in file_to_send:
                conn.sendall(data)


    def merge_cluster_list(self, rec_list):
        for elem in rec_list:
            if elem[0] != self.name:
                duplicated = False
                for elem2 in self.cluster_list:
                    if elem[0] == elem2[0]:
                        #  Node already exists in the cluster_list or is the self node
                        duplicated = True
                if not duplicated:
                    self.cluster_list.append(elem)
#        print("{}: {}".format(self.name, self.cluster_list))

    def show_clusters_list(self):
        print("This is the clusters_list of {}:".format(self.name))
        print(self.clusters_to_string())


def get_user_commands(nodes_list):
    print("Which node do you want to execute commands on? (select number)")
    for i in range(len(nodes_list)):
        print("{}) {}\t".format(i, nodes_list[i].name))
    selected = int(input("> "))
    print('Commands:\n1) "switch": change selected node\n2) "list": show cluster_list of {}'.format(
    nodes_list[int(selected)].name))
    print('3) "get <filename>" download the file from the fastest peer. e.g. "get dark-souls.jpg"')
    print('4) "help": showing this help menu anytime you needed')
    while True:
        command = input("> ")
        if command == "switch":
            print("Which node do you want to execute commands on? (select number)")
            for i in range(len(nodes_list)):
                print("{}) {}\t".format(i, nodes_list[i].name))
            selected = int(input("> "))
        elif command == "list":
            nodes_list[selected].show_clusters_list()
        elif command.split() and command.split()[0] == "get":
            nodes_list[selected].udp_client_get(command.split()[1])
        elif command == "help":
            print('Commands:\n1) "switch": change selected node\n2) "list": show cluster_list of {}'.format(
                nodes_list[int(selected)].name))
            print('3) "get <filename>" download the file from the fastest peer. e.g. "get dark-souls.jpg"')
            print('4) "help": showing this help menu anytime you needed')
        else:
            print('The command is not valid. Enter "help" to see the valid commands.')


def main():
    n1 = Node("N1", "127.0.0.1", 4001)
    n2 = Node("N2", "127.0.0.1", 4002)
    n3 = Node("N3", "127.0.0.1", 4003)
    n4 = Node("N4", "127.0.0.1", 4004)
    t1 = threading.Thread(target=n1.start_running)
    t1.start()
    t2 = threading.Thread(target=n2.start_running)
    t2.start()
    t3 = threading.Thread(target=n3.start_running)
    t3.start()
    t4 = threading.Thread(target=n4.start_running)
    t4.start()
    nodes = [n1, n2, n3, n4]
    get_user_commands(nodes)


if __name__ == '__main__':
    main()