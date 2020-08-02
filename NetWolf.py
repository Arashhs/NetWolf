import threading
import socket
import time
import os.path
from timeit import default_timer as timer



UDP_MESSAGE_LENGTH_SIZE = 1024
ENCODING = 'utf-8'
DISCOVERY_TIMEOUT = 2


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
                print("data",data_list)
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


def main():
#    t1 = threading.Thread(target=Node, args=("N1", "127.0.0.1", 4001))
#    t1.start()
    t2 = threading.Thread(target=Node, args=("N2", "127.0.0.1", 4002))
    t2.start()
    t3 = threading.Thread(target=Node, args=("N3", "127.0.0.1", 4003))
    t3.start()
    t4 = threading.Thread(target=Node, args=("N4", "127.0.0.1", 4004))
    t4.start()
    time.sleep(1)
    n1 = Node("N1", "127.0.0.1", 4001)
    n1.udp_client_get("hello.txt")


if __name__ == '__main__':
    main()