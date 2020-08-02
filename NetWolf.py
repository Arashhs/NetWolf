import threading
import socket
import time

UDP_MESSAGE_LENGTH_SIZE = 1024
ENCODING = 'utf-8'
DISCOVERY_TIMEOUT = 5


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


class Node:
    def __init__(self, name, address, udp_port):
        self.name = name
        self.address = address
        self.udp_port = udp_port
        self.cluster_list = read_initial_clusters(name)
        self.server_thread = threading.Thread(target=self.udp_server_connection)
        self.server_thread.start()
        self.client_thread = threading.Thread(target=self.udp_client_connection)
        self.client_thread.start()

    def udp_client_connection(self):
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
                discovered_list = convert_to_list(data[4:].split("\n"))
                self.merge_cluster_list(discovered_list)

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
    t1 = threading.Thread(target=Node, args=("N1", "127.0.0.1", 4001))
    t1.start()
    t2 = threading.Thread(target=Node, args=("N2", "127.0.0.1", 4002))
    t2.start()
    t3 = threading.Thread(target=Node, args=("N3", "127.0.0.1", 4003))
    t3.start()
    t4 = threading.Thread(target=Node, args=("N4", "127.0.0.1", 4004))
    t4.start()


if __name__ == '__main__':
    main()