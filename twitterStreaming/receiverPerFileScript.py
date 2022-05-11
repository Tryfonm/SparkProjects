import sys
import os
import socket
import time
from datetime import datetime
from typing import Optional
import schedule
import argparse


# Accompanies the serverScript.py file
# Uses inverse logic for the client and server - for educational purposes

class Receiver():

    def __init__(self, duration, IP, port, outputLocation: Optional[str] or None = None) -> None:
        """

        Parameters
        ----------
        duration
        IP
        outputLocation
        """
        self._duration = duration
        self._IP = IP
        self._PORT = port
        self._fileCounter = 1

        self._outputLocation = os.path.join(os.getcwd(), 'stored_data')
        if not os.path.exists(self._outputLocation):
            os.makedirs(self._outputLocation)

    @staticmethod
    def encode(f1, f2) -> None:
        """

        Parameters
        ----------
        f1
        f2

        Returns
        -------

        """
        data = data2 = ""

        with open(f1, 'rb') as fp:
            data = fp.read()

        with open(f2, 'rb') as fp:
            data2 = fp.read()

        data += b"***EOF***"
        data += data2
        with open(os.path.join('stored_data', 'tempFile'), 'wb') as fp:
            fp.write(data)

    @staticmethod
    def decode(fpath) -> None:
        """

        Parameters
        ----------
        fpath

        Returns
        -------

        """
        data1 = data2 = ""
        # with open(fpath) as f:
        with open(fpath, 'rb') as f:
            data1, data2 = f.read().split(b"***EOF***")
            newFileName = f.readline()

        baseName = data2.split()[0].decode('utf-8').replace(':', '')

        with open(fpath[:-8] + baseName + '.csv', 'wb') as csvFile:
            csvFile.write(data1)

        with open(fpath[:-8] + baseName + '.log', 'wb') as csvFile:
            csvFile.write(data2)

    def _setupReceiverConnection(self) -> None:
        """

        Returns
        -------

        """

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self._IP, self._PORT))
        server.listen()

        client_socket, client_adress = server.accept()

        tempFileName = os.path.join(self._outputLocation, 'tempFile')

        tempFileHolder = open(tempFileName, 'wb')
        data = client_socket.recv(2048)
        self._fileCounter += 1
        while data:
            tempFileHolder.write(data)
            data = client_socket.recv(2048)
        tempFileHolder.close()

        # After the whole file has been received
        try:
            Receiver.decode(tempFileName)
            print(f'Successfully received a file')

        except UnicodeDecodeError:
            print(f'Unicode error - Skipping the whole minute')
        finally:
            os.remove(tempFileName)

        client_socket.close()

    def startClient(self) -> None:
        """

        Returns
        -------

        """

        start = time.time()

        schedule.every().minute.at(":01").do(
            self._setupReceiverConnection)  # scrapingThreadWrapper ->

        print(
            f'\n----- Started listening for data at {datetime.now().strftime("%H:%M:%S")} ----- \n')

        while True:
            if time.time() - start >= self._duration * 60:
                break
            schedule.run_pending()
        print(
            f'\n----- Finished listening for data at {datetime.now().strftime("%H:%M:%S")} ----- \n')


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--duration", help="Scraping duration in minutes", default=20, type=int)
    parser.add_argument("-ip", "--receiverIP", help="Receiver IP to use", type=str, required=True)
    parser.add_argument("-port", "--receiverPort", help="Receiver Port to use", type=int, required=True)

    args = parser.parse_args()

    receiver = Receiver(duration=args.duration, IP=args.receiverIP, port=args.receiverPort)
    try:
        receiver.startClient()
    except:
        print(f'\n Stopping...')
        os._exit(0)
