import sys
import os
import argparse
from threading import Thread
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import schedule

host_ip = ...
port = ...
BATCH_DURATION = 60


# REQUIRES THE SERVER TO START FIRST or will just start spitting errors

# To transmit directly from console into spark use:
# --> sudo nc -lk <IP> <PORT>
# In case pycharm has issues with enviroment variables:
# PYTHONUNBUFFERED=1;PYSPARK_PYTHON=/home/tryfonm/miniconda3/envs/spark/bin/python3;PYSPARK_DRIVER_PYTHON=/home/tryfonm/miniconda3/envs/spark/bin/python3


class StreamingThread(Thread):
    def __init__(self, ssc):
        """
        In case this script is ran interactively with ipyton/jupyter this is helpful.
        Parameters
        ----------
        ssc
        """
        Thread.__init__(self)
        self.ssc = ssc

    def run(self):
        print("----- ssc started -----\n")
        self.ssc.start()
        self.ssc.awaitTermination()

    def stop(self):
        print('----- ssc stopped -----\n(ignore the following exception)\n')
        self.ssc.stop(stopSparkContext=False, stopGraceFully=True)


# ------------------------------------------------- #
def sparkStreaming(ip, port):
    HOST_IP = str(ip)
    PORT = int(port)
    print(f"- HOST_IP: {HOST_IP}\n- PORT: {PORT}\n")

    # ------------------------------------------------- #

    spark = SparkSession.builder.appName('Streaming Twitter').getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, BATCH_DURATION)

    # ------------------------------------------------- #

    # Create a DStream that will connect to hostname:port, like localhost:9999
    text = ssc.socketTextStream(HOST_IP, PORT)

    # ------ PREPROCESS HERE ------ #

    # text.pprint()
    text.repartition(1).saveAsTextFiles("./TwitterStream/batch")

    return spark, ssc


if '__name__' == "__name__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-ip", "--ip", help="Receiver socket IP to use", type=str, required=True)
    parser.add_argument("-p", "--port", help="Receiver socket port to use", type=int, required=True)
    args = parser.parse_args()

    try:
        spark, ssc = sparkStreaming(args.ip, args.port)
        ssc_t = StreamingThread(ssc)
        ssc_t.start()
    except KeyboardInterrupt:
        print(f'Stopping...')
        os._exit(0)
    # ssc_t.stop()  # This is intended for ipython
