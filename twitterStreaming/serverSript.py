import time
import timeit
import snscrape.modules.twitter as sntwitter
from tQuery import twitterQuery
import pandas as pd
from datetime import datetime, tzinfo
import threading
import socket
import schedule
import pytz
import os
import sys
import argparse
import warnings
from typing import Optional

warnings.simplefilter(action='ignore', category=FutureWarning)

pd.set_option('display.max_rows', 10000)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1500)


class MyTwitterScraper():
    """

    """

    def __init__(self, dictOfSearchTerms, duration, optimLimit, outputLocation,
                 useSocketIP: Optional[str] or None = None, useSocketPort: Optional[int] or None = None,
                 clearDirectory: Optional[bool] = False,
                 forSparkSocket: Optional[bool] = False) -> None:

        if not dictOfSearchTerms:
            print(f'"dictOfSearchTerms" parameter needs to be defined!')

        if useSocketPort and not useSocketIP:
            print(f"Can't provide Port without providing IP")

        if isinstance(dictOfSearchTerms, dict):
            self._dictOfSearchTerms = dictOfSearchTerms
        else:  # Whatever the input was => it will be converted in string and used as a search term
            # 'toDate' and 'fromDate' params are kept in case of subclassing and augmenting functionality of the script.
            self._dictOfSearchTerms = {
                'searchTerms': dictOfSearchTerms, 'exactPhrase': None, 'hashtags': None,
                'fromAccount': None, 'toDate': None, 'fromDate': None
            }

        self._duration = duration
        self._optimLimit = optimLimit
        self._outputLocation = outputLocation
        self._useSocketIP = useSocketIP
        self._useSocketPort = useSocketPort
        self._clearDirectory = clearDirectory
        self._forSparkSocket = forSparkSocket
        if forSparkSocket:
            self._streamType = self.startSparkStream
        elif not forSparkSocket:
            self._streamType = self.startPerFileStream

        # Create directory if it does not exist
        if not os.path.exists(self._outputLocation):
            os.makedirs(self._outputLocation)

        self._client_socket = self._client_adress = None
        self._client = self._server = None
        self._currentPointer = None
        self._previousPointer = None

    @staticmethod
    def snTwitterWrapper(limit, **kwargs):
        """

        Parameters
        ----------
        limit
        kwargs

        Returns
        -------

        """
        tQuery = twitterQuery()
        tQuery.setQuery(*kwargs.values())
        query = tQuery.getQuery()

        tweets = []
        for tweet in sntwitter.TwitterSearchScraper(query).get_items():
            if len(tweets) == limit:
                break
            else:
                tweets.append([
                    tweet.id,
                    tweet.date,
                    datetime.now(tz=pytz.UTC),
                    tweet.username,
                    tweet.content,
                    tweet.url,
                    kwargs.items()
                ])

        df = pd.DataFrame(tweets, columns=['tweet_id', 'dateTime',
                                           'dateTimeScraped', 'username',
                                           'content', 'url',
                                           'searchParams'])
        return df

    def scrapingThreadWrapper(self) -> None:

        df = self.snTwitterWrapper(
            limit=self._optimLimit,
            searchTerms=self._dictOfSearchTerms['searchTerms'],
            exactPhrase=self._dictOfSearchTerms['exactPhrase'],
            hashtags=self._dictOfSearchTerms['hashtags'],
            fromAccount=self._dictOfSearchTerms['fromAccount'],
            toDate=None,  # hard-coded 'None' - as explained in the constructor above
            fromDate=None  # hard-coded 'None' - as explained in the constructor above
        )

        currentMinute = (df.loc[:, 'dateTimeScraped'] - pd.Timedelta(minutes=1)).dt.minute
        currentTimeStampAsString = \
            f"{df.loc[:, 'dateTimeScraped'].dt.year[0]:04}" + '-' + \
            f"{df.loc[:, 'dateTimeScraped'].dt.month[0]:02}" + '-' + \
            f"{df.loc[:, 'dateTimeScraped'].dt.day[0]:02}" + '_' + \
            f"{df.loc[:, 'dateTimeScraped'].dt.hour[0]:02}" + ':' + \
            f"{df.loc[:, 'dateTimeScraped'].dt.minute[0]:02}"

        previousTimeStamp = df.loc[:, 'dateTimeScraped'] - pd.Timedelta(minutes=2)
        previousMinute = previousTimeStamp.dt.minute

        currentMinuteRecordsCount = df.loc[df.dateTime.dt.minute == currentMinute].shape[0]
        previousMinuteRecordsCount = df.loc[df.dateTime.dt.minute == previousMinute].shape[0]
        nextMinuteRecordsCount = df.loc[df.dateTime.dt.minute == currentMinute + 1].shape[0]
        totalRecords = df.count()

        if previousMinuteRecordsCount == 0:
            print(f'! Did not scrape the full minute '
                  f'=> increasing the optimLimit value from {self._optimLimit} to {self._optimLimit * 2}')
            self._optimLimit *= 2
        elif previousMinuteRecordsCount <= 0.2 * currentMinuteRecordsCount:
            print(f'! Volatility safety not respected '
                  f'=> increasing the optimLimit value from {self._optimLimit} to {int(self._optimLimit * 1.2)}')
            self._optimLimit *= int(1.2)
        else:
            pass
            # print("- No adjustment required")

        df = df.loc[df.dateTime.dt.minute == currentMinute]
        finalDf = df.drop(columns=['dateTimeScraped', 'searchParams'], inplace=False)

        fname = os.path.join(self._outputLocation, currentTimeStampAsString) + '.csv'

        finalDf.to_csv(path_or_buf=fname.replace(':', ''), sep=',')

        try:
            with open(fname.replace('.csv', '').replace(':', '') + '.log', 'w') as logFile:
                logFile.write(
                    f'{currentTimeStampAsString}\n'
                    f'CurrentMinuteRecordsCount:    {currentMinuteRecordsCount}\n'
                    f'PreviousMinuteRecordsCount:   {previousMinuteRecordsCount}\n'
                    f'NextMinuteRecordsCount:       {nextMinuteRecordsCount}\n'
                    f'optimLimit:                   {self._optimLimit}\n'
                    f'SearchParams:\n{[str(x) for x in [y for y in df.searchParams.iloc[0]]]}\n'

                )
            print(f'- Successfully saved data for timestamp {currentTimeStampAsString}')
        except:
            print(f'! Failed to save data for timestamp {currentTimeStampAsString}')
            print('# --------------------------------------')

        self._currentPointer = fname.replace(':', '')

    @staticmethod
    def encode(f1, f2):
        data = data2 = ""

        with open(f1) as fp:
            data = fp.read()

        with open(f2) as fp:
            data2 = fp.read()

        data += "***EOF***"
        data += data2
        with open('tempFile', 'w') as fp:
            fp.write(data)

    def sendThread(self, filePointer) -> None:
        """
        Uses inverse logic for the the client and the server - implemented for educational purposes

        Parameters
        ----------
        filePointer: is the previous filePointer

        Returns
        -------

        """
        self._client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self._client.connect((self._useSocketIP, self._useSocketPort))
            self.encode(filePointer, filePointer.split('.csv')[0] + '.log')

            # file = open(filePointer, 'rb')
            file = open('tempFile', 'rb')
            buffer = file.read(2048)

            while buffer:
                self._client.send(buffer)
                buffer = file.read(2048)

            file.close()
            # os.remove('tempFile')
            self._client.close()
            if self._clearDirectory:
                try:
                    os.remove(filePointer)
                    os.remove(filePointer.split('.csv')[0] + '.log')
                except FileNotFoundError:
                    pass
        except ConnectionRefusedError:
            print(f'! Cannot connect to the receiver => Current file was not sent to the receiver, but saved locally')

        # print(f'Just sent the previous file meaning: {self._previousPointer}')

    def startPerFileStream(self) -> None:

        scrapeThread = threading.Thread(target=self.scrapingThreadWrapper)
        scrapeThread.start()

        if self._previousPointer is not None and self._useSocketIP:
            # TODO: ALSO IMPLEMENT - DELETE THE FILE IN THE DIRECTORY
            sendThread = threading.Thread(target=self.sendThread, args=(self._previousPointer,))  # mind the extra ,
            sendThread.start()
            sendThread.join()

        scrapeThread.join()
        # print(f' -->  {self._previousPointer}')
        # print(f' -->  {self._currentPointer}')

        self._previousPointer = self._currentPointer

    def startSparkStream(self):

        scrapeThread = threading.Thread(target=self.scrapingThreadWrapper)
        scrapeThread.start()

        sparkStreamThread = threading.Thread(target=self.sparkStreamWrapper)
        sparkStreamThread.start()

    def sparkStreamWrapper(self):
        # print(f'PreviousPointer is: {self._previousPointer}')
        # print(f'CurrentPointer is:  {self._currentPointer}')
        while True:
            if self._currentPointer != self._previousPointer:
                # new has been generated and is ready to send
                self.encode(self._currentPointer, self._currentPointer.split('.csv')[0] + '.log')
                tempFileHolder = open('tempFile', 'rb')
                self._client_socket.send(tempFileHolder.read())
                tempFileHolder.close()

                # Clean directory
                if self._clearDirectory:

                    try:
                        os.remove(self._currentPointer)
                        os.remove(self._currentPointer.split('.csv')[0] + '.log')
                    except FileNotFoundError:
                        pass  # dirty as fuck
                # Move on to the next file if it exists
                self._previousPointer = self._currentPointer

                # os.remove('tempFile')

    def triggerScheduler(self) -> None:
        start = time.time()

        if self._forSparkSocket:
            try:
                self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._server.bind((self._useSocketIP, self._useSocketPort))
                self._server.listen(1)

                self._client_socket, self._client_adress = self._server.accept()
                print(f'Connected with {self._client_adress}')
            except ConnectionRefusedError:
                print(f'- Cannot connect to the receiver')
                sys.exit(0)

        schedule.every().minute.at(":02").do(self._streamType)  # scrapingThreadWrapper ->

        print(f'\n----- Started scraping at {datetime.now().strftime("%H:%M:%S")} ----- '
              f'\n- The script will be active for {self._duration} minutes\n')
        while True:
            if time.time() - start >= self._duration * 60:
                break
            schedule.run_pending()
        try:
            os.remove('tempFile')
        except FileNotFoundError:
            pass  # was never created in the first place => ignore the deletion
        self.sendThread(self._currentPointer) if not self._forSparkSocket else 0  # This is dirty...
        print(f'\n----- Finished scraping at {datetime.now().strftime("%H:%M:%S")} ----- \n')


class ScrapingThread(threading.Thread):
    def __init__(self, optimLimit):
        super(ScrapingThread, self).__init__()

    def run(self) -> None:
        start = timeit.timeit()
        self.batch = snTwitterWrapper(
            limit=self._optimLimit,
            searchTerms="ukraine", exactPhrase=None, hashtags=None,
            fromAccount=None,
            toDate=None, fromDate=None  # should be of the form: "YYYY-MM-DD"
        )
        end = timeit.timeit()

    def getDf(self):
        return self.batch


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--searchTerm", help="Term to search for", default="ukraine", type=str)
    parser.add_argument("-d", "--duration", help="Scraping duration in minutes", default=5, type=int)
    parser.add_argument("-l", "--limit", help="Parameter that gets optimized automatically if effectively defined",
                        default=200, type=int)
    parser.add_argument("-o", "--outputLocation", help="Output folder directory",
                        default=os.path.join(os.getcwd(), 'stored_data'))
    parser.add_argument("-ip", "--receiverIP", help="Receiver socket IP to use", type=str)
    parser.add_argument("-port", "--receiverPort", help="Receiver socket port to use", type=int)
    parser.add_argument("-c", "--clearDirectory", help="Clear the server's directory after the file has been sent",
                        action=argparse.BooleanOptionalAction)
    parser.add_argument("-spark", "--sparkStreaming", help="Clear the server's directory after the file has been sent",
                        action=argparse.BooleanOptionalAction)
    # clearDirectory does not have any effect if --ip flag has not been set
    args = parser.parse_args()

    if not args.searchTerm:
        print(f'-s or --searchTerm needs to be defined')

    if args.duration < 1:
        print(f'-d or --duration is required to be greater or equal than 2 (minutes)')
        sys.exit(0)

    t = MyTwitterScraper(
        dictOfSearchTerms=args.searchTerm,
        duration=args.duration,
        optimLimit=args.limit,
        outputLocation=args.outputLocation,
        useSocketIP=args.receiverIP,
        useSocketPort=args.receiverPort,
        clearDirectory=args.clearDirectory,
        forSparkSocket=args.sparkStreaming
    )

    try:
        t.triggerScheduler()
    # except BrokenPipeError:
    #     print(f' Lost connection to the receiverIP')
    # except ConnectionResetError:
    #     print(f'Receiver connection reset')
    # except KeyboardInterrupt:
    except:  # For whatever issue => just stop the script
        print(f' Stopping...')
        os._exit(0)
