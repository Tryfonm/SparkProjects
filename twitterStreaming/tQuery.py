from typing import Optional, List


class twitterQuery():

    def __init__(self):
        """
        """
        self.queryAsList = []
        self.query = ''

    def setQuery(
            self, searchTerms: Optional[str] = None,
            exactPhrase: Optional[str] = None,
            hashtags: Optional[str] = None,
            fromAccount: Optional[str] = None,
            toDate: Optional[str] = None,
            fromDate: Optional[str] = None,
    ) -> None:
        """

        Parameters
        ----------
        searchTerms
        exactPhrase
        hashtags
        fromAccount
        toDate
        fromDate

        Returns
        -------

        """

        if searchTerms:
            _searchTerms = []
            _searchTerms += [x + ' ' for x in searchTerms.split()]
            for term in _searchTerms:
                self.query += term

        if exactPhrase:
            _exactPhrase = ['"']
            _exactPhrase += [x + ' ' for x in exactPhrase.split()]
            for term in _exactPhrase:
                self.query += term

            self.query = self.query[:-1] + '" '

        if hashtags:
            _hashtags = ['(#']
            _hashtags += [x + ' ' for x in hashtags.split()]
            for termIndex, term in enumerate(_hashtags):

                if termIndex in (0, 1):
                    self.query += term
                    continue
                else:
                    self.query += 'OR #'
                self.query += term

            self.query = self.query[:-1] + ')'

        if fromAccount:
            _fromAccount = [' (from:']
            _fromAccount += [x for x in fromAccount.split()]
            for term in _fromAccount:
                self.query += term
            self.query += ')'

        if toDate:
            _toDate = [' until:']
            _toDate += [x for x in toDate.split()]
            for term in _toDate:
                self.query += term

        if fromDate:
            _fromDate = [' since:']
            _fromDate += [x for x in fromDate.split()]
            for term in _fromDate:
                self.query += term

    def getQuery(self):
        """

        Returns
        -------

        """
        return self.query


if __name__ == '__main__':
    testQuery = 'word1 word2 word3 "this is a phrase" (#hashtag1 OR #hashtag2 OR #hashtag3) (from:elonmusk) until:2022-01-01 since:2015-01-01'

    ts = twitterQuery()
    ts.setQuery(
        searchTerms="word1 word2 word3",
        exactPhrase="this is a phrase",
        hashtags="hashtag1 hashtag2 hashtag3",
        fromAccount="elonmusk", toDate="2022-01-01", fromDate="2015-01-01"
    )

    assert ts.getQuery() == testQuery
