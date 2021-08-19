import plotly.graph_objs as go
import plotly
import json
from logger_class import Logger
import pandas as pd
from textblob import TextBlob

class Graphs:
    def __init__(self, dataframe):
        self.df = dataframe

    def sentiment_analysis(self):
        """
        Function returns the polarity and subjectivity of the comments from customers
        :return:
        """
        try:
            self.df[['polarity', 'subjectivity']] = self.df.comment.apply(lambda text: pd.Series(TextBlob(text).sentiment))
        except Exception as e:
            Logger('graphs.py').logger('ERROR', f'unable to process sentiment analysis of the review \n {str(e)}')

    def reveiw_sentiment_graph(self):
        """
        Function returns a scatter plot of the polarity vs subjectivity of the reviews
        :return:
        """
        self.sentiment_analysis()
        data = [go.Scatter(x=self.df.polarity, y=self.df.subjectivity)]
        graphs = [
            dict(data=[dict(x=self.df.polarity, y=self.df.subjectivity, type='scatter', mode='markers')],
                 layout=dict(title='Review Sentiment/Strength graph')),
            dict(data=[dict(x=self.df.rating, y=self.df.subjectivity, type='box')],
                 layout=dict(title='Review Sentiment Box plot'))
        ]

        ids = ['graph-{}'.format(i) for i, _ in enumerate(graphs)]
        graphJSON = json.dumps(graphs, cls=plotly.utils.PlotlyJSONEncoder)

        return graphJSON, ids
