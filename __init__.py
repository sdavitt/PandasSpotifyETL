# This is where we'll do our work

# Before we start doing things, we need to import our modules
import pandas as pd
import requests
import datetime
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
import os
from sqlalchemy.types import Integer, Text, DateTime

# Object Oriented Manner - working within a class
# Each of the steps of our process is going to be its own function

# load up our .env file
load_dotenv()

# set up our class
class Pandas_ETL_Pipeline:
    client_id = os.environ.get('SP_CLIENT_ID')
    client_secret = os.environ.get('SP_CLIENT_SECRET')
    database_url = os.environ.get('DATABASE_URL')

    def get_data(self):
        """
        helper function to make our api call
        """

        sp_api_call = spotipy.Spotify(auth_manager=SpotifyOAuth(
            client_id=self.client_id,
            client_secret=self.client_secret, 
            redirect_uri='http://localhost:3000/callback',
            scope='user-library-read user-read-recently-played'))
        
        return sp_api_call.current_user_recently_played()


    def artists_helper(self, artlist):
        """
        Takes in the full list of artists data for a song
        Returns the artists names as a string
        """
        ans = [x['name'] for x in artlist]
        return ', '.join(ans)

    # we're creating an ETL process - extracting data, transforming data, and loading data
    def extract(self): 
        """
        Get data from spotify API
        Place the sections we want in a Pandas dataframe
        """
        print('... Extracting ...')
        data = self.get_data()# make our api call

        #things I want for each song:
        # Song Name
        song_names = [x['track']['name'] for x in data['items']]
        # Artist Names
        artist_names = [self.artists_helper(x['track']['artists']) for x in data['items']] # complicated bc I can have multiple artist names in each one
        # Popularity
        popularity = [x['track']['popularity'] for x in data['items']]
        # Played At
        played_at = [x['played_at'] for x in data['items']]

        songs_dict = {
            'song_name': song_names,
            'artist_names': artist_names,
            'popularity': popularity,
            'played_at': played_at
        }

        songs_df = pd.DataFrame(songs_dict, columns=['song_name', 'artist_names', 'popularity', 'played_at'])
        print(songs_df)
        print('... Extraction Complete ...')
        return songs_df

    def popularity_categorize(self, pop_index):
        if pop_index < 25:
            return 'Unknown'
        elif pop_index < 50:
            return 'Low'
        elif pop_index < 75:
            return 'High'
        else:
            return 'Overplayed'

    def transform(self):
        """
        Change the numerical popularity index to a string describing the song's popularity
        Values: unknown, low, high, overplayed
        """
        songs = self.extract()
        print('... Checking Data ...')
        # checks for messed up data
        # checking if our data actually exists
        if songs.empty:
            print('This user has listened to no music. How bizarre.')
            return False
        # checking if our data has repeated values *SPECIFICALLY a repeated index number/primary key that might interfere with analysis
        if pd.Series(songs['played_at']).is_unique:
            pass
        else:
            raise Exception(f'[Error during Transformation]: Primary Keys duplcated - double check data extraction to ensure data is not corrupted. Duplicated data in played_at column')

        # checking for null values (we wouldn't expect any here)
        if songs.isnull().values.any():
            raise Exception('Null values found - improper data.')

        print('... Transforming ...')
        # Then we can make our transformation -> popularity index to description
        # Modifying a pandas dataframe using a UDF (user-defined function)
        songs['popularity_category'] = songs['popularity'].apply(self.popularity_categorize)
        
        print(songs)
        return songs

    def load(self):
        data = self.transform()
        print(type(data))
        try:
            if not data:
                print('... Issue with transform ...')
        except:
            print('... Data ready, proceed to load ...')
            print('... Loading ...')

            # uploading our dataframe into a sql database
            
            dburl = os.environ.get('DATABASE_URL')

            data.to_sql('Recently_Played_Popularity', index=False, con=dburl, if_exists='append', schema='public', chunksize=500, dtype={
                'song_name': Text,
                'artist_names': Text,
                'popularity': Integer,
                'played_at': DateTime,
                'popularity_category': Text
            })

            print('... Data load complete, your database is ready to use ...')

etl = Pandas_ETL_Pipeline() # create an instance of our ETL
etl.load()

