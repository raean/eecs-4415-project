import socket
import sys
import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import API
from tweepy import Stream

# Setting up the serial keys and stuff:
consumer_key="skrtPlkUrqc3zqskPiup0Hl30"
consumer_secret="og2hCbNeWkhihoiLuIIJo1jY9qJcUPboHNVRO4FE6K0rTCqSLS"
access_token="785116862-zT7wQtAefZQe7RY2Ni1kODUGpfrk5rIjPPXpM2CK"
access_token_secret="xJRFeS1W40WCkRVdeqPTZaAwjKO266ndRxoaQP2R99Tnh"

# A listener that handles tweets received from the Twitter stream. 
# This listener prints tweets and then forwards them to a local port for processing in the spark app.
# ??? I'm not sure why we have to pass a StreamListener class to our Twitter app class but I know it's in the Tweepy Library.
class TweetListener(StreamListener): 

    # They are overriding the Tweepy method that takes in the data from Twitter.
    def on_data(self, data):
        try:
            global conn
            
            # Load the tweet JSON.
            full_tweet = json.loads(data)
            
            # Gets the text portion of the Tweet.
            tweet_text = full_tweet['text']

            # Print the tweet plus a separator.
            print ("------------------------------------------")
            print(tweet_text + '\n')

            # Send it to spark
            conn.send(str.encode(tweet_text + '\n'))
       
        except: # Exception handling...

            # Handle error.
            e = sys.exc_info()[0]
            print("Error: %s" % e)

        return True

    # Overring the error Tweepy method.
    def on_error(self, status):
        print(status)



# ==== Setup local connection ====

# IP and port of local machine or Docker:
TCP_IP = socket.gethostbyname(socket.gethostname()) # Returns local IP
TCP_PORT = 9009

# Setup local connection, expose socket, listen for spark app
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # ???
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) #???
s.bind((TCP_IP, TCP_PORT)) # Creates the socket s with the following assigned IP and PORT from Docker or local machine.
s.listen(1) # Listens for a connection!
print("Waiting for TCP connection...")

# If the connection is accepted then proceed...
conn, addr = s.accept()
print("Connected... Starting getting tweets.")


# ==== Setup twitter connection ====
listener = TweetListener() # Creates an instance of our Twitter app class.
auth = OAuthHandler(consumer_key, consumer_secret) # Creates an authentication object.
auth.set_access_token(access_token, access_token_secret) # Assigns the authentication object.
stream = Stream(auth, listener) # Passes both the approved authentication and our listener so Tweepy can do it's job.

# Setup search terms
track = ['#'] # ??? This seems to be an array of items we are searching for.
language = ['en'] # This is the lanuage we're looking for.
locations = [-130,-20,100,50] # This is the location rectangle we're looking at.
# We're basically looking at America.

# So, from my understanding when you call filter, it will also call on_data and so the program will run successfully.
try:
    # We also set our Tweet preferances here (in this case, it's track location and language.)
    stream.filter(track=track, languages=language, locations=locations)
except KeyboardInterrupt:
    s.shutdown(socket.SHUT_RD)

