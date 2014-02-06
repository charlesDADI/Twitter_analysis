# -*- coding: utf-8 -*-
"""
Created on Mon Feb  3 11:04:04 2014

@author: root
"""
import pandas as pd 
import numpy as np
import twitter
import string, json, pprint
import urllib
from datetime import datetime
from time import *
import string, os, sys, subprocess, time
import MySQLdb
import re, string
from __future__ import division
import nltk, re, pprint
from urllib import urlopen
import re
import networkx as nx 


#function to clean a tweet
def cleaning_tweet(tweet_text): 
    tweet_text=tweet_text.upper()
    tweet_text=re.sub('[%s]' % re.escape(string.punctuation), '', tweet_text)   # delete punctuation
    tweet_text=filter(lambda x: x in string.printable, tweet_text)              # remove non ascii character
    return tweet_text
    

def extract_metadata(tweet):
        url=[]
        hashtag=[]
        result={}
        text=str()
        tweet.text = tweet.text+ ' '
        if tweet.text.find('http')!=-1:
            for m in re.finditer(r'(https?://\S+)', tweet.text):
                #print '%02d-%02d: %s' % (m.start(), m.end(), m.group(0))
                url=url+[m.group(0)]  # we build up an URL vector
                text=(tweet.text)[0:m.start()-1]+(tweet.text)[m.end()+1:len(tweet.text)] # we delete url in the tweet
            result['url']=str(",".join(url))
            result['tweet']=str(text)
        else: m
            result['url']=str(" ")
            result['tweet']=str(tweet.text)
        if tweet.text.find('#')!=-1:
            for h in re.finditer(r'(#\S+)', t.text):
                #print '%02d-%02d: %s' % (m.start(), m.end(), m.group(0))
                hashtag=hashtag+[h.group(0)]  # we build up an URL vector
                text=(result['tweet'])[0:h.start()-1]+(result['tweet'])[h.end()+1:len(result['tweet'])]    
            result['hashtags']=str(",".join(hashtag))
            result['tweet']=str(text)           
        else: 
            result['hashtags']=str(" ")
            result['tweet']=result['tweet']
        if len(get_rt_origins(tweet))>0:
            result['retweet']=str(",".join(get_rt_origins(tweet)))
            return(result)
        else: 
            result['retweet']=str(" ")
            return(result)

            
   
   
## Extracting retweet origins defined by '@' in a tweet.text
def get_rt_origins(tweet):
    # Regex adapted from
    # http://stackoverflow.com/questions/655903/python-regular-expression-for-retweets
    rt_patterns = re.compile(r"(RT|via)((?:\b\W*@\w+)+)", re.IGNORECASE)
    rt_origins = []
    # Inspect the tweet to see if it was produced with /statuses/retweet/:id
    # See http://dev.twitter.com/doc/post/statuses/retweet/:id
    if tweet.retweeted:
        if tweet.retweet_count > 0:
            rt_origins += [ tweet.user.name.lower() ]
            # Also, inspect the tweet for the presence of "legacy" retweet
            # patterns such as "RT" and "via".
    try:
        rt_origins += [
                        mention.strip()
                        for mention in rt_patterns.findall(tweet.text)[0][1].split()
                      ]
    except IndexError, e:
        pass
                # Filter out any duplicates
    return list(set([rto.strip("@").lower() for rto in rt_origins]))
              
                          



# connect to our database and create a cursor to do some work
#Setting up Twitter API
api = twitter.Api(
 consumer_key='*********',
 consumer_secret='KH74*******V9UYYrI',
 access_token_key='70711981*******bKX',
 access_token_secret='ReI*******74w'
 )


db=MySQLdb.connect(host="localhost",user="root", passwd="****", db="twitter")


keywords_list = ['EURUSD','tapering','VIX']

for tweet_keyword in keywords_list: # for each keyword, do some shit
        # our search for the current keyword
        search_results = api.GetSearch(term= tweet_keyword, lang='en', result_type='recent', count=100, max_id='')
        for t in search_results:
            cur=db.cursor()
            #we copy tweets in the temporary table
            transformation_tweet=None
            transformation_tweet=extract_metadata(t)
            cur.execute('insert into tweetsTemp (tweet_keyword,tweet,user,lang,geo,tweet_datetime,tweet_id,url,hashtags,retweet_count,user_description,user_friend,retweet_origin) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', (tweet_keyword,  cleaning_tweet(transformation_tweet['tweet']).encode('utf-8').replace("'","''").replace(';',''),t.user.GetName().encode('utf-8').replace("'","''").replace(';',''),t.lang.encode('utf-8').replace("'","''").replace(';',''),str(t.user.location).encode('utf-8').replace("'","''").replace(';',''),datetime.strptime(str(t.created_at),'%a %b %d %H:%M:%S +0000 %Y').strftime('%Y-%m-%d %H:%M:%S'),str(t.GetId()).encode('utf-8').replace("'","''").replace(';',''),transformation_tweet['url'].encode('utf-8').replace("'","''").replace(';',''),transformation_tweet['hashtags'].encode('utf-8').replace("'","''"),t.retweet_count,t.user.description.encode('utf-8').replace("'","''").replace(';',''),t.user.friends_count,transformation_tweet['retweet'].encode('utf-8').replace("'","''").replace(';','')))
            #we copy into the table only new tweets            
            cur.execute("""insert into tweets (tweet_keyword,tweet,user,lang,geo,tweet_datetime,tweet_id,url,hashtags,retweet_count,user_description,user_friend,retweet_origin) select tweet_keyword,tweet,user,lang,geo,tweet_datetime,tweet_id,url,hashtags,retweet_count,user_description,user_friend,retweet_origin from tweetsTemp where tweet_id NOT in (select distinct tweet_id from tweets)""")
            #we clear Temp table
            cur.execute('delete from tweetsTemp')
            cur.close ()
            db.commit ()
db.close ()





import sys
import json
import twitter
import networkx as nx


def create_rt_graph(tweets):
    g = nx.DiGraph()
    for tweet in tweets:
        rt_origins = get_rt_origins(tweet)
        if not rt_origins:
            continue
        for rt_origin in rt_origins:
            g.add_edge(rt_origin.encode('ascii', 'ignore'),
                       tweet.user.GetName().encode('ascii', 'ignore'),
                        {'tweet_id': tweet.id})
        
        return g

g = create_rt_graph(search_results)
print >> sys.stderr, "Number nodes:", g.number_of_nodes()
print >> sys.stderr, "Num edges:", g.number_of_edges()
print >> sys.stderr, "Num connected components:",
len(nx.connected_components(g.to_undirected()))
print >> sys.stderr, "Node degrees:", sorted(nx.degree(g))


#export graph in a DOT language file
def write_dot_output(g, out_file):
        try:
            nx.drawing.write_dot(g, out_file)
            print >> sys.stderr, 'Data file written to', out_file
        except ImportError, e:
            dot = ['"%s" -> "%s" [tweet_id=%s]' % (n1, n2, g[n1][n2]['tweet_id'])for (n1, n2) in g.edges()]
            f = open(out_file, 'w')
            f.write('''strict digraph {%s}''' % (';\n'.join(dot), ))
            f.close()
            print >> sys.stderr, 'Data file written to: %s' % f.name

# Visualizing a Graph of Retweet Relationships

OUT = 'twitter_retweet_graph'
# Build up a graph data structure.
g = create_rt_graph(search_results)
# Write Graphviz output.
if not os.path.isdir('out'):
    os.mkdir('out')
f = os.path.join(os.getcwd(), 'out', OUT)
write_dot_output(g, f)
print >> sys.stderr, \
'Try this on the DOT output: $ dot -Tpng -O%s %s.dot' % (f, f,)










#
#
#
#
#
#
##### Cleaning tweets
#tweet=array([])
#db=MySQLdb.connect(host="localhost",user="root", passwd="***", db="twitter")
#cur=db.cursor()
#cur.execute("SELECT tweet FROM tweets")  # WHERE tweet_keyword= tweet_keyword"
#rows = cur.fetchall()
#for row in rows:
#    tweet=np.concatenate((tweet,row))
#
#
#tweet=[]
#for x in range (0,((tweet.shape)[0])):
#    tweet[x]=cleaning_tweet(tweet[x])
#    
#
#
### envoyer un tweet
#status = api.PostUpdate('I love python-twitter!')
#print status.text
#
#GetFollowerIDs(self, user_id=None, screen_name=None, cursor=-1, stringify_ids=False, count=None, total_count=None)
#
#
