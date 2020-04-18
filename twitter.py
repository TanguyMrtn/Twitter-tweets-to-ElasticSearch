# -*- coding: utf-8 -*-
"""
Created on Fri Apr 17 15:17:26 2020

@author: Tanguy
"""

import requests
import tweepy
import json

def createIndex(url,indexName):
    """
    Cette fonction permet de créer un index "indexName" dans elasticsearch à l'url "url"
    """
    
    request = url+"/"+indexName
    response = requests.put(request)
    print(response)

def createMapping(url,indexName, mapping = None):
    """
    Cette fonction permet de créer le mapping sur l'index "indexName" à l'url "url"
    """
    headers = {
    'Content-type': 'application/json',
    }
    request = url+"/"+indexName+"/_mapping"
    if mapping == None :    
        mapping = '{"properties":{"language":{"type":"keyword"},"text":{"type":"text"},"userName":{"type":"text"},"platform":{"type":"keyword"},"hashtags":{"type":"keyword"}}}'
    response = requests.put(request,headers=headers,data=mapping)
    print(response)

def addRecord(url,indexName,data):
    """
    Cette fonction permet d'ajouter un document "data" dans l'index "indexName" à l'url "url"
    """
    headers = {
    'Content-type': 'application/json',
    }
    request = url+"/"+indexName+"/_doc"
    response = requests.post(request,headers=headers,data=json.dumps(data))
    print(response)

class MyStreamListener(tweepy.StreamListener):
    
    def setIndexName(self,indexName):
        self.indexName = indexName
    
    def setUrl(self,url):
        self.url = url
    
    def on_status(self, status):
        """
        Pour chaque tweet du stream, on va récupérer la langue, le contenu et le nom de l'auteur puis on
        va ajouter le document dans elasticsearch
        """
        langage = status.lang
        text = status.text
        userName = status.user.name
        platform = status.source_url
        hashtags=[]
        for hashtagDoc in status.entities["hashtags"]:
            hashtag = hashtagDoc["text"].lower()
            hashtags.append(hashtag)
        doc={"langage":langage,"text":text,"userName":userName,"platform":platform,"hashtags":hashtags}
        addRecord(self.url,self.indexName,doc)

# Votre url elasticsearch
elasticUrl = "https://elastic:votreMdp@votreAdresse:9243"

# Tokens d'authentification
auth = tweepy.OAuthHandler("votreConsumerKey", "votreConsumerSecret")
auth.set_access_token("votreAccessToken", "votreAccessTokenSecret")

# On initialise notre stream listener, en y ajoutant l'index dans lequel on veut ajouter les documents
myListener = MyStreamListener()
myListener.setIndexName("twitter")
myListener.setUrl(elasticUrl)

# On initialise le stream
myStream = tweepy.Stream(auth = auth, listener=myListener)

# On créé l'index et on y associe le mapping

createIndex(elasticUrl,"twitter") # Vérifier via l'affichage d'un code 200 et dans dev tools avec : GET _cat/indices. Si index existe déjà, ne pas rerun.
createMapping(elasticUrl,"twitter") # Vérifier via l'affichage d'un code 200 et dans dev tools avec : GET twitter/_mapping. Si mapping existe déjà, ne pas rerun.


# On stream les tweets ayant un mot clé coronavirus et dans la langue française
myStream.filter(track=['coronavirus','covid','covid19'],languages=['en','fr']) # Vérifier via l'affichage d'un code 201 et dans dev tools avec GET twitter/_search









