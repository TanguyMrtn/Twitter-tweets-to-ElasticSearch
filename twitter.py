# -*- coding: utf-8 -*-
"""
Created on Fri Apr 17 15:17:26 2020

@author: Tanguy
"""

import requests
import tweepy
import json

def createIndex(indexName):
    """
    Cette fonction permet de créer un index "indexName" dans elasticsearch
    """
    
    request = url+"/"+indexName
    response = requests.put(request)
    print(response)

def createMapping(indexName, maping = None):
    """
    Cette fonction permet de créer le mapping sur l'index "indexName"
    """
    headers = {
    'Content-type': 'application/json',
    }
    request = url+"/"+indexName+"/_mapping"
    if maping == None :    
        maping = '{"properties":{"language":{"type":"text"},"text":{"type":"text"},"userName":{"type":"text"}}}'
    response = requests.put(request,headers=headers,data=maping)
    print(response)

def addRecord(indexName,data):
    """
    Cette fonction permet d'ajouter un document "data" dans l'index "indexName"
    """
    headers = {
    'Content-type': 'application/json',
    }
    request = url+"/"+indexName+"/_doc"
    response = requests.post(request,headers=headers,data=json.dumps(data))
    print(response)

class MyStreamListener(tweepy.StreamListener):
    
    def on_status(self, status):
        """
        Pour chaque tweet du stream, on va récupérer la langue, le contenu et le nom de l'auteur puis on
        va ajouter le document dans elasticsearch
        """
        langage = status.lang
        text = status.text
        userName = status.user.name
        doc={"langage":langage,"text":text,"userName":userName}
        addRecord("twitter",doc)



# Votre url elasticsearch
url = "https://elastic:votreMdp@votreAdresse:9243"

# Tokens d'authentification
auth = tweepy.OAuthHandler("votreConsumerKey", "votreConsumerSecret")
auth.set_access_token("votreAccessToken", "votreAccessTokenSecret")

# On initialise le stream
myStream = tweepy.Stream(auth = auth, listener=MyStreamListener())

# On créé l'index et on y associe le mapping

createIndex("twitter") # Vérifier via l'affichage d'un code 200 et dans dev tools avec : GET _cat/indices. Si index existe déjà, ne pas rerun.
createMapping("twitter") # Vérifier via l'affichage d'un code 200 et dans dev tools avec : GET twitter/_mapping. Si mapping existe déjà, ne pas rerun.

# On stream les tweets ayant un mot clé coronavirus et dans la langue française
myStream.filter(track=['coronavirus'],languages=['fr']) # Vérifier via l'affichage d'un code 201 et dans dev tools avec GET twitter/_search











