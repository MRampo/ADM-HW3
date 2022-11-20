
# This is the function that handles the collections of the links of places (ONLY the links, not the html page itself)
# As we were advised in the delivery of the homework, the function was designed to be resumed 
# even in case of premature termination.
# This simply means that the only thing that will have to be passed to the function are the starting and ending points


import csv
import datetime
import glob
import os

#module used to save dictionary files
import pickle

import time
from bs4 import BeautifulSoup as bs
import pandas as pd
import requests as rq
from tqdm import tqdm 

#The modules used to clean the text
import nltk
from nltk.corpus import stopwords
nltk.download('stopwords')
from nltk.tokenize import RegexpTokenizer
from nltk.stem import PorterStemmer
from nltk import tokenize


def getLinks(start: int, finish: int) -> list:
    total_link = []
    s = rq.Session()
    for n in tqdm(range(start,finish)):
        url = f'https://www.atlasobscura.com/places?page={n}&sort=likes_count'
        result = s.get(url)
        soup = bs(result.text)
        puf = soup.find_all("a", {'class': 'content-card content-card-place'})
        for x in puf:
            total_link.append(x['href'])
    return total_link



#This is the function used to download the actual html web page from the list of url that we extracted with the
#previous function.
#In input it will have the starting point and the ending point and the array of string rappresenting the path of each
#page to download.

def downloadPage(start: int,end: int, array: list) -> None:
    #these variables are used to name the pages and the folder correctly
    count_link = ((start-1)*18)+1
    count_page = start
    
    parent_dir = f'./all_Pages'

    #We open a session that will be closed after the 
    s = rq.Session()
    header = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    for x in tqdm(range(start, end)):
        #refresh of the session, just in case it gets cut
        if count_link%10 == 0:
            s = rq.Session()
        
        #If count_link%18 == 1, then it means that i have reached a new pages, and i need to create a folder for it
        if count_link%18 == 1:
            path = os.path.join(parent_dir, f"folder_{count_page}")
            os.mkdir(path)

        #for every link in every pages
        for y in range(18):
            #I build the right url using the paths contained in "array"
            url = f'https://www.atlasobscura.com{array[count_link-1]}'

            #The name of the file and the name of the folder, used to write the html file in the right place
            name_file = f'location_{count_link}'
            name_folder = f'folder_{count_page}'

            # This create the actual file
            with open(f'./all_Pages/{name_folder}/{name_file}.html', 'w', encoding='utf8') as fp:
                req = s.get(url, headers = header)
                fp.write(req.text)

                #If the request code is different than 200 then it means i have an error,
                #  so i go to sleep for 2 min and then start again
                if req.status_code != 200:
                     time.sleep(120)
                     req = s.get(url, headers = header)
                fp.write(s.get(url).text)

            #Used to keep count of the pages and columns
            if count_link%18 == 0:
                count_page += 1
            count_link += 1

# This is the function used to create the TSV files from the .html files that we downloaded from the previous functions
# We decided to create this "mother" function  that encapsulates every other function, 
# one for each attribute that we want to find from the html pages
def download_TSV():
    os.mkdir("TSV Files")
    a = 0
    for path in tqdm(glob.glob(r"all_Pages/*/*")):
        with open(path, encoding='utf8') as f:
                    a += 1
                    p = f.read()
                    soup =  bs(p)
                    pageAttribute = []
                    placeName = findPlaceName(soup)
                    placeTags = findPlaceTags(soup)
                    placeTags = ",".join(placeTags)
                    numPeopleVisited = findNumPeopleVisited(soup)
                    numPeopleWant = findNumPeopleWant(soup)
                    placeDesc = findDescription(soup)
                    placeDesc = " ".join(placeDesc)
                    placeShortDesc = findShortDescription(soup)
                    placeNearby = findNearbyPlaces(soup)
                    placeNearby = ",".join(placeNearby)
                    placeAddress = findAddress(soup)
                    placeAlt, placeLong = findCordinates(soup)
                    placeEditors = findPostEditors(soup)
                    placeEditors = ",".join(placeEditors)
                    placePubDate = findPublishingDate(soup)
                    placeRelatedList = findPlaceNear(soup)
                    placeRelatedList = ",".join(placeRelatedList)
                    placeRelatedPlaces = findRelatedPlaces(soup)
                    placeRelatedPlaces = ",".join(placeRelatedPlaces)
                    placeURL = findPageURL(soup)
                    with open(f'./TSV Files/{a}.tvs', 'wt', encoding='utf8') as fp:
                        csv.writer(fp, delimiter='\t').writerow([placeName, placeTags, numPeopleVisited, numPeopleWant, placeDesc, placeShortDesc, placeNearby, placeAddress, placeAlt, placeLong, placeEditors, placePubDate, placeRelatedList, placeRelatedPlaces, placeURL])

#The function used to extract placeName
def findPlaceName(soup) -> str:
    placeName = soup.find("h1", {"class": "DDPage__header-title"})
    if placeName != None:
        placeName = placeName.text
    #placeName = re.sub('[A-Za-z0-9_.,! "]*' ,'',placeName)
    return placeName

#The function used to extract placeTags
def findPlaceTags(soup) -> list:
    tags = []
    placeTags = soup.find_all("a", {"class": "itemTags__link js-item-tags-link"})
    for tag in placeTags:
        t = tag.text.replace("\n", "")
        #t = re.sub('[A-Za-z0-9 _.,!"]*','',t)
        tags.append(t)
    return tags

#The function used to extract numPeopleVisited
def findNumPeopleVisited(soup) -> int:
    peopleVisited = soup.find_all("div", {"class": "title-md item-action-count"})
    if len(peopleVisited) > 0:
        peopleVisited = int(peopleVisited[0].text)
    return peopleVisited

#The function used to extract numPeopleWant
def findNumPeopleWant(soup) -> int:
    peopleVisited = soup.find_all("div", {"class": "title-md item-action-count"})
    if len(peopleVisited) > 0:
        peopleVisited = int(peopleVisited[1].text)
    return peopleVisited

#The function used to extract findDescription
def findDescription(soup) -> list:
    all_description = []
    descriptions = soup.find_all("div", {"class": "DDP__body-copy"})
    for description in descriptions:
        d = description.text.replace("\n","")
        all_description.append(d)
    return all_description

#The function used to extract shortDescription
def findShortDescription(soup) -> int:
    shortDescription = soup.find("h3", {"class": "DDPage__header-dek"})
    if shortDescription != None:
        shortDescription = shortDescription.text.replace("\n", "")
    return shortDescription

#The function used to extract NearbyPlaces
def findNearbyPlaces(soup) -> set:
    nearPlaces = []
    nearbyPlaces = soup.find_all("div", {"class": "DDPageSiderailRecirc__item-title"})
    if nearbyPlaces != None:
        for place in nearbyPlaces:
            p = place.text.replace("\n","")
            nearPlaces.append(p)
        #Convert the list to set, and then back again to list to remove repetition
    return set(nearPlaces)

#The function used to extract the placeAdress
def findAddress(soup) -> str:
    strings = []
    adress_strings = soup.find("address", {"class": "DDPageSiderail__address"})
    if adress_strings != None:
        adress_strings = adress_strings.find("div")
        for info in adress_strings:
            s = info.text.replace("\n", "")
            if s != "":
                strings.append(s)
        if len(strings) > 3:
            return " ".join(strings[:3])
    return ""

#The function used to extract the cordinates
def findCordinates(soup) :
    cordinates = soup.find("div", {"class":"DDPageSiderail__coordinates js-copy-coordinates"})
    if cordinates != None:
        return cordinates.text.replace("\n", "").replace(" ","").split(",")
    return "", ""

#The function used to extract the postEditors, it returns a list
def findPostEditors(soup) -> list:
    all_editors =[]
    editors = soup.find_all("a", {"class":"DDPContributorsList__contributor"})         
    for person in editors:
        s = person.text.replace("\n", "")
        all_editors.append(s)
    return all_editors

#The function used to extract the publishingDate
def findPublishingDate(soup):
    #Pick the right info
    dateString = soup.find("div", {"class":"DDPContributor__name"}) 
    #Let's clean the string
    if dateString != None:
        s = dateString.text.replace("\n", "")
        #Let's modify it for the right format of datetime
        split = s.split()
        #Let's convert the string Month into the corrispondent number by using "strptime()" 
        split[0] = str(datetime.datetime.strptime(split[0], '%B').month)
        #My format
        format = "%m %d, %Y"
        #Convert from String to datetime
        date = datetime.datetime.strptime(" ".join(split), format)
        return date
    else:
        return ""

#The function used to extract the nearPlaces
def findPlaceNear(soup) -> list:
    lists =[]
    relatedLists = soup.find("div", {"data-gtm-template":"DDP Footer Recirc Nearby"})
    if relatedLists != None:
        relatedLists = relatedLists.find_all("h3", {"class":"Card__heading --content-card-v2-title js-title-content"})
        for list in relatedLists:
            s = list.text.replace("\n", "")
            #s = re.sub('[A-Za-z0-9 _.,!"]*','',s)
            lists.append(s)
    return lists

#The function used to extract relatedPlaces
def findRelatedPlaces(soup) -> list:
    lists =[]
    relatedLists = soup.find("div", {"data-gtm-template":"DDP Footer Recirc Related"})
    if relatedLists != None:
        relatedLists = relatedLists.find_all("h3", {"class":"Card__heading --content-card-v2-title js-title-content"})
        for list in relatedLists:
            s = list.text.replace("\n", "")
            #s = re.sub('[A-Za-z0-9 _.,!"]*','',s)
            lists.append(s)
    return lists

#The function used to extract the page URL
def findPageURL(soup):
    numVisitedPeople = soup.find("link", {"rel":"canonical"})
    return numVisitedPeople['href']

#This is the function used to merge all the .tsv files together in a single pandas dataframe
def load_tsv() -> pd.DataFrame:
    tsv = []
    dtypes = {}
    for x in tqdm(os.listdir("TSV Files")):
        df = pd.read_csv(f'TSV Files/{x}',
            sep="\t",
            header=None,
            names=["placeName", "placeTags", "numPeopleVisited", "numPeopleWant", "placeDesc", "placeShortDesc", "placeNearby","placeAdress", "placeAlt", "placeLong", "placeEditors","placePubDate", "placeRelatedList", "placeRelatedPlace", "placeURL"])
        tsv.append(df)
    return pd.concat(tsv)

def remove_punctuations(string) -> list:
    # first we remove the punctuations
    # in order to do it we need to tekenize the string with the function tokenize and then applying the function RegexpTokenizer
    return RegexpTokenizer(r'\w+').tokenize(string)


def stemming(string) -> list:
    # now we move forward with the stemming
    porter = PorterStemmer()
    string_stem=[porter.stem(word) for word in string]
    # we can now return the cleaned string 
    return string_stem

def remove_stopwords(string) -> list:
    # after this we can now remove all the stopwords in each word in string_t
    return  [word for word in string if not word.lower() in set(stopwords.words("english"))]
    # now we move forward with the stemming

def remove_numbers(string) -> list:
    for num in range(len(string)):
        if any(chr.isdigit() for chr in string[num]):
            string[num] = ''.join((x for x in string[num] if not x.isdigit()))
    return string



def cleaning(string) -> list:
    #I apply all the function for cleaning the string
    string = string.lower()
    string = remove_punctuations(string)
    string = remove_stopwords(string)
    string = remove_numbers(string)
    string = stemming(string)
    #return a list containing all the words of the original string after the cleaning
    return string

#Function used to save the dictionary
def save_dic(dic, name):
    with open(f'./Dictionary/{name}.pkl', 'wb') as f:
        pickle.dump(dic, f)

#Function used to read the previosly saved dictionary
def read_dic(name):
    with open(f'./Dictionary/{name}.pkl', 'rb') as f:
        loaded_dict = pickle.load(f)
        return loaded_dict