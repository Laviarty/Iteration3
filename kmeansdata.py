from sqlalchemy import create_engine
import pandas as pd

#Verbindung zu Datenbank
engine = create_engine('postgresql://postgres:postgres@localhost:5432/Testdatenbank')
con = engine.connect()

#Abfrage an die Datenbank:

#1. Erstellt eines Arrays, welche die Anzahl der #Hashtags Pro Tag angibt
exc = con.execute("SELECT date,text FROM public.hashtag")
rawdata = pd.DataFrame()
for row in exc:
        rowframe = pd.DataFrame([row])
        rawdata = rawdata.append(rowframe, ignore_index= True)
rawdata.columns=['date','text']

rawdata = rawdata.sort_values(['date'])
rawdata = rawdata.reset_index(drop=True)


exc = con.execute("SELECT DISTINCT date FROM public.hashtag")
disdate = pd.DataFrame()
for row in exc:
        rowframe = pd.DataFrame([row])
        disdate = disdate.append(rowframe, ignore_index= True)
disdate.columns=['date']
disdate = disdate.sort_values(['date'])
disdate = disdate.reset_index(drop=True)

exc = con.execute("SELECT DISTINCT text FROM public.hashtag")
distext = pd.DataFrame()
for row in exc:
        rowframe = pd.DataFrame([row])
        distext = distext.append(rowframe, ignore_index= True)
distext.columns=['text']
#distext = distext.sort_values(['text'])
#distext = distext.reset_index(drop=True)


#print(rawdata.to_string())
date = pd.DataFrame()   
myindex = disdate.index.tolist()    
n=0
rawindex = int(rawdata['date'].count()-1)
disindex= int(disdate['date'].count()-1)
while n <= rawindex:
    
    i=0
    while i <= disindex:
        
        if rawdata.loc[n,'date'] == disdate.loc[i,'date']:#rawdata['date'].value[n] == disdate['date'].value[i]:
            date = date.append([myindex[i]] ,ignore_index=True)
            i= int(disdate['date'].count())
            #print(n, "if case")
        else:
            i+=1
            #print(n, "elsecase")
    n+=1    
date.columns=['date']
rawdata['date'] = date['date']



#print(rawdata.to_string())
text = pd.DataFrame()   
myindex = distext.index.tolist()    
n=0
rawindex = int(rawdata['text'].count()-1)
disindex= int(distext['text'].count()-1)
while n <= rawindex:
    
    i=0
    while i <= disindex:
        
        if rawdata.loc[n,'text'] == distext.loc[i,'text']:#rawdata['date'].value[n] == disdate['date'].value[i]:
            text = text.append([myindex[i]] ,ignore_index=True)
            i= int(distext['text'].count())
            #print(n, "if case")
        else:
            i+=1
            #print(n, "elsecase")
    n+=1    
text.columns=['text']
rawdata['text'] = text['text']
#rawdata['text'] = rawdata.index.values
rawdata = rawdata.drop_duplicates(subset=None, keep='first',inplace=False)

rawdata = rawdata.sort_values(['date','text'])
rawdata = rawdata.reset_index(drop=True)

rawdata.to_csv('kmeans.csv', sep=';')

#print(rawdata.to_string())
print("construction donne")