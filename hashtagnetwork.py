import pandas as pd
from sqlalchemy import create_engine


#Mit Datenbank verbinden:
engine = create_engine('postgresql://postgres:postgres@localhost:5432/Testdatenbank')

con = engine.connect()
#Eingabe:
#hashtag = input("Hashtag eingeben:")

#erstellen des speziellen Hashtag Tables:
exc = con.execute("SELECT index, id, text FROM public.hashtag")
rawdata = pd.DataFrame()
for row in exc:
        rowframe = pd.DataFrame([row])
        rawdata = rawdata.append(rowframe, ignore_index= True)


rawdata.columns = ['index', 'id','text']
rawdata.to_csv('rawdata.csv', sep=';')
#Knotentabelle
nodes = rawdata.drop_duplicates(subset=['text'], keep='first',inplace=False)
nodes = nodes.reset_index(drop=True)


#Kantentabelle:
nodeoccurence = pd.DataFrame() #nodeoccurence.columns = ['text', 'id','index']
alledges = pd.DataFrame()
finalframe = pd.DataFrame()
source = pd.DataFrame() #columns=['source']
target = pd.DataFrame() #columns=['target']
label = pd.DataFrame() #columns=['label']
weight = pd.DataFrame() #columns=['weight']
edgescount  = pd.DataFrame() #columns=['source','target','label','weight']

nodeindex= nodes['id'].count()-1 #längenodes tabelle
rawindex= rawdata['id'].count()-1 #länge rawdata tabelle


sorteddata = rawdata.sort_values(['text'])


i= 0
while i<=nodeindex : #für jeden Knotes in nodes

    j=0
    while j<= rawindex: #nodeoccurence aufbauen
        if nodes.loc[i,'id'] == sorteddata.loc[j,'id']:
            k=j
            while nodes.loc[i,'text'] == sorteddata.loc[k,'text']:
                nodeoccurence= nodeoccurence.append(sorteddata.loc[k,['text','id', 'index']], ignore_index=True)
                k +=1
        j += 1

    occindex = nodeoccurence['id'].count()-1 #länge von nodeoccurnece
    
    n=0
    while n <=occindex : #frame erstellen, der alle gemeinsamen Vorkommen mit dem Knoten hat

        indexnr = int(nodeoccurence['index'].values[n])
        
        if (indexnr+1 < rawindex and rawdata['id'].values[indexnr] == rawdata['id'].values[indexnr+1]) or (indexnr-1 > 0 and rawdata['id'].values[int(indexnr)] == rawdata['id'].values[int(indexnr-1)]):
            m=1
            while indexnr+m < rawindex and rawdata['id'].values[indexnr] == rawdata['id'].values[indexnr+m]:
                """finalframe = finalframe.append(rawdata.loc[indexnr+m,['index', 'id','text']])
                source = source.append(rawdata.loc[indexnr,['id']])
                target = target.append(rawdata.loc[indexnr+m,['id']])"""
                finalframe = finalframe.append(rawdata.loc[indexnr+m,['index', 'id','text']], ignore_index=True) #benötigt um gewicht zu ermitteln
                
                sourcename = rawdata['text'].values[indexnr]
                targetname = rawdata['text'].values[indexnr+m]
                
                k=0
                while k <= int(nodes['id'].count()-1):
                    if sourcename == nodes['text'].values[k]:
                        source = source.append(nodes.loc[k,['index']], ignore_index=True)
                        k+=1
                    else:
                        k+=1
                l = 0
                while l <= int(nodes['id'].count()-1):
                    if targetname == nodes['text'].values[l]:
                        target = target.append(nodes.loc[l,['index']], ignore_index=True)
                        l+=1
                    else:
                        l+=1
                        
                labelname = str(rawdata.loc[indexnr,['text']].values + rawdata.loc[indexnr+m,['text']].values)
                label = label.append([labelname], ignore_index=True)
                m +=1
                
  
            m=1
            while indexnr-m > 0 and rawdata['id'].values[int(indexnr)] == rawdata['id'].values[int(indexnr-m)]:
                finalframe = finalframe.append(rawdata.loc[indexnr-m,['index', 'id','text']], ignore_index=True) #benötigt um gewicht zu ermitteln
                source = source.append(rawdata.loc[indexnr,['index']], ignore_index=True)
                target = target.append(rawdata.loc[indexnr-m,['index']], ignore_index=True)
                labelname = str(rawdata.loc[indexnr,['text']].values + rawdata.loc[indexnr-m,['text']].values)
                label = label.append([labelname], ignore_index=True)
                m +=1
        
        
            edgescount = pd.concat([source,target,label],axis=1,ignore_index=True)#speichern aller Ermittelten Werte #HIER STAND AUCH weight in concat
            alledges = alledges.append(edgescount, ignore_index=True)

            #Löschen von Temporären Tabels:
            finalframe = pd.DataFrame()
            source= pd.DataFrame() #columns=source.columns
            target= pd.DataFrame() #columns=target.columns
            label= pd.DataFrame() #columns=label.columns

            n +=1
        else:
            n += 1
    #Löschen der Temporären Tabels:
    nodeoccurence= pd.DataFrame() #columns=nodeoccurence.columns
    

    i +=1
    """print(i)
    if i== 429:
        print("EEEENDEEEE")"""

alledges.columns = ['source', 'target', 'label']
weights = alledges.groupby(['label']).size()
weight = pd.DataFrame(weights, columns=['weights'])
weight=weight.reset_index(drop=True)
alledges = pd.concat([alledges,weight],axis=1,ignore_index=True)
#Ausgabe:

nodes.columns = ['id','tweetid','text']
alledges.columns = ['source', 'target', 'label', 'weight']
alledges.to_csv('alledges.csv', sep=';', float_format='%.f')
nodes.to_csv('allnodes.csv', sep=';')