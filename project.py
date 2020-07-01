import pymongo

#No1

myclient= pymongo.MongoClient("mongodb://localhost:27017/")
mydb= myclient ["test"]
mycol=mydb["solarPower1"]


#태양에너지 발전data를 기상데이터와 조합할 수 있도록 가공(시간 데이터에 맞춰서)
#태양에너지 발전소 중 '영암발전소' 데이터만을 추출

aggr1=[
{'$project':{'date':'$날짜','place' : '$발전기명', 'time' :[['01','$_01시'],['02','$_02시'],['03','$_03시'],
                                                      ['04','$_04시'],['05','$_05시'],['06','$_06시'],['07','$_07시'],
                                                      ['08','$_08시'],['09','$_09시'],['10','$_10시'],
                                                      ['11','$_11시'],['12','$_12시'],['13','$_13시'],
                                                      ['14','$_14시'],['15','$_15시'],['16','$_16시'],
                                                      ['17','$_17시'],['18','$_18시'],['19','$_19시'],
                                                      ['20','$_20시'],['21','$_21시'],['22','$_22시'],
                                                      ['23','$_23시'],['24','$_24시']]}},
{'$unwind' : '$time'},
{'$project':{'date':'$date','place' : '$place',
                 'time' : {'$arrayElemAt' :["$time",0]},
                 'energy' : {'$arrayElemAt' :["$time",1]}}},
{ '$project' : {'date':{'$concat' : ['$date',' ','$time',':00']} , 'place':'$place',  'energy' :'$energy'}},
{'$match' : {'place' : '영암에프원태양광b'}},
{'$project' : {'_id' : 0,'place':0}},
{'$out' : 'solarPower'}]

# Store in SolarPower (DB)
# 앞서 가공한 데이터들을 SolarPower라는 이름의 collection으로 저장

mycol.aggregate(aggr1)


# 세개 파일로 분할 되어있던 2017.1.1~2019.6.30. 의 데이터들을 하나의 collection에 병합
mycol3 = mydb['mokpo2017']
aggr3 = [
    { '$project': {'_id':0}},
    {'$out' : 'weather'}
]
mycol3.aggregate(aggr3)

mycol4 = mydb['mokpo2018']
aggr4=[
    { '$project': {'_id':0}},
    {'$merge':{'into':'weather'}}
]
mycol4.aggregate(aggr4)

mycol5 = mydb['mokpo2019']
aggr5=[
    { '$project': {'_id':0}},
    {'$merge':{'into':'weather'}}
]
mycol5.aggregate(aggr5)


#기상데이터와 태양에너지발전소데이터를 '날짜별 시간'을 기준으로 결합
mycol1=mydb["weather"]


aggr2=[
{
      '$lookup': {
         'from': 'solarPower',
         'localField':'일시',
         'foreignField': 'date',
         'as': "fromItems"
      }
   },
   {
      '$replaceRoot': { 'newRoot': { '$mergeObjects': [ { '$arrayElemAt': [ "$fromItems", 0 ] }, "$$ROOT" ] } }
   },
   { '$project': { '_id' : 0,'fromItems': 0 ,'일시' : 0} },
{'$out': 'projectData'}
]

# Store in projectData (DB)
# 앞서 병합한 데이터들을 projectData 라는 collection으로 저장
mycol1.aggregate(aggr2)



#앞서 저장한 collection의 데이터 중 태양열 발전소에서 전력을 생산하는 유효시간을 판단 후, 유효시간 외의 데이터들을 삭제
mycol2 = mydb["projectData"]

remover1={'$or' : [{'date' : {'$regex' : '00:00$'}},
              {'date' : {'$regex' : '01:00$'}},
              {'date' : {'$regex' : '02:00$'}},
              {'date' : {'$regex' : '03:00$'}},
              {'date' : {'$regex' : '04:00$'}},
              {'date' : {'$regex' : '05:00$'}},
              {'date' : {'$regex' : '21:00$'}},
              {'date' : {'$regex' : '22:00$'}},
              {'date' : {'$regex' : '23:00$'}},
              {'date' : {'$exists' : False}}
              ]
     }

mycol2.delete_many(remover1)

