import pymongo

myclient= pymongo.MongoClient("mongodb://localhost:27017/")
mydb= myclient ["test"]
mycol=mydb["projectData"]


#이상치 데이터 제거
#이상치 데이터 제거한 상황을 반영하여 그래프를 그려 이상치를 판단한 후 이상치를 지우고 코드를 수정
aggr=[{'$sort':{'date':1}}
       ,{'$match' : {'$and' : [{'$and':[{'일조(hr)':{'$ne':''}},{'일조(hr)':{'$ne':0}}]},
                                {'$and':[{'일사(MJ/m2)':{'$ne':''}},{'일사(MJ/m2)':{'$ne':0}}]},
                                {'$and':[{'전운량(10분위)':{'$ne':''}},{'전운량(10분위)':{'$ne':0}}]}
                               ]}
         }
       ]

#이상치 데이터 제거전
#이상치 데이터 제거전의 상황을 반영하여 그래프 작성
#aggr=[{'$sort':{'date':1}}]


#지면온도
g_temp=[]
#온도
temper=[]
#전운량
cloud=[]
#운형
cloudshape=[]
#발전량
energy=[]
#일조량
sunn=[]
#일사량
sunn1=[]

#label_data
label_data =[]


#projectData콜렉션의 data 불러오기
for i in mycol.aggregate(aggr):
    tmp=[]
    if i['일조(hr)']!="":
        sunn.append(i['일조(hr)'])
        tmp.append(i['일조(hr)'])
    else:
        sunn.append(0)
        tmp.append(0)
    if i['일사(MJ/m2)']!="":
        sunn1.append(i['일사(MJ/m2)'])
        tmp.append(i['일사(MJ/m2)'])
    else:
        sunn1.append(0)
        tmp.append(0)

    if i['지면온도(°C)'] != "":
        g_temp.append(i['지면온도(°C)'])
        tmp.append(i['지면온도(°C)'])
    else:
        g_temp.append(0.0)
        tmp.append(0.0)

    if i['기온(°C)'] != "":
        temper.append(i['기온(°C)'])
        tmp.append(i['기온(°C)'])
    else:
        temper.append(0.0)
        tmp.append(0.0)

    if i['전운량(10분위)'] != "":
        cloud.append(i['전운량(10분위)'])
        tmp.append(i['전운량(10분위)'])
    else:
        cloud.append(0)
        tmp.append(0)

    if i['운형(운형약어)'] != "":
        cloudshape.append(i['운형(운형약어)'])

    label_data.append(tmp)

    energy.append(i['energy'])



import numpy as np
import statistics as st


print("-----평균-------")

print('지면온도(°C)',np.mean(g_temp))
print('기온(°C)',np.mean(temper))
print('전운량(10분위)',np.mean(cloud))
print('일사(MJ/m2)',np.mean(sunn1))
print('일조(hr)',np.mean(sunn))
print('energy',np.mean(energy))

print("\n\n\n-----중앙값(Median)------")

print('지면온도(°C)',np.median(g_temp))
print('기온(°C)',np.median(temper))
print('전운량(10분위)',np.median(cloud))
print('일사(MJ/m2)',np.mean(sunn1))
print('일조(hr)',np.mean(sunn))
print('energy',np.median(energy))

print("\n\n\n\n----최빈값(Mode)----")

print('전운량(10분위)',st.mode(cloud))
print('운형(운형약어)',st.mode(cloudshape))

import matplotlib.pyplot as plt



#기온에 대한 히스토그램
plt.hist(temper,50)
plt.xlabel('B_temperature')
plt.show()
#지면온도에 대한 히스토그램
plt.clf()
plt.hist(g_temp,50)
plt.xlabel('B_ground temperature')
plt.show()
#전운량에 대한 히스토그램
plt.clf()
plt.hist(cloud,10)
plt.xlabel('B_cloud')
plt.show()

#일사량에 대한 히스토그램
plt.clf()
plt.hist(sunn1,20)
plt.xlabel('B_Solar radiation')
plt.show()

#일조량에 대한 히스토그램
plt.clf()
plt.hist(sunn,6)
plt.xlabel('B_Sunlight')
plt.show()

#발전량에 대한 히스토그램
plt.clf()
plt.hist(energy,20)
plt.xlabel('B_energy')
plt.show()

#온도 boxplot
plt.clf()
plt.boxplot(temper)
plt.xlabel('B_temperature')
plt.show()

#지면온도 boxplot
plt.clf()
plt.boxplot(g_temp)
plt.xlabel('B_ground temperature')
plt.show()

#전운량 boxplot
plt.clf()
plt.boxplot(cloud)
plt.xlabel('B_cloud')
plt.show()

#일사량 boxplot
plt.clf()
plt.boxplot(sunn1)
plt.xlabel('B_Solar radiation')
plt.show()

#일조량 boxplot
plt.clf()
plt.boxplot(sunn)
plt.xlabel('B_Sunlight')
plt.show()

#태양에너지 boxplot
plt.clf()
plt.boxplot(energy)
plt.xlabel('B_energy')
plt.show()


target_data = energy



# 목표값을 독립변수가 태양열발전소의 발전량
# 라벨값: 종속변수 (연속된 값을 가지는 데이터: 지면온도, 기온, 일사량, 일조량, 전운량 을 이용함)
# 독립변수가 연속된 실수를 가지므로 회귀를 선택하였고, 그 중 선형회귀를 사용함

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression


X_train, X_test, y_train, y_test = train_test_split(label_data, target_data, test_size=0.2, random_state=0)

regression=LinearRegression()
regression.fit(X_train,y_train)

guess=regression.predict(X_test)

from sklearn import metrics

#mean squared error 값 출력
print("mean squared error",metrics.mean_squared_error(y_test, guess))

#선형회귀 그래프 그리기 예측값- 실제결과값을 반영하여 분포 그래프를 그림

plt.clf()
plt.scatter(y_test,guess)
plt.xlabel("Linear Regression")

plt.show()

#결론: 에러율이 이상치를 제거했을때보다 더 높았지만 선형 그래프가 정비례하는 것을 보아, 종속변수들은 독립변수와 관계가 크다.

#Mean Squared Error 가 큰 이유: 애초에 태양열에너지 변수의 값들이 굉장히 크기에 , 이 값은 제곱계산이 들어갔으므로 클 수 밖에 없다.