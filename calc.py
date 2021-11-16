import os 

country_list = []

path = r'D:\code\hadoop-project\NBCorpus\Country'

for c_dir in os.listdir(path):
    country_list.append((c_dir,len(os.listdir(path+'\\'+c_dir))))

country_list = sorted(country_list,key= lambda x:x[1],reverse=True)

print(country_list)



