import os 
from shutil import copyfile

country_list = []

path = r'D:\code\hadoop-project\NBCorpus\Industry\\'
target_path = r'D:\code\hadoop-project\industry_data\train\\'
test_path = r'D:\code\hadoop-project\industry_data\test\\'

for c_dir in os.listdir(path):
    country_list.append((c_dir,len(os.listdir(path+c_dir))))

country_list = sorted(country_list,key= lambda x:x[1],reverse=True)

train_file = open("train.txt","w+")
test_file = open("test.txt","w+")

for item in country_list:
    if item[1] >= 5:
        item_list = os.listdir(path+item[0])
        ran = int(len(item_list)*0.8)
        print(item[0],file=train_file)
        print(ran,file=train_file)
        print(item[0],file=test_file)
        print(len(item_list)-ran,file=test_file)
        for i in range(0,ran):
            fi = item_list[i]
            print(fi,file=train_file)
            if not os.path.exists(target_path+item[0]):
                os.mkdir(target_path+item[0])
            copyfile(path + item[0]+'\\'+fi, target_path+item[0]+'\\'+fi)
        for i in range(ran,len(item_list)):
            fi = item_list[i]
            print(fi,file=test_file)
            if not os.path.exists(test_path+item[0]):
                os.mkdir(test_path+item[0])
            copyfile(path + item[0]+'\\'+fi, test_path+item[0]+'\\'+fi)

train_file.close()
test_file.close()



