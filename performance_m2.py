from JellyDB.test_on_performance_m2 import performance_m2
import os, sys

#for n in range(1000):
list = []
for i in range(10):
    os.system('rm ~/ECS165/*')
    #os.sys('python -m JellyDB.test_on_performance_m2'+' '+str(1000))
    list.append(performance_m2(1000))
    print('finish',i)
print(list)
