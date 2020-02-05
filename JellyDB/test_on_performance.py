from JellyDB.tester_performance import performance_testing
import matplotlib.pyplot as plt

insertion={}
selection = {}
update_ = {}
summation_ = {}
deletion_ = {}
for i in range(1000,11000,1000):
    insert,select,update,summation,deletion = performance_testing(i)
    insertion[i] = insert
    selection[i] = select
    update_[i] = update
    summation_[i] = summation
    deletion_[i] = deletion

x1, y1 = zip(*(insertion.items()))
x2, y2 = zip(*(selection.items()))
x3, y3 = zip(*(update_.items()))
x4, y4 = zip(*(summation_.items()))
x5, y5 = zip(*(deletion_.items()))

plt.figure(figsize=(10, 10))
plt.plot(x1,y1,c = 'r', label='insertion')
plt.plot(x2,y2,c='g',  label='selection')
plt.plot(x3,y3, c='y',label='update')
plt.plot(x4,y4,c = 'b', label='summation')
plt.plot(x5,y5,c = 'c', label='deletion')


#plt.xlim([0.0, 1.0])
#plt.ylim([0.0, 1.05])
plt.xlabel('records')
plt.ylabel('time')
plt.title('Performance_Analysis')
plt.legend(loc="upper right",fontsize='large')
#fig = plt.gcf()
#fig.set_size_inches(18.5, 10.5)
#fig.savefig('performance.png', dpi=100)

plt.show()
