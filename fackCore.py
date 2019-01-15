import calendar
import datetime
import pypyodbc
import random
import csv
# init arg 每月需要修改吨数
needs = [100,100,100,100,100,100,100]
needsPerMonth = []
for i in needs:
    needsPerMonth.append( round(i/50) )
workYear = 2018
startMonth =  5
endMonth = 11
goodsName = "煤灰"
carWaitingMin = 60
carWaitingMax = 360
staffList = ['雷启涛','李艺','文峰','丁小虎']

#连接数据库
DBstr = "Driver={Microsoft Access Driver (*.mdb)};DBQ=source.mdb;PWD=dthq2005;"
targetDBstr = "Driver={Microsoft Access Driver (*.mdb)};DBQ=dtscale.mdb;PWD=dthq2005;"
DB = pypyodbc.win_connect_mdb(DBstr)
targetDB = pypyodbc.win_connect_mdb(targetDBstr)
cursor = DB.cursor()
targetcursor = targetDB.cursor()

# 生成单号并与数据库比对
    # 年月日 + 000只有个位时 00有十位时 eg.201805020001 201805020012
    # 生成一个月的单号
def generateBillNum( m ):
    CalObj = calendar.Calendar()
    BillNum = []
    for i in CalObj.itermonthdates(workYear, m):
        # 剔除 itermonthdates 生成的带非本月的日子
        if i.month != m:
            continue
        else:
            i = i.strftime("%Y%m%d")
            # eg.20180602
            for x in range(89):
                x=x+1
                if x > 9:
                    x = str(i)+"00"+str(x)
                else:
                    x = str(i)+"000"+str(x)
                BillNum.append(x)
    return BillNum

    # 单号去重
def filterBillNum( BillNum , goodsName  ):
    SQL = "SELECT 称重记录.* FROM 称重记录 WHERE 称重记录.货物名称='{goodsFilter}';"
    SQL = SQL.format(goodsFilter=goodsName)
    cursor.execute(SQL)
    row = cursor.fetchall()
    for i in row:
        for m in range(len(BillNum)):
            for item in BillNum[m]:
                if int(i[0]) == int(item):
                    BillNum[m].remove( i[0] )
    return BillNum

    #生成所有月份单号
def generateAllBillNum():
    BillNum = []
    for i in range( startMonth, endMonth+1 ): 
        BillNum.append( generateBillNum( i ) )
    BillNum = filterBillNum( BillNum, goodsName )
    return BillNum

# 匹配车辆载重值
    #车牌去重 取极值作为浮动上下极限
def bindingVolume( goodsName ):
    SQL = "SELECT 称重记录.* FROM 称重记录 WHERE 称重记录.货物名称='{goodsFilter}';"
    SQL = SQL.format(goodsFilter=goodsName)
    cursor.execute(SQL)
    row = cursor.fetchall()
    carNum = []
    for i in row:
        carNum.append( i[1] )
    carNum = list(set(carNum))
    #取值
    SQL = "SELECT 毛重,皮重,净重 FROM 称重记录 WHERE 称重记录.货物名称='{goodsFilter}' AND 称重记录.车号='{carFilter}';"
    carTemplate = {}
    for num in carNum:
        SQL = SQL.format(goodsFilter=goodsName,carFilter=num)
        cursor.execute(SQL)
        row = cursor.fetchall()
        # gross tare net 毛 皮 净
        gross = [] 
        tare = []
        net = []
        for items in row:
            gross.append( items[0] )
            tare.append( items[1] )
            net.append( items[2] )
        carTemplate[num] = [max(gross),min(gross),max(tare),min(tare),str(num)]
    return carTemplate



def generateFack():
    billNumList = generateAllBillNum()
    carObj = bindingVolume( goodsName )
    # - - - - - - -
    carIdList = list(carObj.keys())
    SQL = "Insert into 称重记录(单号,车号,发货单位,收货单位,货物名称,规格型号,承运单位,系统备注,单位,进厂司磅员,司磅员,进厂时间,日期时间,打印次数,毛重,皮重,净重) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    for i in range(len(needsPerMonth)):
        for x in range(needsPerMonth[i]):
            billNum = random.choice( billNumList[i] )
            carNum = random.choice( carIdList )
            #carIdList.remove(carNum)
            dateIn = str(billNum[0:8])
            dateIn = dateIn +" "+ str(random.randint(0,23))+ ":" + str(random.randint(0,59))+ ":" + str(random.randint(0,59))
            dateIn = datetime.datetime.strptime(dateIn, "%Y%m%d %H:%M:%S")
            dateOut = dateIn + datetime.timedelta(minutes = random.randint(carWaitingMin,carWaitingMax))
            dateIn = dateIn.strftime("%Y%m%d %H:%M:%S")
            dateOut = dateOut.strftime("%Y%m%d %H:%M:%S")
            carNum = carObj[carNum][-1]
            gross = round(random.uniform(carObj[carNum][0],carObj[carNum][1]), 2)
            tare = round(random.uniform(carObj[carNum][2],carObj[carNum][3]), 2)
            net = round(gross - tare, 2)
            inStaff = random.choice(staffList)
            outStaff = random.choice(staffList)
                              #'单号','车号','发货单位','收货单位','货物名称','规格型号','承运单位','系统备注','单位','进厂司磅员','司磅员',
                              #'进厂时间','日期时间','打印次数','毛重','皮重','净重'
            billData = (billNum,carNum,"习水黔佰楼建材有限公司","泸州市森源建材有限公司古蔺分公司",goodsName,'','',"'存盘仪表录入","吨",inStaff,outStaff,dateIn,dateOut,0,gross,tare,net)
            targetcursor.execute(SQL,billData)
            targetcursor.commit()
            targetDB.commit()



# # # # # # # # # Function End # # # # # # # # # # # # # #
generateFack()
DB.close()
targetDB.close()
