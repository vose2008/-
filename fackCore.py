import logging
import calendar
import datetime
import pypyodbc
import random
import csv
logging.basicConfig(level=logging.INFO)
#logging.basicConfig(level=logging.DEBUG)
# init arg 日期 需要修改吨数 日期与吨数一一对应
workYear = 2018
monthList = [ 6,7,10 ]
monthLimit = [ 2000,2000,4000]
goodsName = "碎石"
# 其它参数 车辆最小载重量过滤 车辆等待时间单位分钟 磅房工作人员
carMinWeight = 40
carWaitingMax = 360
carWaitingMin = 60
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
    #logging.info("开始生成"+str(m)+"月单号")
    CalObj = calendar.Calendar()
    billNum = []
    for i in CalObj.itermonthdates(workYear, m):
        # 剔除 itermonthdates 生成的带非本月的日子
        if i.month != m:
            continue
        else:
            i = i.strftime("%Y%m%d")
            # 这儿的 通配符是 % 而 ACCESS里是 * 注意
            SQL = "SELECT 称重记录.* FROM 称重记录 WHERE 货物名称='{goodsFilter}' AND 单号 LIKE '{billFilter}%';"
            SQL = SQL.format(goodsFilter=goodsName,billFilter=i)
            cursor.execute(SQL)
            row = cursor.fetchall()
            # eg.20180602 + xxxxx
            for x in range(89):
                x=x+1
                if x < 10:
                    x = str(i)+"000"+str(x)
                elif x < 100:
                    x = str(i)+"00"+str(x)
                elif x < 1000:
                    x = str(i)+"0"+str(x)
                # 单号去重
                if row:
                    for record in row:
                        if int(record[0]) != int(x):
                            billNum.append(x)
                        else:
                            pass
                else:
                    billNum.append(x)
    logging.info( str(m)+"月生成单号："+str(len(billNum))+"个" )
    return billNum

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
    carTemplate = {}
    for num in carNum:
        SQL = "SELECT 毛重,皮重,净重 FROM 称重记录 WHERE 称重记录.货物名称='{goodsFilter}' AND 称重记录.车号='{carFilter}';"
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
        if max(net)>carMinWeight:
            carTemplate[num] = [max(gross),min(gross),max(tare),min(tare),str(num)]
    return carTemplate




def generateRecord( month, needsNet, generateNet, billNumList, carObj, carIdList ):
    billNum = random.choice( billNumList )
    carNum = random.choice( carIdList )
    carIdList.remove(carNum)
    shipper = "伏龙预制场"
    consignee = "泸州市森源建材有限公司古蔺分公司"
    #goodsName 全局变量
    specifications = ''
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
               #'单号','车号','发货单位','收货单位','货物名称','规 格 型 号','承运单位','系统备注','单位','进厂司磅员','司磅员','进厂时间','日期时间','打印次数','毛重','皮重','净重'
    billData = (billNum, carNum, shipper, consignee, goodsName, specifications, '', "'存盘仪表录入", "吨", inStaff, outStaff, dateIn, dateOut, 0, gross, tare, net)
    generateNet += net
    logging.debug( "已生成"+str(round(generateNet,2)) )
    if generateNet < needsNet:
        generateRecord( month, needsNet, generateNet, billNumList, carObj, carIdList )
    else:
        logging.info( str(month)+"月  需  要："+str(needsNet)+"吨" )
        logging.info( str(month)+"月总计生成："+str(round(generateNet,2))+"吨" )
    # ---- 最后进行数据库写入操作 ----
    SQL = "Insert into 称重记录(单号,车号,发货单位,收货单位,货物名称,规格型号,承运单位,系统备注,单位,进厂司磅员,司磅员,进厂时间,日期时间,打印次数,毛重,皮重,净重) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    targetcursor.execute(SQL,billData)
    targetcursor.commit()

def generateFackReport( monthList, monthLimit ):
    carObj = bindingVolume( goodsName )
    for month in monthList:
        logging.info( "---- 开始第"+str(month)+"月记录 ----" )
        billNumList = generateBillNum( month )
        carIdList = list(carObj.keys())
        generateNet = 0
        needsNet = monthLimit[monthList.index(month)]
        generateRecord( month, needsNet, generateNet, billNumList, carObj, carIdList )

# # # # # # # # # Function End # # # # # # # # # # # # # #
generateFackReport( monthList, monthLimit )
DB.close()
targetDB.close()
