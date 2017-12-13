orderItems = sc.textFile("/Users/sahildiwan/CCA-175/data/retail_db/order_items/")
orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))
revenuePerId = orderItemsMap.reduceByKey(lambda t, v: t + v)
for i in revenuePerId.take(10): print(i)