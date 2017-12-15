'''
To find total revenue using aggregateByKey and number of transactions per id
'''
orderItems = sc.textFile("/Users/sahildiwan/CCA-175/data/retail_db/order_items/")
orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))
revenuePerOrder = orderItemsMap.aggregateByKey((0.0, 0),
	lambda inter, revenue: (inter[0] + revenue, inter[1] + 1),
	lambda final, inter: (final[0] + inter[0], final[1] + inter[1]))
for i in revenuePerOrder.take(10): print(i)
