order_amounts = [100,200,50,500,400,900,1200,70]
order_including_gst = []
for x in order_amounts :
    order_including_gst.append(x+(x*18/100))
print(order_including_gst)