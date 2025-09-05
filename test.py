import pandas as pd

data = pd.read_csv('sample_sales_data.csv')
print(data.columns)


sum_total = data['Unit Price']*data['Units Sold']
sum_total = sum_total.tolist()
sum_total = sum(sum_total)
print(sum_total)

no_of_items = data['Units Sold']
no_of_items = no_of_items.tolist()
no_of_items = sum(no_of_items)

avg = sum_total/no_of_items
print(avg)


