import os

os.environ['envn']='DEV'
os.environ['header']='True'
os.environ['inferSchema']='True'
os.environ['user']= 'root'
os.environ['password'] = 'saksham123'

header=os.environ['header']
inferSchema=os.environ['inferSchema']
envn=os.environ['envn']

# env = os.environ['env']
user = os.environ['user']
password = os.environ['password']


appName="Pipelines Spark"
# print(os.getcwd())
current=os.getcwd()

src_olap= current+'/source/olap'
src_oltp= current+'/source/oltp'

city_path = current+'/output/cities'
presc_path = current+'/output/prescriber'
