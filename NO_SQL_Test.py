import boto3
import csv

ACCESS_KEY = "redacted key"
SECRET_KEY = "redacted key"
BUCKET_NAME = "brian-rossi-test-bucket"

s3 = boto3.resource('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

try:
	s3.create_bucket(Bucket=BUCKET_NAME, CreateBucketConfiguration={
		'LocationConstraint': 'us-west-2'})
except Exception as e:
	print(e)
	print("this may already exist")

bucket = s3.Bucket(BUCKET_NAME)
bucket.Acl().put(ACL='public-read')

dyndb = boto3.resource('dynamodb', region_name='us-west-2', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

try:
	table = dyndb.create_table( 
		TableName='DataTable', 
		KeySchema=[
			{
				'AttributeName': 'PartitionKey', 
				'KeyType': 'HASH'
			}, 
			{
				'AttributeName': 'RowKey',
				'KeyType': 'RANGE' 
			}
		], 
		AttributeDefinitions=[
			{
				'AttributeName': 'PartitionKey', 
				'AttributeType': 'S'
			}, 
			{
				'AttributeName': 'RowKey',
				'AttributeType': 'S' 
			},
		], 
		ProvisionedThroughput={
			'ReadCapacityUnits': 5,
			'WriteCapacityUnits': 5 
		}
	) 
except:
	#if there is an exception, the table may already exist.
	table = dyndb.Table("DataTable")

table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')
print(table.item_count)

PATH = "/Users/brianrossi/Desktop/experiments/experiments.csv"


with open(PATH, 'rt') as csvfile: 
	csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
	n = 0
	for item in csvf:
		if(n != 0):
			body = open(f'/Users/brianrossi/Desktop/experiments/{item[4]}', 'rb') 
			s3.Object(BUCKET_NAME, item[3]).put(Body=body )
			md = s3.Object(BUCKET_NAME, item[3]).Acl().put(ACL='public-read')

			url = f"https://s3-us-west-2.amazonaws.com/datacont-name/{item[3]}"
			metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
				'description' : item[4], 'date' : item[2], 'url':url}
			print(metadata_item)
			try: 
				table.put_item(Item=metadata_item)
			except:
				print("item may already be there or another failure")
		else:
			n += 1

response = table.get_item(Key={'PartitionKey': 'experiment1','RowKey': 'data1' })
print(response)
item = response['Item'] 
print(item)

