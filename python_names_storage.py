from bs4 import BeautifulSoup
from google.cloud import storage
from google.oauth2 import service_account
import requests
import csv

headers = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

credentials_dict = {
  "type": "service_account",
  "project_id": "my-project-dataops",
  "private_key_id": "0ba6728a142a1532296ee7217328256d0c7b65c7",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC9wAMQ4tYFk9B6\nffTZrwdnbN9o3uaL1v6sD/JJq9skUAXpNCN1wCd4/mBZ4SdKk3Yy9v7I2RDTwzYe\nztXhaMExAfhOEMkE2uRis5oOz3phItw8YI1OeMh6QiQsGqcZAyDZBJQSjEZOLMAr\nf0r/5o5zN7R7LHRUNVKCWtT0PcdBrGI1F93jroIcqVeyjMP1ArXswskCKdpGZZd+\nv/nISHiB88qEHkHpD5LZlxUzSAncpG0IYOeGiqKMOHe55D67Kg7/QghglomP/mVX\n0D/QeWjmZOWV9k5adqNDk4WjJA3rwNNw+2nDOl1QuEcHxdiBT9x0v4tPEYr+vP78\nljfrxeXfAgMBAAECggEADwJhO7S1uTs4Anh0JcPykfvKsEDxe55GBtY8FBfD/dtW\nnFvDck0A0lFMzc9BjGSdtOpUvQ5uhlGqtkUmJe5jWb0OJx6sx/WIm0YZqZI/2+8l\n/KCmjM6BDSquMc7p6NCWiJQUImN2LDjd+pLXG+bg3bxKSnP5v694F1eYALniSQ92\ns68jo8d6LSr0hpWhybGaim1R4ORRe1sdTFxqrBDAXqf5Hxs8q750ksMxrdEVmayZ\n6w/nAw8LKeTQq9dIOqaAjkHiIYUrWogPpyiZHPvyrLxCuaY/C5C+84fqR6IaBTDY\nMROTuotJFZwWRwoNUpB3gTwMAws/07uUr59hCeQEAQKBgQDw6V4d4dlabfB7VmFD\nO/rk+M2xspbvPUNhuFAwtM1mgFxL9i+N+DdPXLG+G4mKTz6tUeEVHj0l5GHjUtJl\naeSH1Km3oL24+4va9CKPWMrmYhHsthI13QHzBkz/RHzUDg0bbpBF6YGbqgx9Q8k5\ni58LgLwjFYkDm2c9FnKEbwLVtwKBgQDJolnIjez1rONxngnneNzwwQBV0aNkuxRZ\nwG0RWTzTnZ/NN5f/cqpruVoPJD/o5chWP37t6l+/X/AhzWScYe1FWeHHGY9813B1\nGDWTv1U+IzEGA8ssJ1e5gnee4shUXr7o5vkf/h22i40tKq/DePXZj34DIkLbMP5t\nl7dr6yExGQKBgFdzf1S9nVb0Pa2oB5qOdV8U6iYtPBkQVhts+r8TgtRImDiC33Rb\nvEg0z9jAykbIyWnFJT2zQmM06kvIztM9g1XDXvBnizdRKM2MfdcnVlAXHYad+TJQ\nTdrYWK75P48CosNoeTrHruLA8dOu/abEBjH4w1LBCIGkse887MkiOxc1AoGBALTi\noZsKv55yOCSQO6BdTA/rhMp4ZBWUwx/1QaNDpA7PpDFtAz3V907mnAVjZrYflcbR\n9aOoE7dScNFSOkUUkNAbXzHHG3NKrodNbZsbFXTGC0+zDYyRyuAQTBHMrx0JVkkI\nFEwAI1XFAaSYW8+Hbz56vPhAvyt2W3mjlpgI+uDZAoGARUUjhdbucmTwpcVdyZ45\n5jp/WBNfVO8DplBLeFjenK5VMsG16QjS89QWwYLqSIp+fR9A7oVkVxYahCJtL1HS\nVsNF4nZRSAD4VCDjTjImE9oLPaywOMIik79yiACokQCaWYzaGicPDkUIImQAiInV\nlIdBeyV47JSwCUUWolYCr/k=\n-----END PRIVATE KEY-----\n",
  "client_email": "myaccount@my-project-dataops.iam.gserviceaccount.com",
  "client_id": "117589343202180958606",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/myaccount%40my-project-dataops.iam.gserviceaccount.com"
}

try:

  """Uploads a file to the bucket."""
  credentials = service_account.Credentials.from_service_account_info(credentials_dict)
  storage_client = storage.Client(credentials=credentials)
  bucket = storage_client.get_bucket('atv_4_dalla') ### Nome do seu bucket
  blob = bucket.blob('atv_4_dalla.csv')

  pages = []
  names = "Name \n"

  for i in range(1, 5):
    url = 'https://web.archive.org/web/20121007172955/https://www.nga.gov/collection/anZ' + str(i) + '.htm'
    pages.append(url)

  for item in pages:
    page = requests.get(item)
    soup = BeautifulSoup(page.text, 'html.parser')

    last_links = soup.find(class_='AlphaNav')
    last_links.decompose()

    artist_name_list = soup.find(class_='BodyText')
    artist_name_list_items = artist_name_list.find_all('a')

    for artist_name in artist_name_list_items:
      names = names + artist_name.contents[0] + "\n"

    blob.upload_from_string(names, content_type="text/csv")

except Exception as ex:
  print(ex) 
