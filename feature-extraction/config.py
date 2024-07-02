from os import getenv
from dotenv import load_dotenv

load_dotenv()
class Config:

  # MongoDB setup
  MONGO_URI = getenv('DR_MONGO_URI', 'mongodb://localhost:27017/')
  MONGO_DB = 'drdb'
  # Collections to process by loader - {label: collection_name}
  COLLECTIONS = {
      #'malware_bp': 'malware_bp',
      #'umbrella_benign_FINISHED': 'umbrella_benign_FINISHED',
      #'benign_2312': 'benign_2312',
      #'benign_2312_anonymized': 'benign_2312_anonymized',
      #'misp_2402': 'misp_2402',

      #'norbi_malware_test': 'norbi_malware_test',
      #'phishing_4': 'phishing_4'
      #'malware_3': 'malware_3'
      #'phishing_2406_strict': 'phishing_2406_strict'
      'malware_2406_strict': 'malware_2406_strict'
  }
