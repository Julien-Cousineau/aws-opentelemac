import os
from awsopentelemac import AWSOpenTelemac

if __name__ == "__main__":
  api=AWSOpenTelemac()
  id= os.environ.get("AWS_CASID")
  api.run(id,api=True)
  
    
