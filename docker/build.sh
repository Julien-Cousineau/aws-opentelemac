export AWS_REPO="awsopentelemac"
rm -r -f ./packages
mkdir packages

# To install local packages, a local copy needs to be placed under the docker folder. 
cp -r ../../slf-py ./packages/slf-py

mkdir packages/aws-opentelemac
cp -r ../awsopentelemac ./packages/aws-opentelemac/awsopentelemac
cp -r ../setup.py ./packages/aws-opentelemac/setup.py
cp -r ../README.md ./packages/aws-opentelemac/README.md
cp -r ../runapi.py ./packages/aws-opentelemac/runapi.py

docker buildx build --platform linux/amd64 -t $AWS_REPO --load .
rm -r -f ./packages