$( echo "$values" | aws sts  get-caller-identity | jq -r '.Account as $k | "export AWS_ACCOUNT=\($k)"')
export AWS_REPO=awsopentelemac
export AWS_REGION=us-east-1
export AWS_REPOTAG=1

if [ $1 == 'create' ]
then
aws ecr create-repository --repository-name $AWS_REPO
docker run --rm --privileged multiarch/qemu-user-static --reset -p yes
docker buildx create --name awsopentelemac --use
# elif [ $1 == 'login' ]
# then
else
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $AWS_ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com/$AWS_REPO

rm -r -f ./packages
mkdir packages

# To install local packages, a local copy needs to be placed under the docker folder. 
cp -r ../../slf-py ./packages/slf-py

mkdir packages/aws-opentelemac
cp -r ../awsopentelemac ./packages/aws-opentelemac/awsopentelemac
cp -r ../setup.py ./packages/aws-opentelemac/setup.py
cp -r ../README.md ./packages/aws-opentelemac/README.md
cp -r ../runapi.py ./packages/aws-opentelemac/runapi.py

docker buildx build --platform linux/amd64,linux/arm64 -t $AWS_ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com/$AWS_REPO:$AWS_REPOTAG --push .
docker buildx imagetools inspect $AWS_ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com/$AWS_REPO:$AWS_REPOTAG
rm -r -f ./packages

# docker tag $AWS_REPO $AWS_ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com/$AWS_REPO:$AWS_REPOTAG
# docker push $AWS_ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com/$AWS_REPO
fi