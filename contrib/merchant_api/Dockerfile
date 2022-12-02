FROM mcr.microsoft.com/dotnet/sdk:5.0-buster-slim

# NOTE: After startup mAPI needs the admin account to perform an http:// POST to
# http://127.0.0.1:5050/api/v1/Node with body:
# {
#    "id": "host.docker.internal:18332",
#    "username": "rpcuser",
#    "password": "rpcpassword",
#    "remarks": "remarks",
#    "zmqNotificationsEndpoint": "tcp://host.docker.internal:18332"
# }
# This will connect mAPI to the running node in the other docker container

RUN apt-get update
RUN apt-get -y install git

ENV CSPROJ_PATH="/opt/src/MerchantAPI/APIGateway/APIGateway.Rest/MerchantAPI.APIGateway.Rest.csproj"
ENV APPVERSION=1.4.0

RUN git clone --branch "v${APPVERSION}" --depth=1 https://github.com/bitcoin-sv/merchantapi-reference.git /opt
RUN dotnet restore ${CSPROJ_PATH} --verbosity normal
RUN dotnet build ${CSPROJ_PATH} --verbosity normal

RUN dotnet publish ${CSPROJ_PATH} -c Release -o /app/publish -p:Version=${APPVERSION}
# RUN dotnet publish ${CSPROJ_PATH} -r linux-x64 -c Release --self-contained true -p:PublishSingleFile=true -o /opt/publish --verbosity normal

WORKDIR /app/publish

CMD sleep infinity